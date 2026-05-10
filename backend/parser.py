"""DSD-FME stderr log parser.

Pure, line-by-line. Feed lines via `parse_line`; receive a typed `Event` or
`None`. Each meaningful line of DSD-FME output is independently parseable into
one event, but a single transaction in the source spans several lines (CSBK
block, LRRP data packet, voice call frames). The state manager / WebSocket
layer is responsible for stitching them together.

State carried by the parser:
  * `_last_timestamp` — most recent sync HH:MM:SS, attached to continuation
    lines that carry no timestamp of their own.
  * `_last_lrrp_src` — radio id from the most recent `SRC(24):` line. Attached
    to the next `Lat:` event so the GPS position is bound to the reporting
    radio, then cleared.

Phase 1A covers Capacity Plus control channel; Phase 1B adds payload-channel
events (voice, preamble CSBK, data header, IP mapping, LRRP, encryption).
DSD-FME run with `-N` emits NCurses cursor escapes mixed into stderr — the
parser strips ANSI/CSI/charset-designate sequences before matching.
"""
from __future__ import annotations

import re
from datetime import datetime, time
from typing import Callable, Optional, Pattern

from .models import (
    BankCallEvent,
    ChannelStatusEvent,
    DataHeaderEvent,
    EncryptionEvent,
    Event,
    IPMappingEvent,
    LRRPPositionEvent,
    LRRPRequestEvent,
    LSNState,
    LSNStatusEvent,
    PreambleCSBKEvent,
    QualityEvent,
    SiteInfoEvent,
    VoiceCallEvent,
)

# --- ANSI / NCurses escape stripping -----------------------------------------
# DSD-FME with -N emits a status pane via NCurses; cursor-positioning and
# color escapes leak into stderr and corrupt the structural lines. We strip
# them up front so regexes can match clean content.
_ANSI_RE = re.compile(r"\x1b(?:\[[0-9;?]*[a-zA-Z]|\(.)")


def _strip_ansi(s: str) -> str:
    return _ANSI_RE.sub("", s)


# --- Phase 1A: control channel -----------------------------------------------

_SYNC_RE = re.compile(r"(?P<h>\d{2}):(?P<m>\d{2}):(?P<s>\d{2})\s+Sync:\s*\+DMR")

_CH_STATUS_RE = re.compile(
    r"Capacity\s+Plus\s+Channel\s+Status"
    r"\s*-\s*FL:\s*(?P<fl>\d+)"
    r"\s+TS:\s*(?P<ts>\d+)"
    r"\s+RS:\s*(?P<rs>\d+)"
    r"\s*-\s*Rest\s+LSN:\s*(?P<rest_lsn>\d+)"
    r"\s*-\s*(?P<block>Single|Initial|Final)\s+Block",
    re.IGNORECASE,
)

_BANK_CALL_RE = re.compile(
    r"Bank\s+(?P<bank>One|Two)\s+(?P<flag>[0-9A-Fa-f]+)\s+"
    r"(?P<desc>.+?)\s+Call\(s\)\s*-\s*(?P<rest>.+?)\s*$",
    re.IGNORECASE,
)
_BANK_PAIR_RE = re.compile(r"LSN\s+(?P<lsn>\d+):\s*TGT\s+(?P<tg>\d+)")

_LSN_PAIR_RE = re.compile(r"LSN\s+(?P<lsn>\d+):\s*(?P<value>\S+?);")

_SLCO_SITE_RE = re.compile(
    r"SLCO\s+Capacity\s+Plus\s+Site:\s*(?P<site>\d+)"
    r"\s*-\s*Rest\s+LSN:\s*(?P<rest_lsn>\d+)"
    r"\s*-\s*RS:\s*(?P<rs>\w+)",
    re.IGNORECASE,
)

_SLCO_CRC_RE = re.compile(r"^\s*SLCO\s+CRC\s+ERR\s*$", re.IGNORECASE)


# --- Phase 1B: payload channel -----------------------------------------------

# " SLOT 2 TGT=1 SRC=2102 Group Call  "
# " SLOT 2 TGT=1 SRC=223 Group TXI Call  "
# " SLOT 2 TGT=1 SRC=0 Cap+ Group Call  Rest LSN: 5 "
# " SLOT 2 TGT=1 SRC=2102 Cap+ Group TXI Call  Rest LSN: 5 "
_VOICE_CALL_RE = re.compile(
    r"SLOT\s+(?P<slot>\d+)\s+TGT=(?P<tgt>\d+)\s+SRC=(?P<src>\d+)\s+"
    r"(?P<capplus>Cap\+\s+)?Group(?P<txi>\s+TXI)?\s+Call"
    r"(?:\s+Rest\s+LSN:\s*(?P<rest_lsn>\d+))?"
)

# " Preamble CSBK - Individual Data - Source: 199 - Target: 64250 - Rest LSN: 6"
# " Preamble CSBK - Individual CSBK - Source: 64250 - Target: 2102 - Rest LSN: 5"
_PREAMBLE_CSBK_RE = re.compile(
    r"Preamble\s+CSBK\s*-\s*(?P<addr>Individual|Group)\s+(?P<kind>Data|CSBK|Voice)"
    r"\s*-\s*Source:\s*(?P<src>\d+)\s*-\s*Target:\s*(?P<tgt>\d+)"
    r"(?:\s*-\s*Rest\s+LSN:\s*(?P<rest_lsn>\d+))?"
)

# " Slot 1 Data Header - Indiv - Unconfirmed Delivery - Source: 199 Target: 64250 "
# " Slot 1 Data Header - Indiv - Confirmed Delivery - Response Requested - Source: 199 Target: 64250 "
# " Slot 1 Data Header - Indiv - Response Packet - Source: 199 Target: 64250 "
_DATA_HEADER_RE = re.compile(
    r"Slot\s+(?P<slot>\d+)\s+Data\s+Header\s*-\s*(?P<addr>Indiv|Group)\s*-\s*"
    r"(?P<delivery>.+?)\s*-\s*Source:\s*(?P<src>\d+)\s+Target:\s*(?P<tgt>\d+)"
)

# " SRC(24): 00000068; IP: 012.000.000.068; Port: 4001; "
# " DST(24): 00064250; IP: 013.000.250.250; Port: 4001; "
_IP_MAPPING_RE = re.compile(
    r"(?P<role>SRC|DST)\(\d+\):\s*0*(?P<radio>\d+);"
    r"\s*IP:\s*(?P<ip>[\d.]+);"
    r"\s*Port:\s*(?P<port>\d+)"
)

# " Lat: 32.10128 Lon: 34.87151 (32.10128, 34.87151)"
_LRRP_POSITION_RE = re.compile(
    r"Lat:\s*(?P<lat>-?\d+\.\d+)\s+Lon:\s*(?P<lon>-?\d+\.\d+)"
)

# " LRRP SRC: 199; Request from TGT: 64250;"
# " LRRP SRC: 199; Response to TGT: 64250;"
_LRRP_REQRESP_RE = re.compile(
    r"LRRP\s+SRC:\s*(?P<src>\d+);\s*"
    r"(?P<dir>Request\s+from|Response\s+to)\s+TGT:\s*(?P<tgt>\d+)"
)

# " SLOT 1 Protected LC  FLCO=0x04 FID=0x80 "
_ENCRYPTION_RE = re.compile(
    r"SLOT\s+(?P<slot>\d+)\s+Protected\s+LC\s+FLCO=(?P<flco>0x[0-9A-Fa-f]+)\s+FID=(?P<fid>0x[0-9A-Fa-f]+)"
)


class DSDLogParser:
    def __init__(self) -> None:
        self._last_timestamp: Optional[datetime] = None
        # Bound to the next LRRP Lat/Lon event so it carries the reporting
        # radio id. Set by SRC(24) lines, cleared after a position is emitted.
        self._last_lrrp_src: Optional[int] = None
        # Order matters where patterns overlap. The Bank-call line contains
        # `LSN N:` tokens that would otherwise be eaten by the LSN status
        # heuristic; the voice-call line precedes Protected LC inspection.
        self._matchers: list[tuple[Pattern[str], Callable[[re.Match[str], str], Optional[Event]]]] = [
            # Phase 1A
            (_CH_STATUS_RE, self._make_channel_status),
            (_SLCO_SITE_RE, self._make_site_info),
            (_BANK_CALL_RE, self._make_bank_call),
            (_SLCO_CRC_RE, self._make_slco_crc),
            # Phase 1B
            (_VOICE_CALL_RE, self._make_voice_call),
            (_PREAMBLE_CSBK_RE, self._make_preamble_csbk),
            (_DATA_HEADER_RE, self._make_data_header),
            (_IP_MAPPING_RE, self._make_ip_mapping),
            (_LRRP_REQRESP_RE, self._make_lrrp_request),
            (_LRRP_POSITION_RE, self._make_lrrp_position),
            (_ENCRYPTION_RE, self._make_encryption),
        ]

    # --- public API ---

    def parse_line(self, line: str) -> Optional[Event]:
        line = _strip_ansi(line).rstrip("\n")
        if not line.strip():
            return None

        sync = _SYNC_RE.search(line)
        if sync:
            self._update_timestamp(sync)
            if "CSBK (CRC ERR)" in line:
                return QualityEvent(timestamp=self._now(), raw_line=line, error_type="CSBK_CRC")
            if "CSBK (FEC ERR)" in line:
                return QualityEvent(timestamp=self._now(), raw_line=line, error_type="CSBK_FEC")
            if "CACH/Burst FEC ERR" in line:
                return QualityEvent(timestamp=self._now(), raw_line=line, error_type="CACH_BURST_FEC")
            return None

        for pattern, handler in self._matchers:
            m = pattern.search(line)
            if m:
                event = handler(m, line)
                if event is not None:
                    return event

        lsn_event = self._try_lsn_status(line)
        if lsn_event is not None:
            return lsn_event

        return None

    # --- timestamp / helpers ---

    def _update_timestamp(self, m: re.Match[str]) -> None:
        h, mi, s = int(m.group("h")), int(m.group("m")), int(m.group("s"))
        # Log lines carry only HH:MM:SS; combine with today's local date.
        self._last_timestamp = datetime.combine(datetime.now().date(), time(h, mi, s))

    def _now(self) -> datetime:
        return self._last_timestamp or datetime.now()

    # --- Phase 1A handlers ---

    def _make_channel_status(self, m: re.Match[str], raw: str) -> Event:
        return ChannelStatusEvent(
            timestamp=self._now(),
            raw_line=raw,
            fl=int(m.group("fl")),
            ts=int(m.group("ts")),
            rs=int(m.group("rs")),
            rest_lsn=int(m.group("rest_lsn")),
            block_type=m.group("block").capitalize(),
        )

    def _make_site_info(self, m: re.Match[str], raw: str) -> Event:
        return SiteInfoEvent(
            timestamp=self._now(),
            raw_line=raw,
            site=int(m.group("site")),
            rest_lsn=int(m.group("rest_lsn")),
            rs=m.group("rs"),
        )

    def _make_bank_call(self, m: re.Match[str], raw: str) -> Event:
        lsn_to_tg = {
            int(pair.group("lsn")): int(pair.group("tg"))
            for pair in _BANK_PAIR_RE.finditer(m.group("rest"))
        }
        return BankCallEvent(
            timestamp=self._now(),
            raw_line=raw,
            bank=m.group("bank").capitalize(),
            flag_byte=m.group("flag").upper(),
            description=m.group("desc").strip(),
            lsn_to_tg=lsn_to_tg,
        )

    def _make_slco_crc(self, m: re.Match[str], raw: str) -> Event:
        return QualityEvent(timestamp=self._now(), raw_line=raw, error_type="SLCO_CRC")

    def _try_lsn_status(self, line: str) -> Optional[Event]:
        if "Bank" in line and "Call" in line:
            return None
        pairs = _LSN_PAIR_RE.findall(line)
        if len(pairs) < 2:
            return None
        states: list[LSNState] = []
        for lsn_str, value in pairs:
            lsn = int(lsn_str)
            v = value.strip()
            if v.lower() == "idle":
                states.append(LSNState(lsn=lsn, state="Idle"))
            elif v.lower() == "rest":
                states.append(LSNState(lsn=lsn, state="Rest"))
            else:
                try:
                    tg = int(v)
                except ValueError:
                    states.append(LSNState(lsn=lsn, state=v))
                else:
                    states.append(LSNState(lsn=lsn, state="Active", tg=tg))
        return LSNStatusEvent(timestamp=self._now(), raw_line=line, states=states)

    # --- Phase 1B handlers ---

    def _make_voice_call(self, m: re.Match[str], raw: str) -> Event:
        rest_lsn = m.group("rest_lsn")
        return VoiceCallEvent(
            timestamp=self._now(),
            raw_line=raw,
            slot=int(m.group("slot")),
            src=int(m.group("src")),
            tgt=int(m.group("tgt")),
            is_cap_plus=bool(m.group("capplus")),
            is_txi=bool(m.group("txi")),
            rest_lsn=int(rest_lsn) if rest_lsn else None,
        )

    def _make_preamble_csbk(self, m: re.Match[str], raw: str) -> Event:
        rest_lsn = m.group("rest_lsn")
        return PreambleCSBKEvent(
            timestamp=self._now(),
            raw_line=raw,
            addressing=m.group("addr"),
            kind=m.group("kind"),
            src=int(m.group("src")),
            tgt=int(m.group("tgt")),
            rest_lsn=int(rest_lsn) if rest_lsn else None,
        )

    def _make_data_header(self, m: re.Match[str], raw: str) -> Event:
        delivery_raw = m.group("delivery").strip()
        response_requested = "Response Requested" in delivery_raw
        # Strip the "Response Requested" suffix so `delivery` is just the type
        delivery = re.sub(r"\s*-\s*Response\s+Requested\s*$", "", delivery_raw).strip()
        return DataHeaderEvent(
            timestamp=self._now(),
            raw_line=raw,
            slot=int(m.group("slot")),
            addressing=m.group("addr"),
            delivery=delivery,
            response_requested=response_requested,
            src=int(m.group("src")),
            tgt=int(m.group("tgt")),
        )

    def _make_ip_mapping(self, m: re.Match[str], raw: str) -> Event:
        radio_id = int(m.group("radio"))
        # SRC(24) lines establish the LRRP context for the next Lat: line
        if m.group("role") == "SRC":
            self._last_lrrp_src = radio_id
        return IPMappingEvent(
            timestamp=self._now(),
            raw_line=raw,
            role=m.group("role"),
            radio_id=radio_id,
            ip=m.group("ip"),
            port=int(m.group("port")),
        )

    def _make_lrrp_position(self, m: re.Match[str], raw: str) -> Event:
        src = self._last_lrrp_src
        # Consume the binding so a stray later Lat: doesn't reuse stale src.
        self._last_lrrp_src = None
        return LRRPPositionEvent(
            timestamp=self._now(),
            raw_line=raw,
            src=src,
            lat=float(m.group("lat")),
            lon=float(m.group("lon")),
        )

    def _make_lrrp_request(self, m: re.Match[str], raw: str) -> Event:
        direction_raw = m.group("dir")
        direction = "Request" if "Request" in direction_raw else "Response"
        return LRRPRequestEvent(
            timestamp=self._now(),
            raw_line=raw,
            src=int(m.group("src")),
            tgt=int(m.group("tgt")),
            direction=direction,
        )

    def _make_encryption(self, m: re.Match[str], raw: str) -> Event:
        return EncryptionEvent(
            timestamp=self._now(),
            raw_line=raw,
            slot=int(m.group("slot")),
            flco=m.group("flco"),
            fid=m.group("fid"),
        )
