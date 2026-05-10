"""DSD-FME stderr log parser — Phase 1A (Capacity Plus control channel).

Pure, line-by-line. Feed lines via `parse_line`; receive a typed `Event` or
`None`. Each line of DSD-FME output is independently parseable into one event,
but a single CSBK block in the source spans several lines (Channel Status +
Bank announcement + LSN snapshots + SLCO site). The state manager / WebSocket
layer is responsible for stitching them together.

State carried by the parser is intentionally minimal — only the latest sync
timestamp, so continuation lines (which carry no timestamp of their own) get
attributed to the right moment. Phase 1B will extend this with payload-channel
event types (voice_start, lrrp, ars, encryption).
"""
from __future__ import annotations

import re
from datetime import datetime, time
from typing import Callable, Optional, Pattern

from .models import (
    BankCallEvent,
    ChannelStatusEvent,
    Event,
    LSNState,
    LSNStatusEvent,
    QualityEvent,
    SiteInfoEvent,
)

# --- Sync line: source of timestamp + occasional CRC/FEC flags ---
# Examples:
#   20:56:22 Sync: +DMR  [slot1]  slot2  | Color Code=02 | CSBK
#   20:56:22 Sync: +DMR   slot1  [slot2] | CACH/Burst FEC ERR
#   21:04:17 Sync: +DMR  [slot1]  slot2  | Color Code=02 | CSBK (CRC ERR)
_SYNC_RE = re.compile(r"^(?P<h>\d{2}):(?P<m>\d{2}):(?P<s>\d{2})\s+Sync:\s*\+DMR")

# --- Channel Status header ---
# Example: " Capacity Plus Channel Status - FL: 3 TS: 1 RS: 0 - Rest LSN: 6 - Single Block"
_CH_STATUS_RE = re.compile(
    r"Capacity\s+Plus\s+Channel\s+Status"
    r"\s*-\s*FL:\s*(?P<fl>\d+)"
    r"\s+TS:\s*(?P<ts>\d+)"
    r"\s+RS:\s*(?P<rs>\d+)"
    r"\s*-\s*Rest\s+LSN:\s*(?P<rest_lsn>\d+)"
    r"\s*-\s*(?P<block>Single|Initial|Final)\s+Block",
    re.IGNORECASE,
)

# --- Bank announcement (active call list for a bank of 4 LSNs) ---
# Example: " Bank One F80 Private or Data Call(s) -  LSN 05: TGT 215; LSN 06: TGT 64250;"
_BANK_CALL_RE = re.compile(
    r"Bank\s+(?P<bank>One|Two)\s+(?P<flag>[0-9A-Fa-f]+)\s+"
    r"(?P<desc>.+?)\s+Call\(s\)\s*-\s*(?P<rest>.+?)\s*$",
    re.IGNORECASE,
)
_BANK_PAIR_RE = re.compile(r"LSN\s+(?P<lsn>\d+):\s*TGT\s+(?P<tg>\d+)")

# --- LSN status snapshot ---
# Examples:
#   "  LSN 01:  Idle;  LSN 02:  Idle;  LSN 03:  Rest;  LSN 04:  Idle;"
#   "  LSN 05:   215;  LSN 06: 64250;  LSN 07:  Idle;  LSN 08:  Idle;"
_LSN_PAIR_RE = re.compile(r"LSN\s+(?P<lsn>\d+):\s*(?P<value>\S+?);")

# --- SLCO site identification ---
# Example: " SLCO Capacity Plus Site: 2 - Rest LSN: 6 - RS: 00"
_SLCO_SITE_RE = re.compile(
    r"SLCO\s+Capacity\s+Plus\s+Site:\s*(?P<site>\d+)"
    r"\s*-\s*Rest\s+LSN:\s*(?P<rest_lsn>\d+)"
    r"\s*-\s*RS:\s*(?P<rs>\w+)",
    re.IGNORECASE,
)

# --- Standalone quality errors ---
_SLCO_CRC_RE = re.compile(r"^\s*SLCO\s+CRC\s+ERR\s*$", re.IGNORECASE)


class DSDLogParser:
    def __init__(self) -> None:
        self._last_timestamp: Optional[datetime] = None
        # Order matters: bank call lines also contain "LSN N:" tokens, so they
        # must be tested before generic LSN status. SLCO site line contains
        # "Rest LSN:" so it must be tested before channel status (which also
        # contains "Rest LSN:") — no, channel status always begins with the
        # "Capacity Plus Channel Status" prefix that SLCO doesn't have, so the
        # order between those two is not load-bearing.
        self._matchers: list[tuple[Pattern[str], Callable[[re.Match[str], str], Optional[Event]]]] = [
            (_CH_STATUS_RE, self._make_channel_status),
            (_SLCO_SITE_RE, self._make_site_info),
            (_BANK_CALL_RE, self._make_bank_call),
            (_SLCO_CRC_RE, self._make_slco_crc),
        ]

    # --- public API ---

    def parse_line(self, line: str) -> Optional[Event]:
        line = line.rstrip("\n")
        if not line.strip():
            return None

        # Sync line: update timestamp; emit a quality event if it carries an
        # error flag. Sync lines never produce structural events on their own.
        sync = _SYNC_RE.match(line)
        if sync:
            self._update_timestamp(sync)
            if "CSBK (CRC ERR)" in line:
                return QualityEvent(timestamp=self._now(), raw_line=line, error_type="CSBK_CRC")
            if "CACH/Burst FEC ERR" in line:
                return QualityEvent(timestamp=self._now(), raw_line=line, error_type="CACH_BURST_FEC")
            return None

        for pattern, handler in self._matchers:
            m = pattern.search(line)
            if m:
                event = handler(m, line)
                if event is not None:
                    return event

        # LSN status snapshot is checked last because its pattern is the
        # loosest (matches anything containing "LSN N: VAL;").
        lsn_event = self._try_lsn_status(line)
        if lsn_event is not None:
            return lsn_event

        return None

    # --- handlers ---

    def _update_timestamp(self, m: re.Match[str]) -> None:
        h, mi, s = int(m.group("h")), int(m.group("m")), int(m.group("s"))
        # We only have HH:MM:SS in the log; combine with today's date so events
        # carry a proper datetime. Good enough for live monitoring.
        self._last_timestamp = datetime.combine(datetime.utcnow().date(), time(h, mi, s))

    def _now(self) -> datetime:
        return self._last_timestamp or datetime.utcnow()

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
        # Reject lines that are really other event types containing LSN tokens
        # (Bank announcements were already handled above and returned).
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
                    # Unknown token — keep raw so we notice during dev
                    states.append(LSNState(lsn=lsn, state=v))
                else:
                    states.append(LSNState(lsn=lsn, state="Active", tg=tg))
        return LSNStatusEvent(timestamp=self._now(), raw_line=line, states=states)
