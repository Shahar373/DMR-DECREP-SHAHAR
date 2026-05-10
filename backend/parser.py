"""DSD-FME stderr log parser.

Pure, no I/O. Feed lines via `parse_line`; receive a typed `Event` or `None`.

NOTE: Regex patterns here are drafted from documented DSD-FME output. They will
be re-tuned once a real capture from the target Cap+ network is available
(see tests/sample_dsd_log.txt and the project plan).
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable, Optional, Pattern

from .models import (
    ARSEvent,
    CSBKEvent,
    EncryptionEvent,
    Event,
    LRRPEvent,
    VoiceCallEvent,
    VoiceEndEvent,
)


@dataclass
class _SlotState:
    """Tracks whether a slot was last seen carrying voice, so SLOT IDLE
    can be promoted to a voice_end event only when meaningful."""

    in_voice: dict[int, bool] = field(default_factory=dict)


# --- Regex patterns (case-insensitive where useful) ---

# Voice call start: "Slot N Group Voice ... SRC=<id> ... TGT=<id>"
_VOICE_START_RE = re.compile(
    r"Slot\s+(?P<slot>\d+).*?Group\s+Voice.*?SRC\s*=\s*(?P<src>\d+)"
    r".*?TGT\s*=\s*(?P<tgt>\d+)",
    re.IGNORECASE,
)

# Voice call end / slot idle: "Slot N [SLOT IDLE]"
_SLOT_IDLE_RE = re.compile(r"Slot\s+(?P<slot>\d+)\s*\[\s*SLOT\s+IDLE\s*\]", re.IGNORECASE)

# LRRP location report. Accept several common shapes.
_LRRP_RE = re.compile(
    r"LRRP.*?(?:ID|SRC)\s*=\s*(?P<src>\d+)"
    r".*?Lat(?:itude)?\s*[=:]\s*(?P<lat>-?\d+\.\d+)"
    r".*?Lon(?:gitude)?\s*[=:]\s*(?P<lon>-?\d+\.\d+)",
    re.IGNORECASE,
)

# ARS registration / deregistration.
_ARS_RE = re.compile(
    r"ARS.*?(?:SRC|ID)\s*=\s*(?P<src>\d+).*?(?P<state>register|registered|deregister|deregistered)",
    re.IGNORECASE,
)

# Generic CSBK line. SRC is optional.
_CSBK_RE = re.compile(r"\bCSBK\b(?:.*?SRC\s*=\s*(?P<src>\d+))?", re.IGNORECASE)

# Privacy / encryption indicators.
_PI_HEADER_RE = re.compile(r"\bPI\s+Header\b", re.IGNORECASE)
_ALG_ID_RE = re.compile(r"ALG\s*ID\s*[=:]\s*(?P<alg>0x[0-9A-Fa-f]+|\d+)", re.IGNORECASE)


class DSDLogParser:
    def __init__(self) -> None:
        self._slots = _SlotState()
        self._matchers: list[tuple[Pattern[str], Callable[[re.Match[str], str], Optional[Event]]]] = [
            (_VOICE_START_RE, self._make_voice_start),
            (_SLOT_IDLE_RE, self._make_voice_end),
            (_LRRP_RE, self._make_lrrp),
            (_ARS_RE, self._make_ars),
            (_PI_HEADER_RE, self._make_encryption_pi),
            (_ALG_ID_RE, self._make_encryption_alg),
            (_CSBK_RE, self._make_csbk),
        ]

    def parse_line(self, line: str) -> Optional[Event]:
        line = line.rstrip("\n")
        if not line.strip():
            return None
        for pattern, builder in self._matchers:
            m = pattern.search(line)
            if m:
                event = builder(m, line)
                if event is not None:
                    return event
        return None

    # --- builders ---

    def _make_voice_start(self, m: re.Match[str], raw: str) -> Event:
        slot = int(m.group("slot"))
        self._slots.in_voice[slot] = True
        return VoiceCallEvent(
            raw_line=raw,
            slot=slot,
            src_id=int(m.group("src")),
            tgt_id=int(m.group("tgt")),
            encrypted=False,
        )

    def _make_voice_end(self, m: re.Match[str], raw: str) -> Optional[Event]:
        slot = int(m.group("slot"))
        if not self._slots.in_voice.get(slot):
            return None
        self._slots.in_voice[slot] = False
        return VoiceEndEvent(raw_line=raw, slot=slot)

    def _make_lrrp(self, m: re.Match[str], raw: str) -> Event:
        return LRRPEvent(
            raw_line=raw,
            src_id=int(m.group("src")),
            lat=float(m.group("lat")),
            lon=float(m.group("lon")),
        )

    def _make_ars(self, m: re.Match[str], raw: str) -> Event:
        state = m.group("state").lower()
        return ARSEvent(
            raw_line=raw,
            src_id=int(m.group("src")),
            registered=state.startswith("regist"),
        )

    def _make_csbk(self, m: re.Match[str], raw: str) -> Event:
        src = m.group("src") if m.groupdict().get("src") else None
        return CSBKEvent(raw_line=raw, src_id=int(src) if src else None)

    def _make_encryption_pi(self, m: re.Match[str], raw: str) -> Event:
        return EncryptionEvent(raw_line=raw, alg_id=None)

    def _make_encryption_alg(self, m: re.Match[str], raw: str) -> Optional[Event]:
        alg = m.group("alg")
        try:
            value = int(alg, 16) if alg.lower().startswith("0x") else int(alg)
        except ValueError:
            return None
        if value == 0:
            return None  # ALG ID 0 means clear
        return EncryptionEvent(raw_line=raw, alg_id=alg)
