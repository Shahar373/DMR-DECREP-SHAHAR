"""Event models emitted by the DSD-FME log parser.

Phase 1A: Capacity Plus control-channel events. The control channel does not
expose per-call SRC IDs or GPS — those live on payload channels and will be
added in Phase 1B (voice_start, lrrp, ars, encryption).
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Annotated, Literal, Optional, Union

from pydantic import BaseModel, Field


class EventType(str, Enum):
    SITE_INFO = "site_info"
    CHANNEL_STATUS = "channel_status"
    LSN_STATUS = "lsn_status"
    BANK_CALL = "bank_call"
    QUALITY = "quality"


class LSNState(BaseModel):
    """One slot inside an LSN status snapshot. `tg` is set when state == 'Active'."""

    lsn: int
    state: str  # "Idle" | "Rest" | "Active"
    tg: Optional[int] = None


class _BaseEvent(BaseModel):
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    raw_line: str


class SiteInfoEvent(_BaseEvent):
    """Parsed from `SLCO Capacity Plus Site: N - Rest LSN: M - RS: XX`."""

    type: Literal[EventType.SITE_INFO] = EventType.SITE_INFO
    site: int
    rest_lsn: int
    rs: str  # kept as string because RS appears as "00" in logs


class ChannelStatusEvent(_BaseEvent):
    """Parsed from `Capacity Plus Channel Status - FL: X TS: Y RS: Z - Rest LSN: N - <Block>`."""

    type: Literal[EventType.CHANNEL_STATUS] = EventType.CHANNEL_STATUS
    fl: int
    ts: int
    rs: int
    rest_lsn: int
    block_type: str  # "Single" | "Initial" | "Final"


class LSNStatusEvent(_BaseEvent):
    """A single status snapshot line covering 4 LSNs (one bank: 1-4 or 5-8)."""

    type: Literal[EventType.LSN_STATUS] = EventType.LSN_STATUS
    states: list[LSNState]


class BankCallEvent(_BaseEvent):
    """Parsed from `Bank <One|Two> <hex> <description> Call(s) - LSN N: TGT X; ...`.

    `lsn_to_tg` is the active LSN -> Talkgroup mapping announced for this bank.
    """

    type: Literal[EventType.BANK_CALL] = EventType.BANK_CALL
    bank: str  # "One" | "Two"
    flag_byte: str
    description: str
    lsn_to_tg: dict[int, int]


class QualityEvent(_BaseEvent):
    """Decode quality issue (CRC / FEC error). Useful for SNR/health indicator."""

    type: Literal[EventType.QUALITY] = EventType.QUALITY
    error_type: str  # "CSBK_CRC" | "SLCO_CRC" | "CACH_BURST_FEC"


Event = Annotated[
    Union[
        SiteInfoEvent,
        ChannelStatusEvent,
        LSNStatusEvent,
        BankCallEvent,
        QualityEvent,
    ],
    Field(discriminator="type"),
]
