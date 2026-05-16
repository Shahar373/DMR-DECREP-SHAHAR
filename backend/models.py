"""Event models emitted by the DSD-FME log parser.

Phase 1A: Capacity Plus control-channel events (site, channel status, LSN
status, bank announcements, quality).

Phase 1B: payload-channel events (voice calls with SRC, preamble CSBKs, data
headers, IP mappings, LRRP positions/requests, encryption indicators).
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Annotated, Literal, Optional, Union

from pydantic import BaseModel, Field


class EventType(str, Enum):
    # Phase 1A — control channel
    SITE_INFO = "site_info"
    CHANNEL_STATUS = "channel_status"
    LSN_STATUS = "lsn_status"
    BANK_CALL = "bank_call"
    QUALITY = "quality"
    # Phase 1B — payload channel
    VOICE_CALL = "voice_call"
    PREAMBLE_CSBK = "preamble_csbk"
    DATA_HEADER = "data_header"
    IP_MAPPING = "ip_mapping"
    LRRP_POSITION = "lrrp_position"
    LRRP_REQUEST = "lrrp_request"
    ENCRYPTION = "encryption"


class LSNState(BaseModel):
    """One slot inside an LSN status snapshot. `tg` is set when state == 'Active'."""

    lsn: int
    state: str  # "Idle" | "Rest" | "Active"
    tg: Optional[int] = None


# Bumped only when an existing event field's type or semantics change in a
# backwards-incompatible way. Additive fields (new optional columns, new event
# subclasses) do NOT bump this — readers tolerate unknown keys. The current
# value is recorded into the SQLite sidecar at build time and surfaced as
# ``index_outdated`` if a newer-versioned event ever lands in an older index.
EVENT_SCHEMA_VERSION = 1


class _BaseEvent(BaseModel):
    timestamp: datetime = Field(default_factory=lambda: datetime.now())
    raw_line: str
    schema_version: int = Field(default=EVENT_SCHEMA_VERSION)


# --- Phase 1A: control channel ---

class SiteInfoEvent(_BaseEvent):
    type: Literal[EventType.SITE_INFO] = EventType.SITE_INFO
    site: int
    rest_lsn: int
    rs: str


class ChannelStatusEvent(_BaseEvent):
    type: Literal[EventType.CHANNEL_STATUS] = EventType.CHANNEL_STATUS
    fl: int
    ts: int
    rs: int
    rest_lsn: int
    block_type: str  # "Single" | "Initial" | "Final"


class LSNStatusEvent(_BaseEvent):
    type: Literal[EventType.LSN_STATUS] = EventType.LSN_STATUS
    states: list[LSNState]


class BankCallEvent(_BaseEvent):
    type: Literal[EventType.BANK_CALL] = EventType.BANK_CALL
    bank: str  # "One" | "Two"
    flag_byte: str
    description: str
    lsn_to_tg: dict[int, int]


class QualityEvent(_BaseEvent):
    type: Literal[EventType.QUALITY] = EventType.QUALITY
    error_type: str  # "CSBK_CRC" | "SLCO_CRC" | "CACH_BURST_FEC"


# --- Phase 1B: payload channel ---

class VoiceCallEvent(_BaseEvent):
    """A voice burst seen on a payload slot. Carries the actual SRC radio id."""

    type: Literal[EventType.VOICE_CALL] = EventType.VOICE_CALL
    slot: int  # 1 or 2
    src: int  # source radio id
    tgt: int  # target talkgroup or radio id
    is_cap_plus: bool = False  # "Cap+ Group Call" flavor
    is_txi: bool = False  # "Group TXI Call" (transmit interrupt)
    rest_lsn: Optional[int] = None  # only on Cap+ flavor


class PreambleCSBKEvent(_BaseEvent):
    """CC announcement preceding a data/voice/CSBK transaction."""

    type: Literal[EventType.PREAMBLE_CSBK] = EventType.PREAMBLE_CSBK
    addressing: str  # "Individual" | "Group"
    kind: str  # "Data" | "CSBK" | "Voice"
    src: int
    tgt: int
    rest_lsn: Optional[int] = None


class DataHeaderEvent(_BaseEvent):
    """Header of a data packet on a payload slot (precedes the body)."""

    type: Literal[EventType.DATA_HEADER] = EventType.DATA_HEADER
    slot: int
    addressing: str  # "Indiv" | "Group"
    delivery: str  # "Unconfirmed Delivery" | "Confirmed Delivery" | "Response Packet"
    response_requested: bool = False
    src: int
    tgt: int


class IPMappingEvent(_BaseEvent):
    """Radio id ↔ IP/port seen on a data packet routing header.

    Useful for the device table: each radio has a deterministic IP from the
    Motorola data-revert scheme.
    """

    type: Literal[EventType.IP_MAPPING] = EventType.IP_MAPPING
    role: str  # "SRC" | "DST"
    radio_id: int
    ip: str
    port: int


class LRRPPositionEvent(_BaseEvent):
    """GPS position decoded from an LRRP packet body.

    `src` is the reporting radio id — stitched from the most recent SRC(24)
    line in the same data packet. May be None if context was lost.
    """

    type: Literal[EventType.LRRP_POSITION] = EventType.LRRP_POSITION
    src: Optional[int]
    lat: float
    lon: float


class LRRPRequestEvent(_BaseEvent):
    """LRRP control message (request for / response with a position)."""

    type: Literal[EventType.LRRP_REQUEST] = EventType.LRRP_REQUEST
    src: int
    tgt: int
    direction: str  # "Request" | "Response"


class EncryptionEvent(_BaseEvent):
    """Encrypted Link Control seen on a payload slot (voice is not decodable)."""

    type: Literal[EventType.ENCRYPTION] = EventType.ENCRYPTION
    slot: int
    flco: str  # e.g. "0x04"
    fid: str  # e.g. "0x80"


Event = Annotated[
    Union[
        SiteInfoEvent,
        ChannelStatusEvent,
        LSNStatusEvent,
        BankCallEvent,
        QualityEvent,
        VoiceCallEvent,
        PreambleCSBKEvent,
        DataHeaderEvent,
        IPMappingEvent,
        LRRPPositionEvent,
        LRRPRequestEvent,
        EncryptionEvent,
    ],
    Field(discriminator="type"),
]
