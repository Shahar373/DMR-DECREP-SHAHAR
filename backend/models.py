"""Event models emitted by the DSD-FME log parser.

Pydantic models with a discriminated union over `type` so callers can
match on `event.type` without isinstance() chains.
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Annotated, Literal, Optional, Union

from pydantic import BaseModel, Field


class EventType(str, Enum):
    VOICE_START = "voice_start"
    VOICE_END = "voice_end"
    LRRP = "lrrp"
    ARS = "ars"
    CSBK = "csbk"
    ENCRYPTION = "encryption"


class _BaseEvent(BaseModel):
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    raw_line: str


class VoiceCallEvent(_BaseEvent):
    type: Literal[EventType.VOICE_START] = EventType.VOICE_START
    src_id: int
    tgt_id: int
    slot: int
    encrypted: bool = False


class VoiceEndEvent(_BaseEvent):
    type: Literal[EventType.VOICE_END] = EventType.VOICE_END
    slot: int


class LRRPEvent(_BaseEvent):
    type: Literal[EventType.LRRP] = EventType.LRRP
    src_id: int
    lat: float
    lon: float


class ARSEvent(_BaseEvent):
    type: Literal[EventType.ARS] = EventType.ARS
    src_id: int
    registered: bool


class CSBKEvent(_BaseEvent):
    type: Literal[EventType.CSBK] = EventType.CSBK
    src_id: Optional[int] = None


class EncryptionEvent(_BaseEvent):
    type: Literal[EventType.ENCRYPTION] = EventType.ENCRYPTION
    src_id: Optional[int] = None
    alg_id: Optional[str] = None


Event = Annotated[
    Union[
        VoiceCallEvent,
        VoiceEndEvent,
        LRRPEvent,
        ARSEvent,
        CSBKEvent,
        EncryptionEvent,
    ],
    Field(discriminator="type"),
]
