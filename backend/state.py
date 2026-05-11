"""In-memory state aggregator for the DMR monitoring dashboard.

The parser emits typed events one per stream line; this module consumes them
and maintains the "current truth" the UI needs:

  - Per-radio status (last seen, voice frame count, position, IP, encryption)
  - Per-slot active call (who is talking right now on slot 1 / slot 2)
  - System status (site id, rest LSN, per-LSN states, active bank calls)
  - Quality counters (CRC / FEC errors)

A `DashboardSnapshot` view can be requested at any time for WebSocket transport
or HTTP polling. Call cleanup (dropping a call that hasn't received a frame
for N seconds) is driven by an explicit `tick(now)` so replay against a
historical log uses the log's own timestamps as "now".
"""
from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Optional

from pydantic import BaseModel, Field

from .models import (
    BankCallEvent,
    ChannelStatusEvent,
    DataHeaderEvent,
    EncryptionEvent,
    Event,
    EventType,
    IPMappingEvent,
    LRRPPositionEvent,
    LRRPRequestEvent,
    LSNStatusEvent,
    PreambleCSBKEvent,
    QualityEvent,
    SiteInfoEvent,
    VoiceCallEvent,
)


class Position(BaseModel):
    lat: float
    lon: float
    at: datetime


class Radio(BaseModel):
    """Everything the device table / map row needs about one radio."""

    id: int
    first_seen: datetime
    last_seen: datetime
    ip: Optional[str] = None
    voice_frame_count: int = 0
    last_tg: Optional[int] = None
    last_slot: Optional[int] = None
    last_call_was_encrypted: bool = False
    last_position: Optional[Position] = None
    # Recent positions for drawing a movement trail on the map; capped to N.
    position_history: list[Position] = Field(default_factory=list)


class ActiveCall(BaseModel):
    """A call currently in progress on a payload slot."""

    slot: int
    src: int
    tgt: int
    is_cap_plus: bool = False
    is_txi: bool = False
    started_at: datetime
    last_frame_at: datetime
    frame_count: int = 1
    is_encrypted: bool = False
    rest_lsn: Optional[int] = None


class LSNStateSnapshot(BaseModel):
    state: str  # "Idle" | "Rest" | "Active"
    tg: Optional[int] = None
    updated_at: datetime


class SystemStatus(BaseModel):
    site: Optional[int] = None
    rest_lsn: Optional[int] = None
    lsn_states: dict[int, LSNStateSnapshot] = Field(default_factory=dict)
    last_seen: Optional[datetime] = None


class QualityCounters(BaseModel):
    csbk_crc: int = 0
    csbk_fec: int = 0
    cach_burst_fec: int = 0
    slco_crc: int = 0
    last_error_at: Optional[datetime] = None
    total_events_seen: int = 0


class DashboardSnapshot(BaseModel):
    """JSON-serializable view of the entire dashboard state."""

    radios: dict[int, Radio]
    active_calls: dict[int, ActiveCall]
    system: SystemStatus
    quality: QualityCounters
    generated_at: datetime


# ---------------------------------------------------------------------------


class StateManager:
    """Consume parser Events; expose a current-truth snapshot."""

    def __init__(
        self,
        call_idle_timeout: timedelta = timedelta(seconds=2),
        position_history_length: int = 50,
        on_call_start: Optional[Callable[[ActiveCall], None]] = None,
        on_call_end: Optional[Callable[[ActiveCall], None]] = None,
    ) -> None:
        self.radios: dict[int, Radio] = {}
        self.active_calls: dict[int, ActiveCall] = {}
        self.system = SystemStatus()
        self.quality = QualityCounters()
        self._call_idle_timeout = call_idle_timeout
        self._position_history_length = position_history_length
        self._last_event_at: Optional[datetime] = None
        self._on_call_start = on_call_start
        self._on_call_end = on_call_end

    # --- public API ---

    def apply(self, event: Event) -> None:
        self.quality.total_events_seen += 1
        self._last_event_at = event.timestamp

        et = event.type
        if et == EventType.VOICE_CALL:
            self._on_voice_call(event)
        elif et == EventType.PREAMBLE_CSBK:
            self._on_preamble_csbk(event)
        elif et == EventType.DATA_HEADER:
            self._on_data_header(event)
        elif et == EventType.IP_MAPPING:
            self._on_ip_mapping(event)
        elif et == EventType.LRRP_POSITION:
            self._on_lrrp_position(event)
        elif et == EventType.LRRP_REQUEST:
            self._on_lrrp_request(event)
        elif et == EventType.ENCRYPTION:
            self._on_encryption(event)
        elif et == EventType.SITE_INFO:
            self._on_site_info(event)
        elif et == EventType.CHANNEL_STATUS:
            self._on_channel_status(event)
        elif et == EventType.LSN_STATUS:
            self._on_lsn_status(event)
        elif et == EventType.BANK_CALL:
            self._on_bank_call(event)
        elif et == EventType.QUALITY:
            self._on_quality(event)

        # Auto-tick on each event so idle calls expire while replaying a log.
        self._expire_idle_calls(event.timestamp)

    def tick(self, now: datetime) -> None:
        """Manually expire idle calls — useful when no events are flowing."""
        self._expire_idle_calls(now)

    def snapshot(self) -> DashboardSnapshot:
        return DashboardSnapshot(
            radios=dict(self.radios),
            active_calls=dict(self.active_calls),
            system=self.system,
            quality=self.quality,
            generated_at=self._last_event_at or datetime.now(),
        )

    def active_talkgroups(self) -> dict[int, int]:
        """Currently-active TGs from the per-LSN snapshot. {lsn: tg}."""
        return {
            lsn: snap.tg
            for lsn, snap in self.system.lsn_states.items()
            if snap.state == "Active" and snap.tg is not None
        }

    # --- helpers ---

    def _touch_radio(self, radio_id: int, ts: datetime) -> Radio:
        radio = self.radios.get(radio_id)
        if radio is None:
            radio = Radio(id=radio_id, first_seen=ts, last_seen=ts)
            self.radios[radio_id] = radio
        else:
            if ts > radio.last_seen:
                radio.last_seen = ts
        return radio

    def _expire_idle_calls(self, now: datetime) -> None:
        expired = [
            slot
            for slot, call in self.active_calls.items()
            if now - call.last_frame_at > self._call_idle_timeout
        ]
        for slot in expired:
            call = self.active_calls[slot]
            if self._on_call_end is not None:
                try:
                    self._on_call_end(call)
                except Exception:
                    pass
            del self.active_calls[slot]

    # --- per-event handlers ---

    def _on_voice_call(self, ev: VoiceCallEvent) -> None:
        # SRC=0 is the DSD-FME placeholder emitted before the embedded LC is
        # decoded — it does not correspond to a real radio. Skip it so we
        # don't accumulate a phantom "radio 0" with hundreds of frames.
        if ev.src == 0:
            return
        radio = self._touch_radio(ev.src, ev.timestamp)
        radio.voice_frame_count += 1
        radio.last_tg = ev.tgt
        radio.last_slot = ev.slot
        # New encryption status is reset to False here; an EncryptionEvent on
        # the same slot will flip it back to True for the current call.
        radio.last_call_was_encrypted = False

        existing = self.active_calls.get(ev.slot)
        if existing is not None and existing.src == ev.src and existing.tgt == ev.tgt:
            existing.last_frame_at = ev.timestamp
            existing.frame_count += 1
            if ev.rest_lsn is not None:
                existing.rest_lsn = ev.rest_lsn
        else:
            if existing is not None and self._on_call_end is not None:
                try:
                    self._on_call_end(existing)
                except Exception:
                    pass
            new_call = ActiveCall(
                slot=ev.slot,
                src=ev.src,
                tgt=ev.tgt,
                is_cap_plus=ev.is_cap_plus,
                is_txi=ev.is_txi,
                started_at=ev.timestamp,
                last_frame_at=ev.timestamp,
                rest_lsn=ev.rest_lsn,
            )
            self.active_calls[ev.slot] = new_call
            if self._on_call_start is not None:
                try:
                    self._on_call_start(new_call)
                except Exception:
                    pass

    def _on_preamble_csbk(self, ev: PreambleCSBKEvent) -> None:
        self._touch_radio(ev.src, ev.timestamp)
        # Also touch the target if it looks like a radio id (private addressing).
        if ev.addressing == "Individual":
            self._touch_radio(ev.tgt, ev.timestamp)

    def _on_data_header(self, ev: DataHeaderEvent) -> None:
        self._touch_radio(ev.src, ev.timestamp)

    def _on_ip_mapping(self, ev: IPMappingEvent) -> None:
        radio = self._touch_radio(ev.radio_id, ev.timestamp)
        radio.ip = ev.ip

    def _on_lrrp_position(self, ev: LRRPPositionEvent) -> None:
        if ev.src is None:
            return
        radio = self._touch_radio(ev.src, ev.timestamp)
        pos = Position(lat=ev.lat, lon=ev.lon, at=ev.timestamp)
        radio.last_position = pos
        radio.position_history.append(pos)
        if len(radio.position_history) > self._position_history_length:
            radio.position_history = radio.position_history[-self._position_history_length:]

    def _on_lrrp_request(self, ev: LRRPRequestEvent) -> None:
        self._touch_radio(ev.src, ev.timestamp)
        self._touch_radio(ev.tgt, ev.timestamp)

    def _on_encryption(self, ev: EncryptionEvent) -> None:
        call = self.active_calls.get(ev.slot)
        if call is not None:
            call.is_encrypted = True
            radio = self.radios.get(call.src)
            if radio is not None:
                radio.last_call_was_encrypted = True

    def _on_site_info(self, ev: SiteInfoEvent) -> None:
        self.system.site = ev.site
        self.system.rest_lsn = ev.rest_lsn
        self.system.last_seen = ev.timestamp

    def _on_channel_status(self, ev: ChannelStatusEvent) -> None:
        self.system.rest_lsn = ev.rest_lsn
        self.system.last_seen = ev.timestamp

    def _on_lsn_status(self, ev: LSNStatusEvent) -> None:
        for s in ev.states:
            self.system.lsn_states[s.lsn] = LSNStateSnapshot(
                state=s.state, tg=s.tg, updated_at=ev.timestamp
            )
        self.system.last_seen = ev.timestamp

    def _on_bank_call(self, ev: BankCallEvent) -> None:
        # Bank announcements duplicate info that LSN_STATUS already conveys —
        # the bank/flag/description fields are informational. We mark the
        # system as alive but rely on LSN_STATUS for active-TG truth.
        self.system.last_seen = ev.timestamp

    def _on_quality(self, ev: QualityEvent) -> None:
        if ev.error_type == "CSBK_CRC":
            self.quality.csbk_crc += 1
        elif ev.error_type == "CSBK_FEC":
            self.quality.csbk_fec += 1
        elif ev.error_type == "CACH_BURST_FEC":
            self.quality.cach_burst_fec += 1
        elif ev.error_type == "SLCO_CRC":
            self.quality.slco_crc += 1
        self.quality.last_error_at = ev.timestamp
