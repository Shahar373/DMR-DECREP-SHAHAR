"""Traffic-following decoder scheduler (Phase 8).

When the Cap+ site has more channels than the Pi can decode at once, we
don't need a decoder on every channel all the time — only on the ones
that are actually carrying (or about to carry) traffic. This scheduler
decides, moment to moment, which channel labels deserve a live decoder.

It's driven by two signals, both already available:

  * **Control-channel grants** — the site's control channel (always kept
    active) announces which LSN a call is assigned to; the channel plan
    maps LSN → frequency → channel label. This is the precise, primary
    signal.
  * **Energy detection** — an FFT of the wideband IQ gives per-channel
    power; a channel above threshold is carrying *something* even if we
    haven't decoded a grant for it yet. This is the fallback.

Channels stay active for ``hang_seconds`` after their last activity so a
decoder doesn't drop mid-transmission on a brief gap. Control channels
are always active. If more channels are active than ``max_active``, the
most-recently-active ones win (control always kept).

Pure policy — no hardware, no I/O, fully unit-tested.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from ..channel_plan import ChannelPlan
from ..models import Event, EventType

# Event types whose presence on a channel means that channel is carrying
# decodable traffic (keep its decoder fed).
_TRAFFIC_TYPES = {
    EventType.VOICE_CALL,
    EventType.DATA_HEADER,
    EventType.PREAMBLE_CSBK,
    EventType.LRRP_POSITION,
    EventType.LRRP_REQUEST,
}


class TrafficScheduler:
    def __init__(
        self,
        plan: ChannelPlan,
        hang_seconds: float = 4.0,
        max_active: Optional[int] = None,
        energy_threshold: float = 0.0,
    ) -> None:
        self.plan = plan
        self.hang = timedelta(seconds=hang_seconds)
        self.max_active = max_active
        self.energy_threshold = energy_threshold
        # label -> last activity time
        self._last_active: dict[str, datetime] = {}
        self._control = {c.label for c in plan.control_channels()}
        self._lsn_to_label = {
            c.lsn: c.label for c in plan.channels if c.lsn is not None
        }

    # --- signal ingestion ---

    def _bump(self, label: Optional[str], now: datetime) -> None:
        if label is None:
            return
        prev = self._last_active.get(label)
        if prev is None or now > prev:
            self._last_active[label] = now

    def on_event(self, ev: Event, now: Optional[datetime] = None) -> None:
        """Update activity from a decoded event.

        * an event decoded *on* a channel keeps that channel active;
        * a control-channel grant referencing an LSN activates the LSN's
          mapped channel (trunk-following: tune a decoder to the traffic).
        """
        ts = now or ev.timestamp
        if ev.type in _TRAFFIC_TYPES:
            self._bump(getattr(ev, "channel_label", None), ts)

        # LSN → channel grants (Cap+). Multiple event shapes carry LSN info.
        if ev.type == EventType.LSN_STATUS:
            for s in ev.states:
                if s.state == "Active":
                    self._bump(self._lsn_to_label.get(s.lsn), ts)
        elif ev.type == EventType.BANK_CALL:
            for lsn in (ev.lsn_to_tg or {}):
                self._bump(self._lsn_to_label.get(lsn), ts)
        # rest_lsn on voice/preamble points at the CURRENT rest channel, not
        # the traffic channel, so it is deliberately NOT used to activate.

    def update_energy(self, powers: dict[str, float],
                      now: Optional[datetime] = None) -> None:
        """Mark channels whose measured power exceeds the threshold active.

        ``powers`` is {label: linear_power} from ``energy.channel_powers``.
        """
        ts = now or datetime.now()
        for label, p in powers.items():
            if p >= self.energy_threshold:
                self._bump(label, ts)

    # --- decision ---

    def active_labels(self, now: Optional[datetime] = None) -> set[str]:
        """The channel labels that should have a live decoder right now."""
        ts = now or self._reference_now()
        active = set(self._control)  # control channels always decoded
        within_hang: list[tuple[datetime, str]] = []
        for label, last in self._last_active.items():
            if ts - last <= self.hang:
                within_hang.append((last, label))
        # Newest activity first, so the cap keeps the freshest channels.
        within_hang.sort(reverse=True)
        for _, label in within_hang:
            active.add(label)

        if self.max_active is not None and len(active) > self.max_active:
            # Keep all control channels, then fill remaining budget with the
            # most-recently-active non-control channels.
            budget = max(self.max_active, len(self._control))
            kept = set(self._control)
            for _, label in within_hang:
                if len(kept) >= budget:
                    break
                kept.add(label)
            active = kept
        return active

    def _reference_now(self) -> datetime:
        """Latest activity time seen, or wall clock if none — so decisions
        in replay track the log's own clock."""
        if self._last_active:
            return max(self._last_active.values())
        return datetime.now()
