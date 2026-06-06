"""Alerts Engine — rule-based notifications driven by the live event stream.

Pluggable rule kinds (discriminated by ``kind``):

* ``radio_keyup``    — a watched radio id starts a new voice call
* ``encryption``     — an encrypted call appears (optionally filtered by TG)
* ``cc_silent``      — the control channel goes quiet for too long
* ``quality_spike``  — CRC error rate over a rolling window crosses a threshold

The ``Evaluator`` consumes every parsed ``Event`` via ``on_event()`` and runs
``tick(now)`` from a periodic task for the time-based rules. Firings are
recorded into a bounded in-memory deque and pushed to any WebSocket
subscribers (the dashboard's toast bar).

Rules are persisted to a small JSON file (atomic write) so they survive a
service restart. Firings are intentionally in-memory only for v0.13.0 —
external durability is the operator's job (forward to syslog / external
alertmanager later).
"""
from __future__ import annotations

import asyncio
import json
import sys
import threading
import uuid
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Literal, Optional, Union

from pydantic import BaseModel, Field, TypeAdapter

from .models import Event, EventType


# ---------------------------------------------------------------------------
# Rule models
# ---------------------------------------------------------------------------


class _RuleBase(BaseModel):
    id: str = Field(default_factory=lambda: uuid.uuid4().hex[:12])
    name: str
    enabled: bool = True
    # Minimum seconds between consecutive firings of the same rule. Prevents
    # a chatty event source from flooding the toast bar.
    cooldown_seconds: int = Field(default=60, ge=0)
    created_at: datetime = Field(default_factory=lambda: datetime.now())


class RadioKeyupRule(_RuleBase):
    kind: Literal["radio_keyup"] = "radio_keyup"
    radio_ids: list[int] = Field(min_length=1)


class EncryptionRule(_RuleBase):
    kind: Literal["encryption"] = "encryption"
    # Empty list = any TG. Otherwise: fire only when the encrypted slot's
    # active call's tgt is in this list.
    tg_ids: list[int] = Field(default_factory=list)


class CcSilentRule(_RuleBase):
    kind: Literal["cc_silent"] = "cc_silent"
    timeout_seconds: int = Field(default=30, ge=5)


class QualitySpikeRule(_RuleBase):
    kind: Literal["quality_spike"] = "quality_spike"
    window_seconds: int = Field(default=60, ge=10)
    # Overall CRC rate (errors / (errors + decodes)) that triggers the rule.
    # 0.05 = 5%. Matches the "marginal" verdict band of compute_quality_ratios.
    rate_threshold: float = Field(default=0.05, gt=0, le=1.0)


Rule = Annotated[
    Union[RadioKeyupRule, EncryptionRule, CcSilentRule, QualitySpikeRule],
    Field(discriminator="kind"),
]

_RULE_ADAPTER = TypeAdapter(Rule)
_RULE_LIST_ADAPTER = TypeAdapter(list[Rule])


# ---------------------------------------------------------------------------
# Firings
# ---------------------------------------------------------------------------


class AlertFiring(BaseModel):
    rule_id: str
    rule_name: str
    kind: str
    fired_at: datetime
    message: str
    # Arbitrary per-kind payload (src/tgt/slot, silent_for, rate, etc.) that
    # the UI may render alongside the message.
    context: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Evaluator
# ---------------------------------------------------------------------------


class Evaluator:
    """Holds the active rule set; runs them on each event + periodic tick.

    Thread-safe for the parser-thread (``on_event``) ↔ FastAPI-worker
    (``add_rule`` / ``remove_rule`` / ``subscribe``) crossover.
    """

    def __init__(
        self,
        rules_path: Optional[Path] = None,
        firings_capacity: int = 200,
        event_log=None,
    ) -> None:
        self.rules_path = Path(rules_path) if rules_path else None
        self.rules: list[Rule] = []
        self.firings: deque[AlertFiring] = deque(maxlen=firings_capacity)
        self.subscribers: set[asyncio.Queue] = set()
        self._lock = threading.Lock()
        self._event_log = event_log
        # Per-rule cooldown clock.
        self._last_fired: dict[str, datetime] = {}
        # radio_keyup: per-rule per-slot last (src, tgt) so a 100-frame call
        # doesn't fire 100 times.
        self._last_call_key: dict[str, dict[int, tuple[int, int]]] = {}
        # encryption: cache the most recent (src, tgt) per slot from
        # voice_call events so an EncryptionEvent (which only carries slot)
        # can join against the active TG.
        self._slot_call: dict[int, tuple[int, int]] = {}
        # cc_silent: track last CC heartbeat + which rules already fired
        # during the current silent run (cleared when CC returns).
        self._last_cc_at: Optional[datetime] = None
        self._cc_silent_fired: set[str] = set()

        if self.rules_path is not None and self.rules_path.exists():
            self.load_rules()

    # ── rule CRUD ──────────────────────────────────────────────────────

    def list_rules(self) -> list[Rule]:
        with self._lock:
            return list(self.rules)

    def add_rule(self, rule: Rule) -> Rule:
        with self._lock:
            self.rules.append(rule)
            self._save_rules_locked()
        return rule

    def remove_rule(self, rule_id: str) -> bool:
        with self._lock:
            before = len(self.rules)
            self.rules = [r for r in self.rules if r.id != rule_id]
            removed = len(self.rules) != before
            if removed:
                self._last_fired.pop(rule_id, None)
                self._last_call_key.pop(rule_id, None)
                self._cc_silent_fired.discard(rule_id)
                self._save_rules_locked()
        return removed

    def set_enabled(self, rule_id: str, enabled: bool) -> bool:
        with self._lock:
            for r in self.rules:
                if r.id == rule_id:
                    r.enabled = enabled
                    self._save_rules_locked()
                    return True
        return False

    def load_rules(self) -> int:
        """Read rules from disk. Corrupt files are renamed aside, not lost.

        Falls back to ``rules_path.bak`` (kept by ``atomic_write_text``) if
        the main file is missing or unparseable — covers the case where a
        power-yank truncated the current rules file but the previous
        version is still good on disk.
        """
        if self.rules_path is None:
            return 0
        bak_path = self.rules_path.with_suffix(self.rules_path.suffix + ".bak")
        for candidate in (self.rules_path, bak_path):
            if not candidate.exists() or candidate.stat().st_size == 0:
                continue
            try:
                raw = candidate.read_text(encoding="utf-8")
                data = json.loads(raw)
                rules = _RULE_LIST_ADAPTER.validate_python(data)
            except Exception:  # noqa: BLE001 — try next candidate or give up
                continue
            if candidate is bak_path:
                print(
                    f"# alerts: primary rules file unreadable, recovered "
                    f"from {bak_path.name}",
                    file=sys.stderr,
                )
            with self._lock:
                self.rules = list(rules)
            return len(rules)
        # Neither file parsed — quarantine the main one so the operator
        # can inspect it and we start with an empty rule set.
        if self.rules_path.exists():
            try:
                self.rules_path.rename(
                    self.rules_path.with_suffix(self.rules_path.suffix + ".bad")
                )
            except OSError:
                pass
        return 0

    def _save_rules_locked(self) -> None:
        if self.rules_path is None:
            return
        from .state import atomic_write_text  # local import — avoid cycle

        payload = json.dumps(
            _RULE_LIST_ADAPTER.dump_python(self.rules, mode="json"),
            indent=2, default=str,
        )
        try:
            atomic_write_text(self.rules_path, payload)
        except Exception:  # noqa: BLE001 — never let alert persistence break
            pass

    # ── firings + subscribers ──────────────────────────────────────────

    def recent_firings(self, limit: int = 100) -> list[AlertFiring]:
        with self._lock:
            snap = list(self.firings)
        return snap[-limit:][::-1]  # newest first

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=50)
        # subscribers is touched from both the asyncio thread (subscribe/
        # unsubscribe via WS handlers) and the parser thread (_record's
        # fan-out + dead-queue discard), so all mutations go through _lock.
        with self._lock:
            self.subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        with self._lock:
            self.subscribers.discard(q)

    # ── evaluation hooks ───────────────────────────────────────────────

    def on_event(self, ev: Event) -> None:
        """Feed one event through every enabled rule."""
        with self._lock:
            # Side caches first (so rules can join against them).
            # Must be inside the lock — tick() runs in a worker thread
            # (asyncio.to_thread) and reads these caches concurrently.
            if ev.type == EventType.VOICE_CALL and ev.src != 0:
                self._slot_call[ev.slot] = (ev.src, ev.tgt)
            if ev.type in (
                EventType.SITE_INFO, EventType.LSN_STATUS, EventType.CHANNEL_STATUS,
            ):
                self._last_cc_at = ev.timestamp
                # CC came back — reset silent-run latches so they can re-fire on
                # the next outage.
                self._cc_silent_fired.clear()
            rules = list(self.rules)

        for rule in rules:
            if not rule.enabled:
                continue
            with self._lock:
                firing = self._evaluate_event(rule, ev)
            if firing is not None:
                self._record(firing)

    def tick(self, now: Optional[datetime] = None) -> None:
        """Run the time-based rules (cc_silent, quality_spike).

        Called from a worker thread via ``asyncio.to_thread``; cache reads
        inside ``_evaluate_tick`` are themselves lock-guarded so we can
        release the lock during the slow ``quality_ratios_over_window``
        query without blocking the parser path."""
        now = now or datetime.now()
        with self._lock:
            rules = list(self.rules)
        for rule in rules:
            if not rule.enabled:
                continue
            firing = self._evaluate_tick(rule, now)
            if firing is not None:
                self._record(firing)

    # ── per-rule logic ─────────────────────────────────────────────────

    def _evaluate_event(self, rule: Rule, ev: Event) -> Optional[AlertFiring]:
        if isinstance(rule, RadioKeyupRule) and ev.type == EventType.VOICE_CALL:
            if ev.src not in rule.radio_ids:
                return None
            last_keys = self._last_call_key.setdefault(rule.id, {})
            key = (ev.src, ev.tgt)
            if last_keys.get(ev.slot) == key:
                return None  # continuation of the same call, not a new keyup
            last_keys[ev.slot] = key
            if not self._cooldown_ok(rule, ev.timestamp):
                return None
            return AlertFiring(
                rule_id=rule.id, rule_name=rule.name, kind=rule.kind,
                fired_at=ev.timestamp,
                message=f"Radio {ev.src} keyed up → TG {ev.tgt} (slot {ev.slot})",
                context={"src": ev.src, "tgt": ev.tgt, "slot": ev.slot},
            )

        if isinstance(rule, EncryptionRule) and ev.type == EventType.ENCRYPTION:
            slot_info = self._slot_call.get(ev.slot)
            src, tg = (slot_info if slot_info else (None, None))
            if rule.tg_ids and (tg is None or tg not in rule.tg_ids):
                return None
            if not self._cooldown_ok(rule, ev.timestamp):
                return None
            bits = [f"Encrypted slot {ev.slot}"]
            if tg is not None:
                bits.append(f"→ TG {tg}")
            if src is not None:
                bits.append(f"SRC {src}")
            return AlertFiring(
                rule_id=rule.id, rule_name=rule.name, kind=rule.kind,
                fired_at=ev.timestamp,
                message=" ".join(bits),
                context={"slot": ev.slot, "tg": tg, "src": src,
                         "flco": ev.flco, "fid": ev.fid},
            )

        return None

    def _evaluate_tick(self, rule: Rule, now: datetime) -> Optional[AlertFiring]:
        if isinstance(rule, CcSilentRule):
            # Snapshot mutable cache state under the lock; release before the
            # arithmetic + AlertFiring construction to keep the critical
            # section narrow.
            with self._lock:
                if rule.id in self._cc_silent_fired:
                    return None
                last_cc_at = self._last_cc_at
            if last_cc_at is None:
                return None  # cold start — no baseline yet
            silent_for = (now - last_cc_at).total_seconds()
            if silent_for < rule.timeout_seconds:
                return None
            with self._lock:
                # Re-check after lock re-acquisition — another tick may have
                # latched in the meantime; we don't want to fire twice.
                if rule.id in self._cc_silent_fired:
                    return None
                self._cc_silent_fired.add(rule.id)
            return AlertFiring(
                rule_id=rule.id, rule_name=rule.name, kind=rule.kind,
                fired_at=now,
                message=f"Control channel silent for {silent_for:.0f}s "
                        f"(threshold {rule.timeout_seconds}s)",
                context={"silent_for_seconds": silent_for,
                         "last_cc_at": last_cc_at.isoformat()},
            )

        if isinstance(rule, QualitySpikeRule):
            with self._lock:
                if not self._cooldown_ok(rule, now):
                    return None
            if self._event_log is None:
                return None
            try:
                # quality_ratios_over_window scans the JSONL/SQLite index —
                # potentially seconds on a big history — so we never hold the
                # evaluator lock during it.
                from .event_log import quality_ratios_over_window
                qr = quality_ratios_over_window(
                    self._event_log.jsonl_path,
                    window_seconds=rule.window_seconds,
                    now=now, index=self._event_log.index,
                )
            except Exception:  # noqa: BLE001 — alert eval must never break service
                return None
            overall = qr.get("overall", {})
            rate = float(overall.get("rate", 0) or 0)
            errors = int(overall.get("errors", 0) or 0)
            if errors == 0 or rate < rule.rate_threshold:
                return None
            return AlertFiring(
                rule_id=rule.id, rule_name=rule.name, kind=rule.kind,
                fired_at=now,
                message=(f"Quality spike: overall CRC rate {rate*100:.2f}% "
                         f"≥ {rule.rate_threshold*100:.2f}% "
                         f"({errors} errors in {rule.window_seconds}s)"),
                context={"rate": rate, "errors": errors,
                         "decodes": int(overall.get("decodes", 0) or 0),
                         "window_seconds": rule.window_seconds,
                         "verdict": overall.get("verdict")},
            )

        return None

    # ── internals ──────────────────────────────────────────────────────

    def _cooldown_ok(self, rule: Rule, when: datetime) -> bool:
        last = self._last_fired.get(rule.id)
        if last is None:
            return True
        return (when - last).total_seconds() >= rule.cooldown_seconds

    def _record(self, firing: AlertFiring) -> None:
        with self._lock:
            # _last_fired feeds _cooldown_ok in both on_event (asyncio
            # thread) and _evaluate_tick (worker thread); the write has to
            # be under the same lock as the reads.
            self._last_fired[firing.rule_id] = firing.fired_at
            self.firings.append(firing)
            subs = list(self.subscribers)
        payload = firing.model_dump_json()
        dead: list[asyncio.Queue] = []
        for q in subs:
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                dead.append(q)
        if dead:
            # Evict in one critical section instead of mutating per-failure
            # outside the lock (which raced against subscribe/unsubscribe).
            with self._lock:
                for q in dead:
                    self.subscribers.discard(q)


def rule_from_dict(data: dict) -> Rule:
    """Validate a JSON-shaped rule dict (e.g. from a POST body)."""
    return _RULE_ADAPTER.validate_python(data)
