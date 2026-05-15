"""Persistent event log for post-session debrief and CSV export.

Every parsed Event is fed to ``EventLog.append`` which:
  * appends a JSON line to ``jsonl_path`` (durable, survives a crash)
  * pushes the event into a bounded in-memory deque (fast filter for the UI)

The log is the source of truth for:
  * ``/api/events``     — recent events for the Debrief panel
  * ``/api/events.csv`` — CSV download of (filtered) events
  * ``/api/stats``      — distributions for the future statistics panel

The on-disk JSONL is append-only and never rewritten, so it can also be
consumed externally (e.g. ``jq``, pandas) without coordinating with the
live process.
"""
from __future__ import annotations

import csv
import io
import json
import threading
from collections import Counter, deque
from collections.abc import Iterable, Iterator
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from .models import Event, EventType


# Canonical flat schema used for CSV export. Order matters — it becomes the
# CSV header row. Fields not applicable to a given event type are left blank.
CSV_COLUMNS = [
    "timestamp",
    "type",
    "slot",
    "src",
    "tgt",
    "addressing",
    "kind",
    "delivery",
    "encrypted",
    "lat",
    "lon",
    "ip",
    "port",
    "error_type",
    "site",
    "rest_lsn",
    "raw_line",
]


def _row_from_event(ev: Event) -> dict[str, object]:
    """Flatten an Event into a CSV row dict using CSV_COLUMNS keys."""
    row: dict[str, object] = {k: "" for k in CSV_COLUMNS}
    row["timestamp"] = ev.timestamp.isoformat(timespec="seconds")
    row["type"] = ev.type.value
    raw = (ev.raw_line or "").strip().replace("\n", " ").replace("\r", " ")
    # Cap raw_line to keep CSV readable in spreadsheet apps.
    row["raw_line"] = raw[:240]

    et = ev.type
    if et == EventType.VOICE_CALL:
        row["slot"] = ev.slot
        row["src"] = ev.src
        row["tgt"] = ev.tgt
        row["rest_lsn"] = ev.rest_lsn if ev.rest_lsn is not None else ""
    elif et == EventType.PREAMBLE_CSBK:
        row["src"] = ev.src
        row["tgt"] = ev.tgt
        row["addressing"] = ev.addressing
        row["kind"] = ev.kind
        row["rest_lsn"] = ev.rest_lsn if ev.rest_lsn is not None else ""
    elif et == EventType.DATA_HEADER:
        row["slot"] = ev.slot
        row["src"] = ev.src
        row["tgt"] = ev.tgt
        row["addressing"] = ev.addressing
        row["delivery"] = ev.delivery
    elif et == EventType.IP_MAPPING:
        row["src"] = ev.radio_id
        row["ip"] = ev.ip
        row["port"] = ev.port
    elif et == EventType.LRRP_POSITION:
        row["src"] = ev.src if ev.src is not None else ""
        row["lat"] = ev.lat
        row["lon"] = ev.lon
    elif et == EventType.LRRP_REQUEST:
        row["src"] = ev.src
        row["tgt"] = ev.tgt
        row["kind"] = ev.direction
    elif et == EventType.ENCRYPTION:
        row["slot"] = ev.slot
        row["encrypted"] = "true"
    elif et == EventType.SITE_INFO:
        row["site"] = ev.site
        row["rest_lsn"] = ev.rest_lsn
    elif et == EventType.CHANNEL_STATUS:
        row["rest_lsn"] = ev.rest_lsn
    elif et == EventType.QUALITY:
        row["error_type"] = ev.error_type
    return row


class EventLog:
    """Ring-buffered, JSONL-persisted event log.

    The in-memory buffer is ``capacity`` events (default 20k) for fast
    filtering. The on-disk JSONL grows without bound — the user controls
    rotation externally.
    """

    def __init__(
        self,
        jsonl_path: Optional[Path] = None,
        capacity: int = 20_000,
    ) -> None:
        self.jsonl_path = jsonl_path
        self.capacity = capacity
        self._buf: deque[Event] = deque(maxlen=capacity)
        self._lock = threading.Lock()
        self._fh: Optional[io.TextIOBase] = None
        if jsonl_path is not None:
            jsonl_path.parent.mkdir(parents=True, exist_ok=True)
            # Line-buffered append; survives crashes line-by-line.
            self._fh = open(jsonl_path, "a", buffering=1, encoding="utf-8")

    def __len__(self) -> int:
        with self._lock:
            return len(self._buf)

    def close(self) -> None:
        with self._lock:
            if self._fh is not None:
                try:
                    self._fh.flush()
                    self._fh.close()
                finally:
                    self._fh = None

    # --- write path ---

    def append(self, ev: Event) -> None:
        with self._lock:
            self._buf.append(ev)
            if self._fh is not None:
                try:
                    self._fh.write(ev.model_dump_json() + "\n")
                except Exception:  # noqa: BLE001
                    pass  # never let logging take down the pipeline

    # --- read path ---

    def recent(
        self,
        limit: int = 500,
        since: Optional[datetime] = None,
        types: Optional[Iterable[str]] = None,
    ) -> list[Event]:
        type_set = {str(t) for t in types} if types else None
        with self._lock:
            snapshot = list(self._buf)
        out: list[Event] = []
        # Walk newest-first so we can early-cap at ``limit``.
        for ev in reversed(snapshot):
            if since is not None and ev.timestamp < since:
                break
            if type_set is not None and ev.type.value not in type_set:
                continue
            out.append(ev)
            if len(out) >= limit:
                break
        out.reverse()
        return out

    def iter_csv(
        self,
        since: Optional[datetime] = None,
        types: Optional[Iterable[str]] = None,
    ) -> Iterator[str]:
        """Yield CSV lines (including header). Streaming-friendly."""
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        yield buf.getvalue()
        buf.seek(0); buf.truncate()

        type_set = {str(t) for t in types} if types else None
        with self._lock:
            snapshot = list(self._buf)
        for ev in snapshot:
            if since is not None and ev.timestamp < since:
                continue
            if type_set is not None and ev.type.value not in type_set:
                continue
            writer.writerow(_row_from_event(ev))
            yield buf.getvalue()
            buf.seek(0); buf.truncate()

    def stats(self) -> dict[str, object]:
        """Compute distributions over the in-memory window.

        Intentionally lightweight — the UI uses this to render the future
        statistics panel. Counters returned as plain dicts (JSON-friendly).
        """
        with self._lock:
            snapshot = list(self._buf)

        by_type: Counter[str] = Counter()
        calls_by_src: Counter[int] = Counter()
        calls_by_tg: Counter[int] = Counter()
        positions_by_src: Counter[int] = Counter()
        hourly: Counter[str] = Counter()
        encrypted_calls = 0
        quality_by_kind: Counter[str] = Counter()

        # Distinct calls: count one per (slot, src, tgt) transition.
        seen_call_key: dict[int, tuple[int, int]] = {}
        for ev in snapshot:
            by_type[ev.type.value] += 1
            hourly[ev.timestamp.strftime("%Y-%m-%d %H:00")] += 1
            if ev.type == EventType.VOICE_CALL:
                if ev.src == 0:
                    continue
                key = (ev.src, ev.tgt)
                if seen_call_key.get(ev.slot) != key:
                    seen_call_key[ev.slot] = key
                    calls_by_src[ev.src] += 1
                    calls_by_tg[ev.tgt] += 1
            elif ev.type == EventType.LRRP_POSITION:
                if ev.src is not None:
                    positions_by_src[ev.src] += 1
            elif ev.type == EventType.ENCRYPTION:
                encrypted_calls += 1
            elif ev.type == EventType.QUALITY:
                quality_by_kind[ev.error_type] += 1

        first_ts = snapshot[0].timestamp if snapshot else None
        last_ts = snapshot[-1].timestamp if snapshot else None
        return {
            "window_size": len(snapshot),
            "window_capacity": self.capacity,
            "first_event_at": first_ts.isoformat() if first_ts else None,
            "last_event_at": last_ts.isoformat() if last_ts else None,
            "events_by_type": dict(by_type),
            "calls_by_src": dict(calls_by_src.most_common(20)),
            "calls_by_tg": dict(calls_by_tg.most_common(20)),
            "positions_by_src": dict(positions_by_src.most_common(20)),
            "hourly": dict(sorted(hourly.items())),
            "encrypted_calls": encrypted_calls,
            "quality_by_kind": dict(quality_by_kind),
        }


def parse_since(value: Optional[str]) -> Optional[datetime]:
    """Accept either an ISO timestamp or a duration like '5m', '1h', '30s'.

    Returns ``None`` if the value is empty or unparseable.
    """
    if not value:
        return None
    s = value.strip()
    if not s:
        return None
    if len(s) > 1 and s[-1] in ("s", "m", "h", "d") and s[:-1].isdigit():
        n = int(s[:-1])
        unit = s[-1]
        delta = {
            "s": timedelta(seconds=n),
            "m": timedelta(minutes=n),
            "h": timedelta(hours=n),
            "d": timedelta(days=n),
        }[unit]
        return datetime.now() - delta
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None
