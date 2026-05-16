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
import sys
import threading
from collections import Counter, deque
from collections.abc import Iterable, Iterator
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from .event_index import EventIndex
from .models import EVENT_SCHEMA_VERSION, Event, EventType


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


def _row_from_dict(obj: dict) -> dict[str, object]:
    """Flatten a JSON-serialised event dict into a CSV row dict.

    Used by both the live-buffer CSV export and the on-disk history export
    so the column shape stays identical.
    """
    row: dict[str, object] = {k: "" for k in CSV_COLUMNS}
    ts = obj.get("timestamp", "")
    if isinstance(ts, str) and "T" in ts:
        row["timestamp"] = ts.split(".")[0]  # drop sub-seconds for readability
    else:
        row["timestamp"] = ts
    row["type"] = obj.get("type", "")
    raw = (obj.get("raw_line") or "")
    if isinstance(raw, str):
        raw = raw.strip().replace("\n", " ").replace("\r", " ")
    row["raw_line"] = raw[:240] if isinstance(raw, str) else ""

    et = obj.get("type")
    if et == "voice_call":
        row["slot"] = obj.get("slot", "")
        row["src"] = obj.get("src", "")
        row["tgt"] = obj.get("tgt", "")
        if obj.get("rest_lsn") is not None:
            row["rest_lsn"] = obj["rest_lsn"]
    elif et == "preamble_csbk":
        row["src"] = obj.get("src", "")
        row["tgt"] = obj.get("tgt", "")
        row["addressing"] = obj.get("addressing", "")
        row["kind"] = obj.get("kind", "")
        if obj.get("rest_lsn") is not None:
            row["rest_lsn"] = obj["rest_lsn"]
    elif et == "data_header":
        row["slot"] = obj.get("slot", "")
        row["src"] = obj.get("src", "")
        row["tgt"] = obj.get("tgt", "")
        row["addressing"] = obj.get("addressing", "")
        row["delivery"] = obj.get("delivery", "")
    elif et == "ip_mapping":
        row["src"] = obj.get("radio_id", "")
        row["ip"] = obj.get("ip", "")
        row["port"] = obj.get("port", "")
    elif et == "lrrp_position":
        if obj.get("src") is not None:
            row["src"] = obj["src"]
        row["lat"] = obj.get("lat", "")
        row["lon"] = obj.get("lon", "")
    elif et == "lrrp_request":
        row["src"] = obj.get("src", "")
        row["tgt"] = obj.get("tgt", "")
        row["kind"] = obj.get("direction", "")
    elif et == "encryption":
        row["slot"] = obj.get("slot", "")
        row["encrypted"] = "true"
    elif et == "site_info":
        row["site"] = obj.get("site", "")
        row["rest_lsn"] = obj.get("rest_lsn", "")
    elif et == "channel_status":
        row["rest_lsn"] = obj.get("rest_lsn", "")
    elif et == "quality":
        row["error_type"] = obj.get("error_type", "")
    return row


def _row_from_event(ev: Event) -> dict[str, object]:
    """Flatten an Event into a CSV row dict using CSV_COLUMNS keys."""
    return _row_from_dict(ev.model_dump(mode="json"))


def stream_history(
    jsonl_path: Optional[Path],
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    src: Optional[int] = None,
    tgt: Optional[int] = None,
    types: Optional[Iterable[str]] = None,
) -> Iterator[dict]:
    """Stream parsed event dicts from a JSONL file with server-side filtering.

    Designed for the /debrief browser: the file may grow unbounded, so we
    iterate line-by-line without loading it all into memory. Malformed
    lines (e.g. a half-written tail line during live append) are silently
    skipped.
    """
    if jsonl_path is None or not jsonl_path.exists():
        return
    type_set = {str(t) for t in types} if types else None
    with open(jsonl_path, "r", encoding="utf-8", errors="replace") as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError:
                continue
            ts_str = obj.get("timestamp")
            if not ts_str:
                continue
            try:
                ts = datetime.fromisoformat(ts_str)
            except (TypeError, ValueError):
                continue
            if since is not None and ts < since:
                continue
            if until is not None and ts > until:
                continue
            if type_set is not None and obj.get("type") not in type_set:
                continue
            if src is not None:
                row_src = obj.get("src")
                if row_src is None:
                    row_src = obj.get("radio_id")
                if row_src != src:
                    continue
            if tgt is not None and obj.get("tgt") != tgt:
                continue
            yield obj


def iter_history_csv(
    jsonl_path: Optional[Path],
    **filters,
) -> Iterator[str]:
    """Yield CSV lines (header + filtered rows) from a JSONL history file."""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    yield buf.getvalue()
    buf.seek(0); buf.truncate()
    for obj in stream_history(jsonl_path, **filters):
        writer.writerow(_row_from_dict(obj))
        yield buf.getvalue()
        buf.seek(0); buf.truncate()


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
        db_path: Optional[Path] = None,
        enable_index: bool = True,
    ) -> None:
        self.jsonl_path = jsonl_path
        self.capacity = capacity
        self._buf: deque[Event] = deque(maxlen=capacity)
        self._lock = threading.Lock()
        self._fh: Optional[io.TextIOBase] = None
        self._index: Optional[EventIndex] = None
        self._index_failed_once = False
        if jsonl_path is not None:
            jsonl_path.parent.mkdir(parents=True, exist_ok=True)
            # Line-buffered append; survives crashes line-by-line.
            self._fh = open(jsonl_path, "a", buffering=1, encoding="utf-8")
            if enable_index:
                resolved_db = db_path if db_path is not None else jsonl_path.with_suffix(".db")
                try:
                    self._index = EventIndex(resolved_db, schema_version=EVENT_SCHEMA_VERSION)
                except Exception as exc:  # noqa: BLE001
                    print(
                        f"# event index: failed to open ({exc}); "
                        "running with JSONL-only history",
                        file=sys.stderr,
                    )
                    self._index = None

    def __len__(self) -> int:
        with self._lock:
            return len(self._buf)

    def prime_from_jsonl(self, path: Optional[Path] = None) -> int:
        """Refill the in-memory ring buffer from the on-disk JSONL.

        Called at startup so the live ``/api/events`` feed (Debrief panel
        on the dashboard) doesn't go blank for a few minutes after a
        restart. Reads the whole file forward; the deque's maxlen takes
        care of keeping only the most recent ``capacity`` entries.

        Returns the number of events loaded into the buffer.
        """
        from pydantic import TypeAdapter
        from .models import Event as _Event
        target = path or self.jsonl_path
        if target is None or not target.exists():
            return 0
        ta = TypeAdapter(_Event)
        loaded = 0
        with self._lock:
            with open(target, "r", encoding="utf-8", errors="replace") as f:
                for raw in f:
                    raw = raw.strip()
                    if not raw:
                        continue
                    try:
                        obj = json.loads(raw)
                        ev = ta.validate_python(obj)
                    except Exception:  # noqa: BLE001 — skip junk lines
                        continue
                    self._buf.append(ev)
                    loaded += 1
        return loaded

    def close(self) -> None:
        with self._lock:
            if self._fh is not None:
                try:
                    self._fh.flush()
                    self._fh.close()
                finally:
                    self._fh = None
            if self._index is not None:
                try:
                    self._index.close()
                finally:
                    self._index = None

    @property
    def index(self) -> Optional[EventIndex]:
        return self._index

    # --- write path ---

    def append(self, ev: Event) -> None:
        with self._lock:
            self._buf.append(ev)
            if self._fh is not None:
                try:
                    self._fh.write(ev.model_dump_json() + "\n")
                except Exception:  # noqa: BLE001
                    pass  # never let logging take down the pipeline
            if self._index is not None:
                try:
                    self._index.append(ev.model_dump(mode="json"))
                except Exception as exc:  # noqa: BLE001
                    # JSONL is the source of truth — an index failure must
                    # never break the live monitor. Warn once per session.
                    if not self._index_failed_once:
                        print(
                            f"# event index: append failed ({exc}); "
                            "continuing without index",
                            file=sys.stderr,
                        )
                        self._index_failed_once = True

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
        # Events are appended chronologically, so once we cross `since` no
        # earlier entry can match. Skip the prefix in one shot.
        start = 0
        if since is not None:
            for start, ev in enumerate(snapshot):
                if ev.timestamp >= since:
                    break
            else:
                start = len(snapshot)
        for ev in snapshot[start:]:
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
            "quality_ratios": compute_quality_ratios(dict(by_type), dict(quality_by_kind)),
        }


# Verdict thresholds, expressed as decoded-CRC-error rate against successful
# decodes. Calibrated for a typical Cap+ link where the control channel is
# the dominant traffic — sub-1% is "I cannot tell this isn't fibre", and
# above 15% the system is effectively unusable.
_VERDICT_THRESHOLDS = [
    (0.01, "excellent", "Strong signal — RF chain is healthy."),
    (0.03, "good",      "Healthy link. Minor losses are normal in DMR."),
    (0.10, "marginal",  "Drop in SNR — check antenna placement / cable."),
    (0.20, "poor",      "Bad SNR — re-aim antenna, add LNA, or move site."),
    (1.01, "unusable",  "RF chain or site is broken. Most frames are lost."),
]


def _verdict_for(rate: float) -> tuple[str, str]:
    for threshold, name, hint in _VERDICT_THRESHOLDS:
        if rate < threshold:
            return name, hint
    return "unusable", _VERDICT_THRESHOLDS[-1][2]


def compute_quality_ratios(
    events_by_type: dict[str, int],
    quality_by_kind: dict[str, int],
) -> dict[str, object]:
    """Turn raw counters into error-rate ratios per CSBK/CACH/SLCO channel.

    Denominators are approximate — they're the count of *successful* decodes
    that came from the same underlying frame class:

      * CSBK frames → site_info + channel_status + preamble_csbk + bank_call
      * CACH bursts → lsn_status
      * Voice (SLCO) → voice_call frames

    The errors are the per-kind QualityEvent counters from dsd-fme's own
    error lines. ratio = errors / (errors + successful_decodes).

    Returned shape (JSON-friendly):
      {"overall": {errors, decodes, rate, verdict, hint},
       "csbk_crc": {errors, decodes, rate},
       "csbk_fec": {...}, "cach_fec": {...}, "slco_crc": {...}}
    """
    def _ratio(errs: int, ok: int) -> float:
        total = errs + ok
        return (errs / total) if total else 0.0

    csbk_ok = (events_by_type.get("site_info", 0)
               + events_by_type.get("channel_status", 0)
               + events_by_type.get("preamble_csbk", 0)
               + events_by_type.get("bank_call", 0))
    cach_ok = events_by_type.get("lsn_status", 0)
    voice_ok = events_by_type.get("voice_call", 0)

    csbk_crc = quality_by_kind.get("CSBK_CRC", 0)
    csbk_fec = quality_by_kind.get("CSBK_FEC", 0)
    cach_fec = quality_by_kind.get("CACH_BURST_FEC", 0)
    slco_crc = quality_by_kind.get("SLCO_CRC", 0)

    # CRC errors mean the FEC failed and the frame was actually lost.
    # FEC errors mean FEC was needed — a warning indicator, not a loss.
    # The "overall" verdict uses CRCs only so it reflects lost frames.
    overall_errs = csbk_crc + slco_crc
    overall_ok = csbk_ok + voice_ok
    overall_rate = _ratio(overall_errs, overall_ok)
    verdict, hint = _verdict_for(overall_rate)

    return {
        "overall": {
            "errors": overall_errs,
            "decodes": overall_ok,
            "rate": overall_rate,
            "verdict": verdict,
            "hint": hint,
        },
        "csbk_crc": {"errors": csbk_crc, "decodes": csbk_ok,
                     "rate": _ratio(csbk_crc, csbk_ok)},
        "csbk_fec": {"errors": csbk_fec, "decodes": csbk_ok,
                     "rate": _ratio(csbk_fec, csbk_ok)},
        "cach_fec": {"errors": cach_fec, "decodes": cach_ok,
                     "rate": _ratio(cach_fec, cach_ok)},
        "slco_crc": {"errors": slco_crc, "decodes": voice_ok,
                     "rate": _ratio(slco_crc, voice_ok)},
    }


def quality_ratios_over_window(
    jsonl_path: Optional[Path],
    window_seconds: int,
    now: Optional[datetime] = None,
    index: Optional[EventIndex] = None,
) -> dict[str, object]:
    """Compute quality ratios over a fixed time window from the JSONL.

    Unlike ``EventLog.stats()`` which aggregates over the in-memory ring
    buffer (a few minutes of busy-channel data), this scans the on-disk
    JSONL and aggregates only events whose timestamp falls in the last
    ``window_seconds``. The result carries the actual sample bounds so
    the UI can label the chart honestly ("23,415 events from 13:22 to
    14:22").

    Falls back to an empty result if the JSONL is missing.
    """
    now = now or datetime.now()
    since = now - timedelta(seconds=window_seconds)

    by_type: Counter[str] = Counter()
    quality: Counter[str] = Counter()
    earliest: Optional[datetime] = None
    latest: Optional[datetime] = None
    sample = 0

    if index is not None and index.count() > 0:
        by_type_map = index.count_by_type(since=since)
        for k, v in by_type_map.items():
            by_type[k] = v
            sample += v
        for k, v in index.count_quality_by_kind(since=since).items():
            quality[k] = v
        min_ts, max_ts = index.time_bounds(since=since)
        if isinstance(min_ts, str):
            try:
                earliest = datetime.fromisoformat(min_ts)
            except ValueError:
                pass
        if isinstance(max_ts, str):
            try:
                latest = datetime.fromisoformat(max_ts)
            except ValueError:
                pass
    elif jsonl_path is not None and jsonl_path.exists():
        for obj in stream_history(jsonl_path, since=since):
            sample += 1
            et = obj.get("type")
            if et:
                by_type[et] += 1
            if et == "quality":
                quality[obj.get("error_type", "")] += 1
            ts_str = obj.get("timestamp")
            if isinstance(ts_str, str):
                try:
                    ts = datetime.fromisoformat(ts_str)
                except ValueError:
                    continue
                if earliest is None or ts < earliest:
                    earliest = ts
                if latest is None or ts > latest:
                    latest = ts

    ratios = compute_quality_ratios(dict(by_type), dict(quality))
    ratios["window_seconds"] = window_seconds
    ratios["window_start"] = earliest.isoformat() if earliest else None
    ratios["window_end"] = latest.isoformat() if latest else None
    ratios["sample_events"] = sample
    ratios["events_by_type"] = dict(by_type)
    ratios["quality_by_kind"] = dict(quality)
    return ratios


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
