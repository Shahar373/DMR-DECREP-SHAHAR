"""FastAPI server for the DMR Cap+ Monitor dashboard.

Exposes:
  GET  /                       → serves frontend/index.html
  GET  /api/snapshot           → current DashboardSnapshot as JSON
  GET  /api/recordings         → list per-call WAVs written by dsd-fme
  GET  /recordings/{filename}  → stream a WAV file
  WS   /ws                     → pushes a snapshot JSON every broadcast cycle
"""
from __future__ import annotations

import asyncio
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import (
    FastAPI,
    Header,
    HTTPException,
    Query,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import FileResponse, HTMLResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

from . import __build_date__, __version__
from .alerts import Evaluator, rule_from_dict
from .event_log import (
    CSV_COLUMNS,
    EventLog,
    _row_from_dict,
    iter_history_csv,
    parse_since,
    quality_ratios_over_window,
    stream_history,
)
from .recordings import RecordingRegistry
from .state import StateManager

app = FastAPI(title="DMR Cap+ Monitor", docs_url=None, redoc_url=None)

_state: Optional[StateManager] = None
_subscribers: set[asyncio.Queue] = set()
_recordings: Optional[RecordingRegistry] = None
_event_log: Optional[EventLog] = None
# When the server process came up — used by ``/api/health`` to report
# uptime. Pinned at import time so it's stable across requests.
_started_at: datetime = datetime.now()
# Last voice frame seen — distinct from "any event" so an idle channel
# (quality errors still arriving from CC) doesn't look healthy to an
# operator who actually cares about traffic.
_last_voice_at: Optional[datetime] = None

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"

# Shared static assets (design tokens CSS, shared header JS). Mounted only
# when the directory exists so older checkouts keep working.
_ASSETS_DIR = FRONTEND_DIR / "assets"
if _ASSETS_DIR.is_dir():
    app.mount("/assets", StaticFiles(directory=str(_ASSETS_DIR)), name="assets")

# HTML pages cached in memory at first hit — previously every page load did
# a fresh read_text() from the SD card. --dev-reload-html re-reads on every
# request for frontend iteration without restarts.
_PAGE_CACHE: dict[str, str] = {}
_dev_reload_html = False


def set_dev_reload_html(enabled: bool) -> None:
    global _dev_reload_html
    _dev_reload_html = enabled


def _page(name: str) -> HTMLResponse:
    if _dev_reload_html or name not in _PAGE_CACHE:
        _PAGE_CACHE[name] = (FRONTEND_DIR / name).read_text(encoding="utf-8")
    return HTMLResponse(
        _PAGE_CACHE[name], headers={"Cache-Control": "no-cache"},
    )


# ── Heavy-endpoint concurrency guard ────────────────────────────────────
# The full-scan endpoints (/api/history, /api/network, /api/radio,
# /api/quality) each run seconds of SQLite/JSONL work on a worker thread.
# Unbounded, a handful of concurrent dashboard tabs exhausts the small
# default to_thread pool on a Pi and starves every other request. Two run
# at a time; a short queue absorbs bursts; beyond that we shed load with
# 503 + Retry-After instead of queueing forever.
_HEAVY_CONCURRENCY = 2
_HEAVY_MAX_QUEUE = 4
_heavy_sem = asyncio.Semaphore(_HEAVY_CONCURRENCY)
_heavy_waiting = 0


async def _run_heavy(fn, /, *args, **kwargs):
    global _heavy_waiting
    if _heavy_waiting >= _HEAVY_MAX_QUEUE:
        raise HTTPException(
            status_code=503,
            detail="server busy — too many concurrent heavy queries, retry shortly",
            headers={"Retry-After": "2"},
        )
    _heavy_waiting += 1
    try:
        async with _heavy_sem:
            return await asyncio.to_thread(fn, *args, **kwargs)
    finally:
        _heavy_waiting -= 1


# Guard for the destructive /api/reset endpoint. When a token is
# configured (--reset-token) the X-Reset-Token header must match; without
# one, only loopback clients may reset.
_reset_token: Optional[str] = None


def attach_reset_token(token: Optional[str]) -> None:
    global _reset_token
    _reset_token = token or None


def note_voice_event(ts: datetime) -> None:
    """Bump the last-voice-seen marker (called by the wrapper on each
    voice_call event so /api/health can answer "are radios actually
    talking, not just is the CC alive")."""
    global _last_voice_at
    if _last_voice_at is None or ts > _last_voice_at:
        _last_voice_at = ts


def attach_state(sm: StateManager) -> None:
    global _state, _broadcast_payload
    _state = sm
    # A payload from a previously-attached StateManager is stale by
    # definition (matters for tests that re-attach fresh state).
    _broadcast_payload = None


def attach_recordings(r: RecordingRegistry) -> None:
    global _recordings
    _recordings = r


def attach_event_log(log: EventLog) -> None:
    global _event_log
    _event_log = log


_snapshot_path: Optional[Path] = None


def attach_snapshot_path(path: Optional[Path]) -> None:
    """Tell /api/health where the snapshot.json lives, so it can report
    the file's size and whether it's actually being written."""
    global _snapshot_path
    _snapshot_path = path


_evaluator: Optional[Evaluator] = None


def attach_evaluator(ev: Optional[Evaluator]) -> None:
    """Wire the Alerts Engine into the HTTP/WS routes."""
    global _evaluator
    _evaluator = ev


# The most recent broadcast payload (trimmed snapshot, already serialised).
# Produced once per tick by the CLI's snapshot task and reused by the WS
# fan-out, new WS connections, AND /api/snapshot — previously each of those
# re-serialised the full unbounded snapshot (on the event loop for HTTP),
# which grew with every radio ever heard and froze the live feed under load.
_broadcast_payload: Optional[str] = None


async def push_snapshot(payload: Optional[str] = None) -> None:
    """Fan a snapshot payload out to every connected WS client.

    ``payload`` is the pre-serialised trimmed snapshot from the CLI tick.
    When omitted (tests, /api/reset), it is built here off-loop.
    """
    global _broadcast_payload
    if payload is None:
        if _state is None:
            return
        sm = _state
        payload = await asyncio.to_thread(
            lambda: sm.snapshot(trim=True).model_dump_json()
        )
    _broadcast_payload = payload
    if not _subscribers:
        return
    dead: set[asyncio.Queue] = set()
    for q in _subscribers:
        try:
            q.put_nowait(payload)
        except asyncio.QueueFull:
            dead.add(q)
    # In-place mutation — using ``_subscribers -= dead`` here would rebind
    # the name and Python would treat _subscribers as local for the whole
    # function, raising UnboundLocalError on the early read above.
    _subscribers.difference_update(dead)


@app.get("/api/snapshot")
async def get_snapshot():
    if _broadcast_payload is not None:
        # Zero serialisation per request — the payload is refreshed every
        # broadcast tick (1 s in live mode), well within polling freshness.
        return Response(content=_broadcast_payload, media_type="application/json")
    if _state is None:
        return {}
    sm = _state
    return Response(
        content=await asyncio.to_thread(
            lambda: sm.snapshot(trim=True).model_dump_json()
        ),
        media_type="application/json",
    )


@app.get("/api/recordings")
async def list_recordings():
    if _recordings is None:
        return {"recordings": []}
    # list_recent() iterdir's the calls dir and opens each WAV header — after
    # weeks of recordings this is a synchronous scan of thousands of files,
    # polled every 3 s by the dashboard. Hand it to a worker thread.
    recent = await asyncio.to_thread(_recordings.list_recent)
    return {"recordings": [r.model_dump(mode="json") for r in recent]}


@app.get("/api/debug")
async def debug_info():
    # Two synchronous disk scans: list_recent() opens every WAV header, and
    # the iterdir+stat loop below stats every file. Off-loop together so a
    # debug-probe doesn't stall the live feed.
    return await asyncio.to_thread(_debug_info_sync)


def _debug_info_sync() -> dict:
    recs = _recordings.list_recent() if _recordings else []
    base = _recordings.base_dir if _recordings else None
    files_on_disk = []
    if base is not None and base.exists():
        for p in sorted(base.iterdir()):
            try:
                files_on_disk.append({"name": p.name, "bytes": p.stat().st_size})
            except OSError:
                pass
    return {
        "calls_dir": str(base) if base else None,
        "min_duration": _recordings.min_duration if _recordings else None,
        "recordings_visible": len(recs),
        "files_on_disk_total": len(files_on_disk),
        "files_on_disk": files_on_disk[:50],  # cap output
        "recordings": [r.model_dump(mode="json") for r in recs[:20]],
    }


@app.get("/api/version")
async def get_version():
    return {"version": __version__, "build_date": __build_date__}


@app.get("/api/health")
async def get_health():
    """Liveness + capacity snapshot for ops dashboards and watchdogs.

    Stable JSON shape; individual probes degrade to ``null`` rather than
    raising so a partial outage in one subsystem doesn't take the
    endpoint with it.
    """
    from .health import compute_health
    last_event_at = None
    if _state is not None:
        last_event_at = getattr(_state, "_last_event_at", None)
    jsonl_path = _event_log.history_path if _event_log is not None else None
    db_path = None
    writer_health: Optional[dict] = None
    if _event_log is not None:
        if _event_log.index is not None:
            db_path = _event_log.index.db_path
        try:
            writer_health = _event_log.write_health()
        except Exception:  # noqa: BLE001 — health must never raise
            writer_health = None
    calls_dir = _recordings.base_dir if _recordings is not None else None
    return compute_health(
        version=__version__,
        build_date=__build_date__,
        started_at=_started_at,
        last_event_at=last_event_at,
        last_voice_at=_last_voice_at,
        jsonl_path=jsonl_path,
        db_path=db_path,
        snapshot_path=_snapshot_path,
        calls_dir=calls_dir,
        writer_health=writer_health,
    )


@app.get("/api/events")
async def list_events(
    limit: int = Query(500, ge=1, le=5000),
    since: Optional[str] = Query(None, description="ISO timestamp or duration like '5m', '1h'"),
    types: Optional[str] = Query(None, description="Comma-separated EventType values"),
):
    if _event_log is None:
        return {"events": [], "total_buffered": 0}
    type_list = [t.strip() for t in types.split(",") if t.strip()] if types else None
    evs = _event_log.recent(limit=limit, since=parse_since(since), types=type_list)
    return {
        "events": [e.model_dump(mode="json") for e in evs],
        "total_buffered": len(_event_log),
    }


@app.get("/api/events.csv")
async def export_events_csv(
    since: Optional[str] = Query(None),
    types: Optional[str] = Query(None),
):
    if _event_log is None:
        return Response("", media_type="text/csv")
    type_list = [t.strip() for t in types.split(",") if t.strip()] if types else None
    iterator = _event_log.iter_csv(since=parse_since(since), types=type_list)
    fname = f"dmr_events_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    return StreamingResponse(
        iterator,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@app.get("/api/stats")
async def event_stats():
    if _event_log is None:
        return {}
    # Iterates the in-memory ring buffer under a threading.Lock — small but
    # still synchronous work; keep it off the event loop so WS pushes
    # don't stall when stats.html polls every 5 s.
    return await asyncio.to_thread(_event_log.stats)


def _prefer_index():
    """Return the SQLite EventIndex if it's attached and non-empty.

    Endpoints use this to switch transparently from JSONL-scan to indexed
    SELECTs without changing their response shape.
    """
    if _event_log is None or _event_log.index is None:
        return None
    try:
        if _event_log.index.count() > 0:
            return _event_log.index
    except Exception:  # noqa: BLE001
        return None
    return None


@app.get("/api/quality")
async def quality_window(window: int = Query(3600, ge=60, le=7 * 86400)):
    """Quality ratios computed over a fixed rolling window.

    ``window`` is in seconds (60s – 7d). Default 1h matches what most
    operators want to see — "is the link healthy *right now*". Prefers the
    SQLite index when available; falls back to scanning the JSONL.
    """
    path = _event_log.history_path if _event_log is not None else None
    # On a cold-start day before the index is populated this falls through to
    # a full JSONL scan — hundreds of MB of disk reads on a long-running Pi.
    # Off-loop (and behind the heavy-endpoint semaphore) so it never
    # freezes the live WS feed.
    return await _run_heavy(
        quality_ratios_over_window, path, window_seconds=window, index=_prefer_index(),
    )


def _history_filters(since, until, src, tgt, types, day=None):
    type_list = [t.strip() for t in types.split(",") if t.strip()] if types else None
    filters = {
        "since": parse_since(since),
        "until": parse_since(until),
        "src": src,
        "tgt": tgt,
        "types": type_list,
    }
    if day:
        from .export import is_valid_day
        if not is_valid_day(day):
            raise HTTPException(status_code=422, detail="day must be YYYY-MM-DD")
        filters["day_from"] = day
        filters["day_to"] = day
    return filters


@app.get("/api/history")
async def history(
    since: Optional[str] = Query(None, description="ISO timestamp or duration like '1h'"),
    until: Optional[str] = Query(None, description="ISO timestamp"),
    src: Optional[int] = Query(None, description="Filter by source radio id"),
    tgt: Optional[int] = Query(None, description="Filter by talkgroup / target id"),
    types: Optional[str] = Query(None, description="Comma-separated EventType values"),
    day: Optional[str] = Query(None, description="Restrict to one local day (YYYY-MM-DD)"),
    limit: int = Query(1000, ge=1, le=2000),
    offset: int = Query(0, ge=0),
):
    """Read events from the on-disk JSONL with server-side filtering.

    Unlike /api/events (which only sees the in-memory ring buffer), this
    walks the persisted history file so the debrief browser can query
    older events from previous sessions. ``limit`` is capped at 2000 (was
    20000 — a single response materialising 20k JSON dicts was a memory
    spike big enough to matter on a Pi); bulk pulls belong to the
    streaming CSV export.
    """
    path = _event_log.history_path if _event_log is not None else None
    if path is None:
        return {"events": [], "limit": limit, "offset": offset, "truncated": False}
    filters = _history_filters(since, until, src, tgt, types, day=day)
    # The indexed path can materialise up to ``limit`` JSON-decoded dicts;
    # the fallback path walks the on-disk JSONL line-by-line. Both are
    # synchronous and would block every other coroutine if run on the loop.
    return await _run_heavy(
        _history_query_sync, path, filters, limit, offset,
    )


def _history_query_sync(
    path: Path, filters: dict, limit: int, offset: int,
) -> dict:
    idx = _prefer_index()
    if idx is not None:
        out = idx.query(limit=limit + 1, offset=offset, **filters)
        truncated = len(out) > limit
        return {"events": out[:limit], "limit": limit, "offset": offset, "truncated": truncated}
    out: list[dict] = []
    skipped = 0
    truncated = False
    for obj in stream_history(path, **filters):
        if skipped < offset:
            skipped += 1
            continue
        if len(out) >= limit:
            truncated = True
            break
        out.append(obj)
    return {"events": out, "limit": limit, "offset": offset, "truncated": truncated}


@app.get("/api/history.csv")
async def history_csv(
    since: Optional[str] = Query(None),
    until: Optional[str] = Query(None),
    src: Optional[int] = Query(None),
    tgt: Optional[int] = Query(None),
    types: Optional[str] = Query(None),
    day: Optional[str] = Query(None),
):
    path = _event_log.history_path if _event_log is not None else None
    if path is None:
        return Response("", media_type="text/csv")
    filters = _history_filters(since, until, src, tgt, types, day=day)
    idx = _prefer_index()
    if idx is not None:
        from .event_index import stream_query

        # Flush pending batched appends so the read-only streaming
        # connection sees rows from the last couple of seconds too.
        idx.flush()

        import csv as _csv
        import io as _io

        def _iter_csv_from_index(db_path=idx.db_path):
            buf = _io.StringIO()
            writer = _csv.DictWriter(buf, fieldnames=CSV_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            yield buf.getvalue()
            buf.seek(0); buf.truncate()
            # stream_query walks its own read-only SQLite connection with
            # fetchmany — O(batch) memory instead of the old fetchall()
            # that loaded the entire filtered history into RAM before the
            # first byte went out.
            for obj in stream_query(db_path, **filters):
                writer.writerow(_row_from_dict(obj))
                yield buf.getvalue()
                buf.seek(0); buf.truncate()

        iterator = _iter_csv_from_index()
    else:
        iterator = iter_history_csv(path, **filters)
    fname = f"dmr_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    return StreamingResponse(
        iterator,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@app.get("/api/days")
async def list_days():
    """Days with recorded data — drives the UI day picker and tells the
    operator what's exportable. Prefers the SQLite ``day`` column
    (indexed GROUP BY); falls back to listing partition files."""
    idx = _prefer_index()
    if idx is not None:
        return {"days": await asyncio.to_thread(idx.days_summary)}
    if (
        _event_log is not None
        and getattr(_event_log, "partition", False)
        and _event_log.partition_dir is not None
    ):
        from .event_log import _day_from_filename

        def _scan():
            out = []
            for p in sorted(_event_log.partition_dir.glob("*.jsonl")):
                d = _day_from_filename(p)
                try:
                    size = p.stat().st_size
                except OSError:
                    continue
                out.append({
                    "day": d or "unknown",
                    "events": None,
                    "voice_events": None,
                    "first_ts": None,
                    "last_ts": None,
                    "bytes": size,
                })
            return out

        return {"days": await asyncio.to_thread(_scan)}
    return {"days": []}


@app.get("/api/export")
async def export_range(
    day: Optional[str] = Query(None, description="One local day, YYYY-MM-DD"),
    from_day: Optional[str] = Query(None, alias="from", description="Range start day (inclusive)"),
    to_day: Optional[str] = Query(None, alias="to", description="Range end day (inclusive)"),
    format: str = Query("ndjson", pattern="^(ndjson|csv)$"),
    types: Optional[str] = Query(None),
    src: Optional[int] = Query(None),
    tgt: Optional[int] = Query(None),
):
    """Structured export of a day or an inclusive day range.

    NDJSON of a single unfiltered day streams the raw partition file
    byte-for-byte (the file IS the export); everything else streams
    through the read-only SQLite cursor (or the JSONL fallback) with
    O(batch) memory. 404 when the index knows the range is empty.
    """
    import json as _json

    from .export import is_valid_day

    if day:
        if not is_valid_day(day):
            raise HTTPException(status_code=422, detail="day must be YYYY-MM-DD")
        day_from, day_to = day, day
    elif from_day and to_day:
        if not (is_valid_day(from_day) and is_valid_day(to_day)):
            raise HTTPException(status_code=422, detail="from/to must be YYYY-MM-DD")
        if from_day > to_day:
            raise HTTPException(status_code=422, detail="from must be <= to")
        day_from, day_to = from_day, to_day
    else:
        raise HTTPException(
            status_code=422,
            detail="specify day=YYYY-MM-DD, or from= and to=",
        )

    type_list = [t.strip() for t in types.split(",") if t.strip()] if types else None
    filtered = bool(type_list or src is not None or tgt is not None)
    label = day_from if day_from == day_to else f"{day_from}_{day_to}"

    # Fast path: the day file itself is already exactly the export.
    if (
        format == "ndjson" and not filtered and day_from == day_to
        and _event_log is not None and getattr(_event_log, "partition", False)
    ):
        p = _event_log.day_file(day_from)
        if p.exists():
            return FileResponse(
                p,
                media_type="application/x-ndjson",
                filename=f"dmr_{label}.ndjson",
            )

    idx = _prefer_index()
    if idx is not None:
        from .event_index import stream_query

        idx.flush()
        n = await _run_heavy(
            idx.count, types=type_list, src=src, tgt=tgt,
            day_from=day_from, day_to=day_to,
        )
        if n == 0:
            raise HTTPException(status_code=404, detail=f"no events for {label}")
        row_iter = stream_query(
            idx.db_path, src=src, tgt=tgt, types=type_list,
            day_from=day_from, day_to=day_to,
        )
    else:
        path = _event_log.history_path if _event_log is not None else None
        if path is None or not path.exists():
            raise HTTPException(status_code=404, detail="no event history")
        row_iter = stream_history(
            path, src=src, tgt=tgt, types=type_list,
            day_from=day_from, day_to=day_to,
        )

    if format == "ndjson":
        def _ndjson():
            for obj in row_iter:
                yield _json.dumps(obj, separators=(",", ":")) + "\n"

        return StreamingResponse(
            _ndjson(),
            media_type="application/x-ndjson",
            headers={"Content-Disposition":
                     f'attachment; filename="dmr_{label}.ndjson"'},
        )

    import csv as _csv
    import io as _io

    def _csv_iter():
        buf = _io.StringIO()
        writer = _csv.DictWriter(buf, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        yield buf.getvalue()
        buf.seek(0); buf.truncate()
        for obj in row_iter:
            writer.writerow(_row_from_dict(obj))
            yield buf.getvalue()
            buf.seek(0); buf.truncate()

    return StreamingResponse(
        _csv_iter(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition":
                 f'attachment; filename="dmr_{label}.csv"'},
    )


@app.get("/api/radio/{radio_id}")
async def radio_dossier(
    radio_id: int,
    window: int = Query(24 * 3600, ge=60, le=30 * 86400),
):
    """Per-radio Dossier — lifetime stats, co-talkers, calls, positions.

    Returns 404 if the radio has not appeared in the window.
    """
    from .dossier import build_dossier

    idx = _prefer_index()
    if idx is None:
        return Response(status_code=404)
    radio_state = None
    if _state is not None:
        radio_state = _state.radios.get(radio_id) if hasattr(_state, "radios") else None
    # Off-loop: build_dossier can pull tens of thousands of rows on a busy
    # 24h window. Running it synchronously inside the async handler would
    # block the event loop and freeze the live WebSocket feed.
    result = await _run_heavy(
        build_dossier,
        idx, radio_id, window_seconds=window,
        recordings=_recordings, radio_state=radio_state,
    )
    if result is None:
        return Response(status_code=404)
    return result


@app.get("/api/network")
async def network_graph(
    window: int = Query(3600, ge=60, le=7 * 86400),
    min_weight: int = Query(2, ge=1),
    limit: int = Query(500, ge=1, le=5000),
):
    """Talker-Pair Graph over a rolling window.

    Returns ``{nodes, edges, window_seconds, generated_at}``. ``nodes`` are
    only radios that participate in at least one surviving edge. ``edges``
    have ``kind ∈ {group, private}`` so the UI can colour them differently.
    """
    from .network import compute_talker_pairs

    idx = _prefer_index()
    if idx is None:
        return {"nodes": [], "edges": [], "window_seconds": window,
                "generated_at": datetime.now().isoformat()}
    # Off-loop: at 24h on a busy net this pulls hundreds of thousands of
    # rows and runs an O(n) classification + O(pairs) accumulation in
    # Python. Doing it synchronously freezes every other request,
    # including the live snapshot WebSocket.
    return await _run_heavy(
        compute_talker_pairs,
        idx, window_seconds=window, min_weight=min_weight, limit=limit,
    )


@app.get("/network")
async def network_page():
    return _page("network.html")


@app.get("/stats")
async def stats_page():
    return _page("stats.html")


@app.get("/debrief")
async def debrief_page():
    return _page("debrief.html")


@app.get("/recordings/{filename}")
async def get_recording(filename: str):
    if _recordings is None:
        return Response(status_code=404)
    p = _recordings.file_path(filename)
    if not p.exists() or not p.is_file():
        return Response(status_code=404)
    return FileResponse(p, media_type="audio/wav")


@app.get("/api/alerts/rules")
async def list_alert_rules():
    if _evaluator is None:
        return {"rules": []}
    return {"rules": [r.model_dump(mode="json") for r in _evaluator.list_rules()]}


@app.post("/api/alerts/rules")
async def create_alert_rule(payload: dict):
    if _evaluator is None:
        raise HTTPException(status_code=503, detail="alerts engine not attached")
    # Drop client-supplied ids so the server always assigns one.
    payload = dict(payload)
    payload.pop("id", None)
    try:
        rule = rule_from_dict(payload)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"invalid rule: {exc}") from exc
    _evaluator.add_rule(rule)
    return rule.model_dump(mode="json")


@app.delete("/api/alerts/rules/{rule_id}")
async def delete_alert_rule(rule_id: str):
    if _evaluator is None:
        raise HTTPException(status_code=503, detail="alerts engine not attached")
    if not _evaluator.remove_rule(rule_id):
        raise HTTPException(status_code=404, detail="rule not found")
    return Response(status_code=204)


@app.post("/api/alerts/rules/{rule_id}/toggle")
async def toggle_alert_rule(rule_id: str, payload: dict):
    if _evaluator is None:
        raise HTTPException(status_code=503, detail="alerts engine not attached")
    enabled = bool(payload.get("enabled", True))
    if not _evaluator.set_enabled(rule_id, enabled):
        raise HTTPException(status_code=404, detail="rule not found")
    return {"id": rule_id, "enabled": enabled}


@app.get("/api/alerts/recent")
async def recent_alerts(limit: int = Query(100, ge=1, le=500)):
    if _evaluator is None:
        return {"firings": []}
    return {"firings": [f.model_dump(mode="json")
                        for f in _evaluator.recent_firings(limit=limit)]}


@app.websocket("/ws/alerts")
async def ws_alerts(websocket: WebSocket):
    """Push channel for AlertFiring JSON. The dashboard's toast bar
    subscribes here and shows each incoming message as a notification."""
    await websocket.accept()
    if _evaluator is None:
        await websocket.close(code=1011)
        return
    q = _evaluator.subscribe()
    try:
        # Replay the last few firings so a freshly-loaded UI doesn't show
        # an empty bar when interesting things just happened.
        for past in _evaluator.recent_firings(limit=5)[::-1]:
            await websocket.send_text(past.model_dump_json())
        while True:
            try:
                data = await asyncio.wait_for(q.get(), timeout=30.0)
            except asyncio.TimeoutError:
                # Alerts are infrequent; 30 s silence is normal on a quiet
                # channel.  Probe the socket so dead clients don't accumulate
                # as leaked asyncio tasks.
                try:
                    await websocket.send_text('{"type":"_ping"}')
                except Exception:
                    break
                continue
            await websocket.send_text(data)
    except WebSocketDisconnect:
        pass
    except Exception as exc:  # noqa: BLE001
        print(f"# ws/alerts: client error: {exc}", file=sys.stderr)
    finally:
        _evaluator.unsubscribe(q)


@app.post("/api/reset")
async def reset_all_data(
    request: Request,
    x_reset_token: Optional[str] = Header(None),
):
    """Erase all accumulated state: radios, events, SQLite index, and snapshot file.

    Destructive and previously unauthenticated — anyone who could reach
    the port could wipe weeks of data. Now: when ``--reset-token`` is
    configured the ``X-Reset-Token`` header must match; without a token
    only loopback clients may reset.
    """
    if _reset_token is not None:
        if x_reset_token != _reset_token:
            raise HTTPException(
                status_code=403,
                detail="invalid or missing X-Reset-Token header",
            )
    else:
        client_host = request.client.host if request.client else None
        if client_host not in ("127.0.0.1", "::1", "localhost"):
            raise HTTPException(
                status_code=403,
                detail="reset is only allowed from localhost unless "
                       "--reset-token is configured",
            )
    if _state is not None:
        _state.reset()
    if _event_log is not None:
        _event_log.clear()
    if _snapshot_path is not None:
        for p in (_snapshot_path, _snapshot_path.with_suffix(".bak")):
            try:
                p.unlink(missing_ok=True)
            except OSError:
                pass
    await push_snapshot()
    return {"status": "reset", "cleared_at": datetime.now().isoformat()}


@app.get("/alerts")
async def alerts_page():
    return _page("alerts.html")


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    q: asyncio.Queue = asyncio.Queue(maxsize=10)
    _subscribers.add(q)
    try:
        # First frame: reuse the broadcast payload (already serialised)
        # instead of re-serialising the full snapshot per new connection.
        if _broadcast_payload is not None:
            await websocket.send_text(_broadcast_payload)
        elif _state is not None:
            sm = _state
            await websocket.send_text(
                await asyncio.to_thread(
                    lambda: sm.snapshot(trim=True).model_dump_json()
                )
            )
        while True:
            try:
                data = await asyncio.wait_for(q.get(), timeout=30.0)
            except asyncio.TimeoutError:
                # If we were evicted from _subscribers (queue was full when
                # push_snapshot tried to put_nowait), nobody will ever push to
                # q again — break so the task doesn't leak.
                if q not in _subscribers:
                    break
                # Still subscribed but channel is quiet (dsd-fme silent).
                # Probe the socket so an undetected dead client doesn't live
                # as a leaked asyncio task indefinitely.
                try:
                    await websocket.send_text('{"type":"_ping"}')
                except Exception:
                    break
                continue
            await websocket.send_text(data)
    except WebSocketDisconnect:
        pass
    except Exception as exc:  # noqa: BLE001
        print(f"# ws: client error: {exc}", file=sys.stderr)
    finally:
        _subscribers.discard(q)


@app.get("/")
async def index():
    return _page("index.html")
