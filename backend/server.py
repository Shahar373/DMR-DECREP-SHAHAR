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

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, Response, StreamingResponse

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


def note_voice_event(ts: datetime) -> None:
    """Bump the last-voice-seen marker (called by the wrapper on each
    voice_call event so /api/health can answer "are radios actually
    talking, not just is the CC alive")."""
    global _last_voice_at
    if _last_voice_at is None or ts > _last_voice_at:
        _last_voice_at = ts


def attach_state(sm: StateManager) -> None:
    global _state
    _state = sm


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


async def push_snapshot() -> None:
    """Serialise current state and enqueue to every connected WS client."""
    if _state is None or not _subscribers:
        return
    payload = _state.snapshot().model_dump_json()
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
    if _state is None:
        return {}
    return _state.snapshot().model_dump()


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
    jsonl_path = _event_log.jsonl_path if _event_log is not None else None
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
    path = _event_log.jsonl_path if _event_log is not None else None
    # On a cold-start day before the index is populated this falls through to
    # a full JSONL scan — hundreds of MB of disk reads on a long-running Pi.
    # Off-loop so it never freezes the live WS feed.
    return await asyncio.to_thread(
        quality_ratios_over_window, path, window_seconds=window, index=_prefer_index(),
    )


def _history_filters(since, until, src, tgt, types):
    type_list = [t.strip() for t in types.split(",") if t.strip()] if types else None
    return {
        "since": parse_since(since),
        "until": parse_since(until),
        "src": src,
        "tgt": tgt,
        "types": type_list,
    }


@app.get("/api/history")
async def history(
    since: Optional[str] = Query(None, description="ISO timestamp or duration like '1h'"),
    until: Optional[str] = Query(None, description="ISO timestamp"),
    src: Optional[int] = Query(None, description="Filter by source radio id"),
    tgt: Optional[int] = Query(None, description="Filter by talkgroup / target id"),
    types: Optional[str] = Query(None, description="Comma-separated EventType values"),
    limit: int = Query(1000, ge=1, le=20000),
    offset: int = Query(0, ge=0),
):
    """Read events from the on-disk JSONL with server-side filtering.

    Unlike /api/events (which only sees the in-memory ring buffer), this
    walks the persisted history file so the debrief browser can query
    older events from previous sessions.
    """
    path = _event_log.jsonl_path if _event_log is not None else None
    if path is None:
        return {"events": [], "limit": limit, "offset": offset, "truncated": False}
    filters = _history_filters(since, until, src, tgt, types)
    # The indexed path can materialise up to ``limit`` (= 20k max) JSON-decoded
    # dicts; the fallback path walks the on-disk JSONL line-by-line. Both are
    # synchronous and would block every other coroutine if run on the loop.
    return await asyncio.to_thread(
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
):
    path = _event_log.jsonl_path if _event_log is not None else None
    if path is None:
        return Response("", media_type="text/csv")
    filters = _history_filters(since, until, src, tgt, types)
    idx = _prefer_index()
    if idx is not None:
        import csv as _csv
        import io as _io

        def _iter_csv_from_index():
            buf = _io.StringIO()
            writer = _csv.DictWriter(buf, fieldnames=CSV_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            yield buf.getvalue()
            buf.seek(0); buf.truncate()
            for obj in idx.iter_query(**filters):
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
    result = await asyncio.to_thread(
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
    return await asyncio.to_thread(
        compute_talker_pairs,
        idx, window_seconds=window, min_weight=min_weight, limit=limit,
    )


@app.get("/network")
async def network_page():
    return HTMLResponse((FRONTEND_DIR / "network.html").read_text(encoding="utf-8"))


@app.get("/stats")
async def stats_page():
    return HTMLResponse((FRONTEND_DIR / "stats.html").read_text(encoding="utf-8"))


@app.get("/debrief")
async def debrief_page():
    return HTMLResponse((FRONTEND_DIR / "debrief.html").read_text(encoding="utf-8"))


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


@app.get("/alerts")
async def alerts_page():
    return HTMLResponse((FRONTEND_DIR / "alerts.html").read_text(encoding="utf-8"))


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    q: asyncio.Queue = asyncio.Queue(maxsize=10)
    _subscribers.add(q)
    try:
        if _state is not None:
            await websocket.send_text(_state.snapshot().model_dump_json())
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
    html_path = FRONTEND_DIR / "index.html"
    return HTMLResponse(html_path.read_text(encoding="utf-8"))
