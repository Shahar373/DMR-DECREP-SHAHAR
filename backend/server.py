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
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, Response, StreamingResponse

from . import __build_date__, __version__
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

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"


def attach_state(sm: StateManager) -> None:
    global _state
    _state = sm


def attach_recordings(r: RecordingRegistry) -> None:
    global _recordings
    _recordings = r


def attach_event_log(log: EventLog) -> None:
    global _event_log
    _event_log = log


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
    _subscribers -= dead


@app.get("/api/snapshot")
async def get_snapshot():
    if _state is None:
        return {}
    return _state.snapshot().model_dump()


@app.get("/api/recordings")
async def list_recordings():
    if _recordings is None:
        return {"recordings": []}
    return {"recordings": [r.model_dump(mode="json") for r in _recordings.list_recent()]}


@app.get("/api/debug")
async def debug_info():
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
    return _event_log.stats()


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
    return quality_ratios_over_window(path, window_seconds=window, index=_prefer_index())


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


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    q: asyncio.Queue = asyncio.Queue(maxsize=10)
    _subscribers.add(q)
    try:
        if _state is not None:
            await websocket.send_text(_state.snapshot().model_dump_json())
        while True:
            data = await q.get()
            await websocket.send_text(data)
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        _subscribers.discard(q)


@app.get("/")
async def index():
    html_path = FRONTEND_DIR / "index.html"
    return HTMLResponse(html_path.read_text(encoding="utf-8"))
