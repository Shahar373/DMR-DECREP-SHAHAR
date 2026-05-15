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
from .event_log import EventLog, parse_since
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
