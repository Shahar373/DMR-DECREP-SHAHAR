"""FastAPI server for the DMR Cap+ Monitor dashboard (Phase 4a).

Exposes:
  GET  /              → serves frontend/index.html
  GET  /api/snapshot  → current DashboardSnapshot as JSON (HTTP polling fallback)
  WS   /ws            → pushes a snapshot JSON every broadcast cycle

The StateManager is injected via `attach_state()` before uvicorn starts.
`push_snapshot()` is called periodically by the CLI event loop so the server
itself does not need an internal timer — it just fans the payload out to all
connected clients.
"""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from .state import StateManager

app = FastAPI(title="DMR Cap+ Monitor", docs_url=None, redoc_url=None)

_state: Optional[StateManager] = None
_subscribers: set[asyncio.Queue] = set()

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"


def attach_state(sm: StateManager) -> None:
    global _state
    _state = sm


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


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    q: asyncio.Queue = asyncio.Queue(maxsize=10)
    _subscribers.add(q)
    try:
        # Send current state immediately on connect so the UI isn't blank.
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
