"""Per-call MP3 recording registry for the DMR Cap+ Monitor.

A `RecordingRegistry` tracks one MP3 file per voice call on disk and exposes a
short rolling window of recent recordings to the dashboard. The registry only
maintains metadata + filesystem lifecycle; the actual byte-writing is done by
``AudioBroadcaster.start_recording`` / ``stop_recording``.

Lifecycle::

    rec = registry.start(active_call)         # caller opens broadcaster file
    ...                                        # broadcaster writes chunks
    registry.end(slot, ended_at)              # caller closes broadcaster file
"""
from __future__ import annotations

import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from pydantic import BaseModel


class CallRecording(BaseModel):
    id: str                       # short uuid
    src: int
    tgt: int
    slot: int
    is_cap_plus: bool = False
    is_encrypted: bool = False
    started_at: datetime
    ended_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    file_bytes: Optional[int] = None


class RecordingRegistry:
    """Track per-call MP3 files; keep the newest ``max_keep`` on disk."""

    def __init__(self, base_dir: Path, max_keep: int = 20) -> None:
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.max_keep = max_keep
        self._recordings: list[CallRecording] = []      # chronological, oldest first
        self._active_by_slot: dict[int, str] = {}       # slot -> recording id

    def file_path(self, rec_id: str) -> Path:
        return self.base_dir / f"{rec_id}.mp3"

    def start(self, call) -> CallRecording:
        """Begin a recording for an ActiveCall.  Caller must be sure no
        recording is active on the same slot (call .end() first if so)."""
        rec_id = uuid.uuid4().hex[:8]
        rec = CallRecording(
            id=rec_id,
            src=call.src,
            tgt=call.tgt,
            slot=call.slot,
            is_cap_plus=getattr(call, "is_cap_plus", False),
            is_encrypted=getattr(call, "is_encrypted", False),
            started_at=call.started_at,
        )
        self._recordings.append(rec)
        self._active_by_slot[call.slot] = rec_id
        return rec

    def end(self, slot: int, ended_at: datetime) -> Optional[CallRecording]:
        rec_id = self._active_by_slot.pop(slot, None)
        if rec_id is None:
            return None
        rec = self.get(rec_id)
        if rec is None:
            return None
        rec.ended_at = ended_at
        rec.duration_seconds = (ended_at - rec.started_at).total_seconds()
        p = self.file_path(rec_id)
        try:
            rec.file_bytes = p.stat().st_size if p.exists() else 0
        except OSError:
            rec.file_bytes = 0
        self._prune()
        return rec

    def get(self, rec_id: str) -> Optional[CallRecording]:
        for r in self._recordings:
            if r.id == rec_id:
                return r
        return None

    def list_recent(self) -> list[CallRecording]:
        return list(reversed(self._recordings))  # newest first

    def active_id(self, slot: int) -> Optional[str]:
        return self._active_by_slot.get(slot)

    def _prune(self) -> None:
        while len(self._recordings) > self.max_keep:
            old = self._recordings.pop(0)
            try:
                self.file_path(old.id).unlink()
            except FileNotFoundError:
                pass
