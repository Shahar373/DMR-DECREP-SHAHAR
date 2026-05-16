"""System health snapshot for the dashboard's ``/api/health`` endpoint.

Pure read-only metrics about the running process and its on-disk state:

* uptime (seconds since the FastAPI server started)
* last event ingested (age in seconds; ``None`` if nothing yet)
* last voice frame seen (separately — quality errors don't count as "alive")
* persistent file sizes (events.jsonl, events.db, snapshot.json)
* free space on the disk that holds the JSONL
* per-call WAV directory (count, total bytes, oldest age)
* current version + build date

Everything here is best-effort; any individual probe that fails is
returned as ``None`` so a partial outage in one subsystem never breaks
the endpoint itself. The shape is stable so external watchdogs (cron
``curl`` + ``jq``, healthchecks.io, prometheus textfile collector) can
rely on it.
"""
from __future__ import annotations

import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Optional


def _safe_stat_size(path: Optional[Path]) -> Optional[int]:
    if path is None:
        return None
    try:
        return path.stat().st_size
    except OSError:
        return None


def _safe_free_bytes(path: Optional[Path]) -> Optional[int]:
    """Free bytes on the filesystem holding ``path`` (or its parent)."""
    if path is None:
        return None
    probe = path if path.exists() else path.parent
    try:
        return shutil.disk_usage(str(probe)).free
    except OSError:
        return None


def _calls_dir_summary(base: Optional[Path]) -> dict:
    out = {"count": 0, "total_bytes": 0, "oldest_age_seconds": None}
    if base is None or not base.exists():
        return out
    now = time.time()
    oldest_mtime: Optional[float] = None
    try:
        for p in base.iterdir():
            if not p.is_file() or p.suffix.lower() != ".wav":
                continue
            try:
                stat = p.stat()
            except OSError:
                continue
            out["count"] += 1
            out["total_bytes"] += stat.st_size
            if oldest_mtime is None or stat.st_mtime < oldest_mtime:
                oldest_mtime = stat.st_mtime
    except OSError:
        return out
    if oldest_mtime is not None:
        out["oldest_age_seconds"] = max(0.0, now - oldest_mtime)
    return out


def compute_health(
    *,
    version: str,
    build_date: str,
    started_at: datetime,
    last_event_at: Optional[datetime] = None,
    last_voice_at: Optional[datetime] = None,
    jsonl_path: Optional[Path] = None,
    db_path: Optional[Path] = None,
    snapshot_path: Optional[Path] = None,
    calls_dir: Optional[Path] = None,
    now: Optional[datetime] = None,
) -> dict:
    """Build the health dict. Pure — no I/O failure can raise out of it."""
    now = now or datetime.now()
    uptime = max(0.0, (now - started_at).total_seconds())

    def _age(ts: Optional[datetime]) -> Optional[float]:
        if ts is None:
            return None
        return max(0.0, (now - ts).total_seconds())

    return {
        "version": version,
        "build_date": build_date,
        "now": now.isoformat(),
        "uptime_seconds": uptime,
        "last_event_age_seconds": _age(last_event_at),
        "last_voice_age_seconds": _age(last_voice_at),
        "files": {
            "jsonl_bytes": _safe_stat_size(jsonl_path),
            "db_bytes": _safe_stat_size(db_path),
            "snapshot_bytes": _safe_stat_size(snapshot_path),
        },
        "disk": {
            "free_bytes": _safe_free_bytes(jsonl_path or snapshot_path),
        },
        "calls_dir": _calls_dir_summary(calls_dir),
    }
