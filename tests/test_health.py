"""Tests for backend.health.compute_health and the /api/health endpoint."""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from backend import server as srv
from backend.event_log import EventLog
from backend.health import compute_health
from backend.models import VoiceCallEvent


def _ts(seconds: int = 0) -> datetime:
    return datetime(2026, 5, 10, 21, 0, 0) + timedelta(seconds=seconds)


def test_compute_health_with_no_inputs_returns_stable_shape() -> None:
    h = compute_health(
        version="0.12.0", build_date="2026-05-16",
        started_at=_ts(0), now=_ts(60),
    )
    assert h["version"] == "0.12.0"
    assert h["uptime_seconds"] == 60.0
    assert h["last_event_age_seconds"] is None
    assert h["last_voice_age_seconds"] is None
    assert h["files"] == {"jsonl_bytes": None, "db_bytes": None, "snapshot_bytes": None}
    assert h["calls_dir"] == {"count": 0, "total_bytes": 0, "oldest_age_seconds": None}


def test_compute_health_reports_file_sizes_and_free_space(tmp_path: Path) -> None:
    jsonl = tmp_path / "events.jsonl"
    jsonl.write_text("x" * 500, encoding="utf-8")
    snap = tmp_path / "snapshot.json"
    snap.write_text("y" * 100, encoding="utf-8")

    h = compute_health(
        version="0.12.0", build_date="2026-05-16",
        started_at=_ts(0), now=_ts(120),
        jsonl_path=jsonl, snapshot_path=snap,
        last_event_at=_ts(115),
        last_voice_at=_ts(100),
    )
    assert h["files"]["jsonl_bytes"] == 500
    assert h["files"]["snapshot_bytes"] == 100
    assert h["disk"]["free_bytes"] is not None and h["disk"]["free_bytes"] > 0
    assert h["last_event_age_seconds"] == 5.0
    assert h["last_voice_age_seconds"] == 20.0


def test_compute_health_summarises_calls_dir(tmp_path: Path) -> None:
    calls = tmp_path / "calls"
    calls.mkdir()
    (calls / "a.wav").write_bytes(b"\x00" * 200)
    (calls / "b.wav").write_bytes(b"\x00" * 300)
    (calls / "notes.txt").write_text("ignore me")
    h = compute_health(
        version="0.12.0", build_date="2026-05-16",
        started_at=_ts(0), now=_ts(10),
        calls_dir=calls,
    )
    assert h["calls_dir"]["count"] == 2
    assert h["calls_dir"]["total_bytes"] == 500
    assert h["calls_dir"]["oldest_age_seconds"] is not None
    assert h["calls_dir"]["oldest_age_seconds"] >= 0


def test_health_endpoint_smoke(tmp_path: Path) -> None:
    log = EventLog(jsonl_path=tmp_path / "events.jsonl", capacity=50)
    log.append(VoiceCallEvent(
        timestamp=_ts(5), raw_line="v", slot=1, src=101, tgt=9,
    ))
    srv.attach_event_log(log)
    srv.attach_snapshot_path(tmp_path / "snapshot.json")
    try:
        client = TestClient(srv.app)
        r = client.get("/api/health")
        assert r.status_code == 200
        data = r.json()
        # Stable contract for external watchdogs:
        assert "version" in data and "uptime_seconds" in data
        assert "files" in data and "disk" in data and "calls_dir" in data
        # JSONL exists, so its size should be reported as an int.
        assert isinstance(data["files"]["jsonl_bytes"], int)
    finally:
        srv.attach_event_log(None)  # type: ignore[arg-type]
        srv.attach_snapshot_path(None)
        log.close()


def test_note_voice_event_updates_health_marker(tmp_path: Path) -> None:
    """The wrapper-side hook bumps last_voice_at so /api/health can answer
    'are radios actually talking, not just is the CC alive'."""
    # Reset module state.
    srv._last_voice_at = None  # type: ignore[attr-defined]
    log = EventLog(jsonl_path=tmp_path / "events.jsonl", capacity=10)
    srv.attach_event_log(log)
    try:
        srv.note_voice_event(_ts(10))
        srv.note_voice_event(_ts(20))
        srv.note_voice_event(_ts(15))  # out-of-order, must not regress
        client = TestClient(srv.app)
        data = client.get("/api/health").json()
        # latest of the three is _ts(20); age is non-negative.
        assert data["last_voice_age_seconds"] is not None
    finally:
        srv.attach_event_log(None)  # type: ignore[arg-type]
        srv._last_voice_at = None  # type: ignore[attr-defined]
        log.close()
