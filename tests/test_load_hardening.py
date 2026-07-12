"""Phase-1 load-hardening tests (v0.18.0).

Covers:
* trimmed broadcast snapshots (radio cap, trail dropping, radios_total)
* /api/snapshot reusing the pre-serialised broadcast payload
* the heavy-endpoint semaphore shedding load with 503
* /api/history limit clamp (2000)
* /api/reset guard (loopback-only / X-Reset-Token)
* stream_query — bounded-memory streaming reads from the SQLite index
* RecordingRegistry metadata cache + listing memo
"""
from __future__ import annotations

import asyncio
import os
import sqlite3
import struct
import time
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from backend import server as srv
from backend.event_index import EventIndex, stream_query
from backend.event_log import EventLog
from backend.models import LRRPPositionEvent, VoiceCallEvent
from backend.recordings import RecordingRegistry
from backend.state import StateManager

_T0 = datetime(2026, 5, 10, 21, 0, 0)


def _voice(seconds: int, src: int = 101, tgt: int = 9) -> VoiceCallEvent:
    return VoiceCallEvent(
        timestamp=_T0 + timedelta(seconds=seconds),
        raw_line="v", slot=1, src=src, tgt=tgt,
    )


def _position(seconds: int, src: int) -> LRRPPositionEvent:
    return LRRPPositionEvent(
        timestamp=_T0 + timedelta(seconds=seconds),
        raw_line="p", src=src, lat=32.0, lon=34.0,
    )


# ── Trimmed broadcast snapshots ──────────────────────────────────────


def test_full_snapshot_reports_radios_total():
    sm = StateManager()
    for i in range(4):
        sm.apply(_voice(i, src=100 + i))
    snap = sm.snapshot()
    assert snap.radios_total == 4
    assert len(snap.radios) == 4


def test_trimmed_snapshot_caps_radios_by_last_seen():
    sm = StateManager(broadcast_max_radios=3)
    for i in range(5):
        sm.apply(_voice(i * 10, src=100 + i))
    trimmed = sm.snapshot(trim=True)
    # Newest three survive; total still reports the real count.
    assert set(trimmed.radios) == {102, 103, 104}
    assert trimmed.radios_total == 5
    # The full view is untouched.
    full = sm.snapshot()
    assert len(full.radios) == 5


def test_trimmed_snapshot_drops_stale_position_trails():
    sm = StateManager()
    sm.apply(_position(0, src=200))
    # 20 minutes later another event moves the "now" past the trail age.
    sm.apply(_voice(20 * 60, src=201))
    trimmed = sm.snapshot(trim=True)
    assert trimmed.radios[200].position_history == []
    # last_position is kept — only the trail is trimmed.
    assert trimmed.radios[200].last_position is not None
    # The full snapshot keeps the trail (persistence must not lose it).
    assert len(sm.snapshot().radios[200].position_history) == 1


def test_trimmed_snapshot_keeps_fresh_position_trails():
    sm = StateManager()
    sm.apply(_position(0, src=200))
    sm.apply(_voice(60, src=201))  # 1 min later — trail still fresh
    trimmed = sm.snapshot(trim=True)
    assert len(trimmed.radios[200].position_history) == 1


def test_trim_does_not_mutate_manager_state():
    sm = StateManager()
    sm.apply(_position(0, src=200))
    sm.apply(_voice(20 * 60, src=201))
    sm.snapshot(trim=True)
    assert len(sm.radios[200].position_history) == 1


# ── /api/snapshot payload reuse ──────────────────────────────────────


def test_api_snapshot_returns_broadcast_payload_verbatim():
    sm = StateManager()
    srv.attach_state(sm)
    sentinel = '{"marker": "broadcast-payload"}'
    asyncio.run(srv.push_snapshot(sentinel))
    client = TestClient(srv.app)
    r = client.get("/api/snapshot")
    assert r.status_code == 200
    assert r.text == sentinel


def test_api_snapshot_builds_fresh_when_no_payload():
    sm = StateManager()
    sm.apply(_voice(0, src=42))
    srv.attach_state(sm)  # clears any stored payload
    client = TestClient(srv.app)
    data = client.get("/api/snapshot").json()
    assert data["radios_total"] == 1
    assert "42" in data["radios"]


def test_attach_state_clears_stale_payload():
    srv.attach_state(StateManager())
    asyncio.run(srv.push_snapshot('{"stale": true}'))
    srv.attach_state(StateManager())  # fresh state → payload must reset
    client = TestClient(srv.app)
    assert "stale" not in client.get("/api/snapshot").text


# ── Heavy-endpoint semaphore ─────────────────────────────────────────


def test_heavy_endpoints_shed_load_with_503(tmp_path):
    state = StateManager()
    log = EventLog(jsonl_path=tmp_path / "events.jsonl", capacity=100)
    log.append(_voice(0))
    srv.attach_state(state)
    srv.attach_event_log(log)
    client = TestClient(srv.app)
    old = srv._heavy_waiting
    srv._heavy_waiting = srv._HEAVY_MAX_QUEUE
    try:
        r = client.get("/api/history")
        assert r.status_code == 503
        assert r.headers.get("retry-after") == "2"
    finally:
        srv._heavy_waiting = old
        log.close()


def test_history_limit_clamped_to_2000():
    client = TestClient(srv.app)
    assert client.get("/api/history?limit=20000").status_code == 422
    # 2000 itself is accepted (may return empty without an event log).
    assert client.get("/api/history?limit=2000").status_code == 200


# ── /api/reset guard ─────────────────────────────────────────────────


@pytest.fixture
def reset_client():
    srv.attach_state(StateManager())
    srv.attach_reset_token(None)
    yield TestClient(srv.app)
    srv.attach_reset_token(None)


def test_reset_denied_for_non_loopback_without_token(reset_client):
    # TestClient's synthetic host is "testclient" — not loopback.
    r = reset_client.post("/api/reset")
    assert r.status_code == 403


def test_reset_with_token_configured(reset_client):
    srv.attach_reset_token("s3cret")
    assert reset_client.post("/api/reset").status_code == 403
    assert reset_client.post(
        "/api/reset", headers={"X-Reset-Token": "wrong"}
    ).status_code == 403
    r = reset_client.post("/api/reset", headers={"X-Reset-Token": "s3cret"})
    assert r.status_code == 200
    assert r.json()["status"] == "reset"


# ── stream_query ─────────────────────────────────────────────────────


def _make_index(tmp_path: Path, n: int = 20) -> EventIndex:
    idx = EventIndex(tmp_path / "events.db")
    for i in range(n):
        idx.append(_voice(i, src=100 + (i % 3)).model_dump(mode="json"))
    idx.flush()
    return idx


def test_stream_query_matches_query(tmp_path):
    idx = _make_index(tmp_path)
    try:
        streamed = list(stream_query(idx.db_path, batch_size=4))
        direct = idx.query(limit=1000)
        assert streamed == direct
    finally:
        idx.close()


def test_stream_query_applies_filters(tmp_path):
    idx = _make_index(tmp_path, n=21)
    try:
        out = list(stream_query(idx.db_path, src=100))
        assert len(out) == 7
        assert all(o["src"] == 100 for o in out)
    finally:
        idx.close()


def test_stream_query_survives_concurrent_appends(tmp_path):
    idx = _make_index(tmp_path, n=10)
    try:
        gen = stream_query(idx.db_path, batch_size=3)
        first = [next(gen) for _ in range(5)]
        # Writer keeps appending while the export is mid-stream.
        for i in range(5):
            idx.append(_voice(100 + i).model_dump(mode="json"))
        idx.flush()
        rest = list(gen)
        assert len(first) + len(rest) >= 10  # never loses pre-existing rows
    finally:
        idx.close()


def test_stream_query_missing_db_raises_operational_error(tmp_path):
    with pytest.raises(sqlite3.OperationalError):
        list(stream_query(tmp_path / "nope.db"))


# ── RecordingRegistry cache ──────────────────────────────────────────


def _write_wav(path: Path, duration_sec: float, sample_rate: int = 8000) -> None:
    n_samples = int(duration_sec * sample_rate)
    byte_rate = sample_rate * 2
    data_size = n_samples * 2
    header = b"RIFF" + struct.pack("<I", 36 + data_size) + b"WAVE"
    header += b"fmt " + struct.pack("<I", 16)
    header += struct.pack("<HHIIHH", 1, 1, sample_rate, byte_rate, 2, 16)
    header += b"data" + struct.pack("<I", data_size)
    path.write_bytes(header + b"\x00\x00" * n_samples)


def _age_file(path: Path, seconds: float) -> None:
    t = time.time() - seconds
    os.utime(path, (t, t))


def test_recordings_memo_serves_within_ttl(tmp_path):
    reg = RecordingRegistry(tmp_path, memo_ttl_seconds=60.0)
    _write_wav(tmp_path / "TG_9_SRC_1.wav", 1.0)
    _age_file(tmp_path / "TG_9_SRC_1.wav", 10)
    assert len(reg.list_recent()) == 1
    # A new file appears, but the memo is still fresh — same answer.
    _write_wav(tmp_path / "TG_9_SRC_2.wav", 1.0)
    _age_file(tmp_path / "TG_9_SRC_2.wav", 10)
    assert len(reg.list_recent()) == 1
    reg._memo = None  # TTL expiry
    assert len(reg.list_recent()) == 2


def test_recordings_cache_invalidated_on_mtime_change(tmp_path):
    reg = RecordingRegistry(tmp_path, memo_ttl_seconds=0.0)
    wav = tmp_path / "TG_9_SRC_1.wav"
    _write_wav(wav, 1.0)
    _age_file(wav, 20)
    out1 = reg.list_recent()
    assert out1[0].duration_seconds == pytest.approx(1.0, abs=0.01)
    # File replaced with a longer recording (new size + mtime).
    _write_wav(wav, 3.0)
    _age_file(wav, 10)
    out2 = reg.list_recent()
    assert out2[0].duration_seconds == pytest.approx(3.0, abs=0.01)


def test_recordings_cache_drops_vanished_files(tmp_path):
    reg = RecordingRegistry(tmp_path, memo_ttl_seconds=0.0)
    wav = tmp_path / "TG_9_SRC_1.wav"
    _write_wav(wav, 1.0)
    _age_file(wav, 20)
    assert len(reg.list_recent()) == 1
    assert "TG_9_SRC_1.wav" in reg._meta_cache
    wav.unlink()
    assert reg.list_recent() == []
    assert "TG_9_SRC_1.wav" not in reg._meta_cache


def test_recordings_prune_invalidates_memo(tmp_path):
    reg = RecordingRegistry(tmp_path, memo_ttl_seconds=60.0)
    wav = tmp_path / "TG_9_SRC_1.wav"
    _write_wav(wav, 1.0)
    _age_file(wav, 48 * 3600)
    assert len(reg.list_recent()) == 1
    deleted, _ = reg.prune_older_than(24)
    assert deleted == 1
    assert reg.list_recent() == []
