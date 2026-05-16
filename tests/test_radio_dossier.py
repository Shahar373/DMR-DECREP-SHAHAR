"""Tests for the per-radio Dossier endpoint."""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from backend import server as srv
from backend.dossier import _group_calls, build_dossier
from backend.event_log import EventLog
from backend.models import (
    EncryptionEvent,
    IPMappingEvent,
    LRRPPositionEvent,
    PreambleCSBKEvent,
    VoiceCallEvent,
)


def _ts(seconds: int = 0) -> datetime:
    return datetime(2026, 5, 10, 9, 0, 0) + timedelta(seconds=seconds)


def _voice(sec: int, src: int, tgt: int, slot: int = 1) -> VoiceCallEvent:
    return VoiceCallEvent(
        timestamp=_ts(sec), raw_line="voice", slot=slot, src=src, tgt=tgt,
    )


def _seed_radio(log: EventLog) -> None:
    # 20 events involving radio 70 within a short window.
    # 5 voice_call frames on TG 9 (one call session).
    for s in range(5):
        log.append(_voice(s, 70, 9))
    # 8 frames on TG 1 split into two distinct calls (gap > 2s).
    for s in range(4):
        log.append(_voice(10 + s, 70, 1))
    for s in range(4):
        log.append(_voice(30 + s, 70, 1))
    # 2 positions for the GPS track.
    log.append(LRRPPositionEvent(
        timestamp=_ts(40), raw_line="pos", src=70, lat=32.10, lon=34.87,
    ))
    log.append(LRRPPositionEvent(
        timestamp=_ts(50), raw_line="pos", src=70, lat=32.11, lon=34.88,
    ))
    # Encryption on slot 1 (which 70 used).
    log.append(EncryptionEvent(
        timestamp=_ts(2), raw_line="enc", slot=1, flco="0x04", fid="0x80",
    ))
    # IP mapping.
    log.append(IPMappingEvent(
        timestamp=_ts(60), raw_line="ip", role="SRC", radio_id=70,
        ip="172.16.0.70", port=4000,
    ))
    # A co-talker on TG 9: radio 71 with 3 calls.
    for s in range(3):
        log.append(_voice(70 + s, 71, 9))


def test_group_calls_splits_on_gap() -> None:
    rows = [
        {"timestamp": _ts(0).isoformat(), "slot": 1, "src": 70, "tgt": 9},
        {"timestamp": _ts(1).isoformat(), "slot": 1, "src": 70, "tgt": 9},
        # 5s gap → new session
        {"timestamp": _ts(6).isoformat(), "slot": 1, "src": 70, "tgt": 9},
        # Different tgt → new session
        {"timestamp": _ts(7).isoformat(), "slot": 1, "src": 70, "tgt": 1},
    ]
    sessions = _group_calls(rows)
    assert len(sessions) == 3
    assert sessions[0]["frames"] == 2
    assert sessions[1]["frames"] == 1
    assert sessions[2]["frames"] == 1


def test_dossier_happy_path(tmp_path: Path) -> None:
    log = EventLog(jsonl_path=tmp_path / "events.jsonl", capacity=200)
    try:
        _seed_radio(log)
        d = build_dossier(log.index, 70, window_seconds=24 * 3600, now=_ts(120))
    finally:
        log.close()
    assert d is not None
    assert d["id"] == 70
    assert d["ip"] == "172.16.0.70"
    assert d["total_calls"] == 3  # 1 on TG 9 + 2 distinct on TG 1
    assert d["encrypted_calls"] == 1  # one encryption event on slot 1
    tgs = {t["tg"]: t["count"] for t in d["tgs_touched"]}
    assert tgs[9] == 5 and tgs[1] == 8
    assert len(d["position_history"]) == 2
    # Co-talker 71 should appear in top_co_talkers (both on TG 9).
    ids = {c["id"] for c in d["top_co_talkers"]}
    assert 71 in ids
    # Hourly buckets sum to total events for this radio (excluding 71).
    assert sum(d["hourly_activity"]) > 0
    # Recent calls: 3 sessions, newest first.
    assert len(d["recent_calls"]) == 3


def test_dossier_unknown_returns_none(tmp_path: Path) -> None:
    log = EventLog(jsonl_path=tmp_path / "events.jsonl", capacity=200)
    try:
        log.append(_voice(0, 70, 9))
        d = build_dossier(log.index, 9999, window_seconds=24 * 3600, now=_ts(10))
    finally:
        log.close()
    assert d is None


def test_dossier_window_excludes_old_events(tmp_path: Path) -> None:
    log = EventLog(jsonl_path=tmp_path / "events.jsonl", capacity=200)
    try:
        log.append(_voice(0, 70, 9))
        log.append(_voice(1, 70, 9))
        # 30 minutes ago — outside a 600s window.
        log.append(VoiceCallEvent(
            timestamp=_ts(0) - timedelta(hours=2), raw_line="voice",
            slot=1, src=70, tgt=9,
        ))
        d = build_dossier(log.index, 70, window_seconds=600, now=_ts(2))
    finally:
        log.close()
    assert d is not None
    # The 2-hour-old event must not count.
    assert d["total_calls"] == 1


def test_radio_endpoint_404_when_unknown(tmp_path: Path) -> None:
    log = EventLog(jsonl_path=tmp_path / "events.jsonl", capacity=200)
    log.append(_voice(0, 70, 9))
    srv.attach_event_log(log)
    try:
        client = TestClient(srv.app)
        resp = client.get("/api/radio/9999")
        assert resp.status_code == 404
    finally:
        srv.attach_event_log(None)  # type: ignore[arg-type]
        log.close()


def test_radio_endpoint_returns_dossier(tmp_path: Path) -> None:
    log = EventLog(jsonl_path=tmp_path / "events.jsonl", capacity=200)
    _seed_radio(log)
    srv.attach_event_log(log)
    try:
        client = TestClient(srv.app)
        # Big enough window so synthetic 2026 timestamps land in it.
        resp = client.get("/api/radio/70", params={"window": 30 * 86400})
        assert resp.status_code == 200
        d = resp.json()
        assert d["id"] == 70
        assert d["ip"] == "172.16.0.70"
        assert d["total_calls"] >= 3
        assert any(c["id"] == 71 for c in d["top_co_talkers"])
    finally:
        srv.attach_event_log(None)  # type: ignore[arg-type]
        log.close()
