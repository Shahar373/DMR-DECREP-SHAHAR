"""End-to-end tests for the /api/events* server endpoints."""
from __future__ import annotations

from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from backend import server as srv
from backend.event_log import EventLog
from backend.models import QualityEvent, VoiceCallEvent
from backend.state import StateManager


def _voice(seconds: int, src: int, tgt: int) -> VoiceCallEvent:
    return VoiceCallEvent(
        timestamp=datetime(2026, 5, 10, 21, 0, seconds),
        raw_line="voice", slot=1, src=src, tgt=tgt,
    )


@pytest.fixture
def client():
    state = StateManager()
    log = EventLog(jsonl_path=None, capacity=100)
    log.append(_voice(0, 101, 9))
    log.append(_voice(5, 102, 9))
    log.append(QualityEvent(
        timestamp=datetime(2026, 5, 10, 21, 0, 10),
        raw_line="x", error_type="CSBK_CRC",
    ))
    srv.attach_state(state)
    srv.attach_event_log(log)
    return TestClient(srv.app)


def test_events_endpoint_returns_buffered_events(client):
    r = client.get("/api/events")
    assert r.status_code == 200
    data = r.json()
    assert data["total_buffered"] == 3
    assert len(data["events"]) == 3


def test_events_endpoint_filters_by_type(client):
    r = client.get("/api/events?types=voice_call")
    assert r.status_code == 200
    events = r.json()["events"]
    assert len(events) == 2
    assert all(e["type"] == "voice_call" for e in events)


def test_events_csv_export_returns_attachment(client):
    r = client.get("/api/events.csv")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/csv")
    assert "attachment" in r.headers["content-disposition"]
    body = r.text
    lines = body.strip().splitlines()
    # Header + 3 events
    assert len(lines) == 4
    assert lines[0].startswith("timestamp,type")
    assert "voice_call" in body
    assert "CSBK_CRC" in body


def test_events_csv_export_filters_by_type(client):
    r = client.get("/api/events.csv?types=quality")
    assert r.status_code == 200
    lines = r.text.strip().splitlines()
    assert len(lines) == 2  # header + 1 row
    assert "CSBK_CRC" in lines[1]


def test_stats_endpoint(client):
    r = client.get("/api/stats")
    assert r.status_code == 200
    s = r.json()
    assert s["window_size"] == 3
    assert s["events_by_type"]["voice_call"] == 2
    assert s["calls_by_src"]["101"] == 1
    assert s["calls_by_tg"]["9"] == 2
