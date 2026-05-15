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


def test_version_endpoint(client):
    from backend import __build_date__, __version__
    r = client.get("/api/version")
    assert r.status_code == 200
    data = r.json()
    assert data["version"] == __version__
    assert data["build_date"] == __build_date__
    # Sanity: semver-ish "X.Y.Z"
    parts = data["version"].split(".")
    assert len(parts) == 3 and all(p.isdigit() for p in parts)


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


# ---------------------------------------------------------------------------
# Historical /api/history endpoint — backed by the on-disk JSONL file.
# ---------------------------------------------------------------------------


@pytest.fixture
def history_client(tmp_path):
    """A client whose EventLog is bound to a real on-disk JSONL file."""
    state = StateManager()
    log_path = tmp_path / "events.jsonl"
    log = EventLog(jsonl_path=log_path, capacity=100)
    log.append(_voice(0, 101, 9))
    log.append(_voice(5, 102, 9))
    log.append(QualityEvent(
        timestamp=datetime(2026, 5, 10, 21, 0, 10),
        raw_line="x", error_type="CSBK_CRC",
    ))
    log.append(_voice(20, 101, 7))
    log.close()
    # Re-open in append-only mode so the server has a path attached, but
    # we stop appending — the file already has all the test data.
    log2 = EventLog(jsonl_path=log_path, capacity=100)
    srv.attach_state(state)
    srv.attach_event_log(log2)
    return TestClient(srv.app)


def test_history_endpoint_returns_persisted_events(history_client):
    r = history_client.get("/api/history?limit=100")
    assert r.status_code == 200
    data = r.json()
    assert len(data["events"]) == 4
    assert data["truncated"] is False
    assert data["events"][0]["src"] == 101


def test_history_endpoint_filters_by_radio(history_client):
    r = history_client.get("/api/history?src=101")
    events = r.json()["events"]
    # voice(0,101,9) and voice(20,101,7) — quality has no src field.
    assert len(events) == 2
    assert {e["tgt"] for e in events} == {9, 7}


def test_history_endpoint_filters_by_target(history_client):
    r = history_client.get("/api/history?tgt=9")
    events = r.json()["events"]
    assert len(events) == 2


def test_history_endpoint_filters_by_type(history_client):
    r = history_client.get("/api/history?types=quality")
    events = r.json()["events"]
    assert len(events) == 1
    assert events[0]["error_type"] == "CSBK_CRC"


def test_history_endpoint_paginates_with_offset_and_limit(history_client):
    r = history_client.get("/api/history?limit=2&offset=0")
    page1 = r.json()
    assert len(page1["events"]) == 2
    assert page1["truncated"] is True

    r = history_client.get("/api/history?limit=2&offset=2")
    page2 = r.json()
    assert len(page2["events"]) == 2
    assert page2["truncated"] is False
    # Pages don't overlap.
    p1_ts = {e["timestamp"] for e in page1["events"]}
    p2_ts = {e["timestamp"] for e in page2["events"]}
    assert p1_ts.isdisjoint(p2_ts)


def test_history_csv_export(history_client):
    r = history_client.get("/api/history.csv?types=voice_call")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/csv")
    assert "attachment" in r.headers["content-disposition"]
    lines = r.text.strip().splitlines()
    # header + 3 voice_call rows
    assert len(lines) == 4
    assert lines[0].startswith("timestamp,type")
