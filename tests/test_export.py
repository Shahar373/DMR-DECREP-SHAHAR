"""Phase-3 tests: /api/days + /api/export + day= history filter (v0.20.0)."""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from backend import server as srv
from backend.event_log import EventLog
from backend.models import QualityEvent, VoiceCallEvent
from backend.state import StateManager


def _voice(ts: datetime, src: int = 101, tgt: int = 9) -> VoiceCallEvent:
    return VoiceCallEvent(timestamp=ts, raw_line="v", slot=1, src=src, tgt=tgt)


@pytest.fixture
def day_client(tmp_path):
    """Server bound to a partitioned EventLog spanning two days."""
    log = EventLog(
        jsonl_path=tmp_path / "events.jsonl", capacity=100, partition=True,
    )
    d1 = datetime(2026, 7, 10, 22, 0, 0)
    d2 = datetime(2026, 7, 11, 8, 0, 0)
    for i in range(4):
        log.append(_voice(d1 + timedelta(seconds=i)))
    log.append(QualityEvent(
        timestamp=d1 + timedelta(seconds=10), raw_line="q",
        error_type="CSBK_CRC",
    ))
    for i in range(3):
        log.append(_voice(d2 + timedelta(seconds=i), src=202))
    log.index.flush()
    srv.attach_state(StateManager())
    srv.attach_event_log(log)
    yield TestClient(srv.app), log, tmp_path
    log.close()


def test_api_days_lists_both_days_with_counts(day_client):
    client, _, _ = day_client
    days = client.get("/api/days").json()["days"]
    assert [d["day"] for d in days] == ["2026-07-10", "2026-07-11"]
    assert days[0]["events"] == 5
    assert days[0]["voice_events"] == 4
    assert days[1]["events"] == 3
    assert days[1]["first_ts"].startswith("2026-07-11T08:00:00")


def test_export_ndjson_single_day_is_raw_day_file(day_client):
    client, log, tmp_path = day_client
    r = client.get("/api/export?day=2026-07-10&format=ndjson")
    assert r.status_code == 200
    raw = (tmp_path / "events" / "events-2026-07-10.jsonl").read_bytes()
    assert r.content == raw  # byte-identical — the file IS the export
    assert "dmr_2026-07-10.ndjson" in r.headers["content-disposition"]


def test_export_csv_single_day(day_client):
    client, _, _ = day_client
    r = client.get("/api/export?day=2026-07-11&format=csv")
    assert r.status_code == 200
    lines = r.text.strip().splitlines()
    assert len(lines) == 4  # header + 3 events
    assert lines[0].startswith("timestamp,type")
    assert all("2026-07-11" in ln for ln in lines[1:])


def test_export_range_and_filters(day_client):
    client, _, _ = day_client
    r = client.get("/api/export?from=2026-07-10&to=2026-07-11&format=ndjson"
                   "&types=voice_call")
    assert r.status_code == 200
    objs = [json.loads(ln) for ln in r.text.strip().splitlines()]
    assert len(objs) == 7
    assert all(o["type"] == "voice_call" for o in objs)
    r = client.get("/api/export?day=2026-07-11&format=ndjson&src=202")
    assert len(r.text.strip().splitlines()) == 3


def test_export_404_on_empty_day(day_client):
    client, _, _ = day_client
    assert client.get("/api/export?day=2026-01-01&format=csv").status_code == 404


def test_export_validation(day_client):
    client, _, _ = day_client
    assert client.get("/api/export").status_code == 422
    assert client.get("/api/export?day=11-07-2026").status_code == 422
    assert client.get(
        "/api/export?from=2026-07-12&to=2026-07-10"
    ).status_code == 422
    assert client.get(
        "/api/export?day=2026-07-10&format=xml"
    ).status_code == 422


def test_history_day_filter(day_client):
    client, _, _ = day_client
    r = client.get("/api/history?day=2026-07-10&limit=100")
    events = r.json()["events"]
    assert len(events) == 5
    assert all(e["timestamp"].startswith("2026-07-10") for e in events)
    assert client.get("/api/history?day=bogus").status_code == 422


def test_history_csv_day_filter(day_client):
    client, _, _ = day_client
    r = client.get("/api/history.csv?day=2026-07-11")
    lines = r.text.strip().splitlines()
    assert len(lines) == 4  # header + 3


# ── /api/stats/day/{day} ─────────────────────────────────────────────


def test_stats_day_shape_matches_live_stats(day_client):
    client, _, _ = day_client
    s = client.get("/api/stats/day/2026-07-10").json()
    assert s["day"] == "2026-07-10"
    assert s["window_size"] == 5
    assert s["events_by_type"] == {"voice_call": 4, "quality": 1}
    assert s["calls_by_src"] == {"101": 4}
    assert s["calls_by_tg"] == {"9": 4}
    assert s["quality_by_kind"] == {"CSBK_CRC": 1}
    assert s["first_event_at"].startswith("2026-07-10T22:00:00")
    # Hourly buckets keyed like the live stats ("YYYY-MM-DD HH:00").
    assert s["hourly"] == {"2026-07-10 22:00": 5}
    # Quality ratios computed server-side, same shape as /api/quality.
    assert "overall" in s["quality_ratios"]
    assert s["quality_ratios"]["overall"]["errors"] == 1


def test_stats_day_404_on_empty_day(day_client):
    client, _, _ = day_client
    assert client.get("/api/stats/day/2026-01-01").status_code == 404
    assert client.get("/api/stats/day/bogus").status_code == 422
