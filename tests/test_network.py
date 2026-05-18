"""Tests for backend.network (Talker-Pair Graph)."""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from backend import server as srv
from backend.event_log import EventLog
from backend.network import _classify, compute_talker_pairs
from backend.models import (
    LRRPPositionEvent,
    LRRPRequestEvent,
    PreambleCSBKEvent,
    VoiceCallEvent,
)


# Anchor on wall-clock time (one hour ago) so the synthetic events always
# fall inside the network endpoint's 7-day max window — the endpoint
# computes ``now - window`` itself and there's no hook to override it from
# a test. Captured once at import so every event in a single test run
# shares the same baseline.
_TS_BASE = datetime.now() - timedelta(hours=1)


def _ts(seconds: int = 0) -> datetime:
    return _TS_BASE + timedelta(seconds=seconds)


def _voice(sec: int, src: int, tgt: int, slot: int = 1) -> VoiceCallEvent:
    return VoiceCallEvent(
        timestamp=_ts(sec), raw_line="voice", slot=slot, src=src, tgt=tgt,
    )


def _csbk(sec: int, src: int, tgt: int, addressing: str = "Group") -> PreambleCSBKEvent:
    return PreambleCSBKEvent(
        timestamp=_ts(sec), raw_line="csbk", addressing=addressing,
        kind="Voice", src=src, tgt=tgt,
    )


def _lrrp_req(sec: int, src: int, tgt: int) -> LRRPRequestEvent:
    return LRRPRequestEvent(
        timestamp=_ts(sec), raw_line="lrrp", src=src, tgt=tgt, direction="Request",
    )


def test_pair_construction_from_tiny_event_set(tmp_path: Path) -> None:
    log = EventLog(jsonl_path=tmp_path / "events.jsonl", capacity=100)
    try:
        # Three radios all talk on TG 9:
        #   100 → 2 calls, 101 → 3 calls, 102 → 1 call.
        log.append(_voice(0, 100, 9))
        log.append(_voice(1, 100, 9))
        log.append(_voice(2, 101, 9))
        log.append(_voice(3, 101, 9))
        log.append(_voice(4, 101, 9))
        log.append(_voice(5, 102, 9))
        # Private direct: 100 ↔ 200, two-way.
        log.append(_csbk(10, 100, 200, addressing="Individual"))
        log.append(_csbk(11, 200, 100, addressing="Individual"))
        graph = compute_talker_pairs(
            log.index, window_seconds=24 * 3600, min_weight=1, now=_ts(12),
        )
    finally:
        log.close()

    edges = {(e["src_a"], e["src_b"], e["kind"]): e["weight"] for e in graph["edges"]}
    # Group edges on TG 9: min(2,3)=2, min(2,1)=1, min(3,1)=1.
    assert edges[(100, 101, "group")] == 2
    assert edges[(100, 102, "group")] == 1
    assert edges[(101, 102, "group")] == 1
    # Private edge collapses both directions.
    assert edges[(100, 200, "private")] == 2
    # Nodes: only participants in surviving edges.
    ids = {n["id"] for n in graph["nodes"]}
    assert ids == {100, 101, 102, 200}


def test_window_honoured(tmp_path: Path) -> None:
    log = EventLog(jsonl_path=tmp_path / "events.jsonl", capacity=100)
    try:
        # Old event (2h ago).
        log.append(_voice(-7200, 100, 9))
        log.append(_voice(-7199, 101, 9))
        # Recent event (in last 30m).
        log.append(_voice(0, 100, 9))
        log.append(_voice(1, 101, 9))
        # Use a 30-minute window anchored at the recent timestamp.
        graph = compute_talker_pairs(
            log.index, window_seconds=1800, min_weight=1, now=_ts(2),
        )
    finally:
        log.close()
    assert len(graph["edges"]) == 1
    assert graph["edges"][0]["weight"] == 1  # only the recent pair contributes


def test_min_weight_filters_edges(tmp_path: Path) -> None:
    log = EventLog(jsonl_path=tmp_path / "events.jsonl", capacity=100)
    try:
        # Pair (100,101) has weight 1 on TG 9; (100,102) has weight 3.
        log.append(_voice(0, 100, 9))
        log.append(_voice(1, 101, 9))
        for s in range(3):
            log.append(_voice(2 + s, 102, 9))
        for s in range(3):
            log.append(_voice(10 + s, 100, 9))  # to give 100 ≥ 3 calls
        graph = compute_talker_pairs(
            log.index, window_seconds=24 * 3600, min_weight=3, now=_ts(20),
        )
    finally:
        log.close()
    kinds = [(e["src_a"], e["src_b"]) for e in graph["edges"]]
    assert (100, 102) in kinds  # weight ≥ 3
    assert (100, 101) not in kinds  # weight 1 → filtered


def test_group_vs_private_edge_kinds(tmp_path: Path) -> None:
    log = EventLog(jsonl_path=tmp_path / "events.jsonl", capacity=100)
    try:
        log.append(_voice(0, 1, 9))
        log.append(_voice(1, 2, 9))  # group via voice_call
        log.append(_csbk(2, 3, 4, addressing="Group"))  # group via group CSBK
        log.append(_csbk(3, 5, 5, addressing="Group"))  # singleton, no edge
        # Need a second radio on TG 4 for a group edge to form there:
        log.append(_csbk(4, 6, 4, addressing="Group"))
        log.append(_lrrp_req(5, 7, 8))  # private via LRRP request
        log.append(_csbk(6, 9, 10, addressing="Individual"))  # private via Individual CSBK
        graph = compute_talker_pairs(
            log.index, window_seconds=24 * 3600, min_weight=1, now=_ts(10),
        )
    finally:
        log.close()
    kinds = {e["kind"] for e in graph["edges"]}
    assert kinds == {"group", "private"}
    # The Individual CSBK and the LRRP request both produce private edges.
    private_pairs = {(e["src_a"], e["src_b"]) for e in graph["edges"] if e["kind"] == "private"}
    assert (7, 8) in private_pairs
    assert (9, 10) in private_pairs


def test_classify_helper() -> None:
    assert _classify("voice_call", None) == "group"
    assert _classify("lrrp_request", None) == "private"
    assert _classify("preamble_csbk", "Individual") == "private"
    assert _classify("preamble_csbk", "Group") == "group"
    assert _classify("data_header", "Indiv") == "private"
    assert _classify("data_header", "Group") == "group"
    assert _classify("preamble_csbk", "Bogus") is None
    assert _classify("quality", None) is None


def test_network_endpoint_smoke(tmp_path: Path) -> None:
    log = EventLog(jsonl_path=tmp_path / "events.jsonl", capacity=200)
    log.append(_voice(0, 100, 9))
    log.append(_voice(1, 101, 9))
    # Add an lrrp_position so has_gps works.
    log.append(LRRPPositionEvent(
        timestamp=_ts(2), raw_line="pos", src=100, lat=32.0, lon=34.0,
    ))
    srv.attach_event_log(log)
    try:
        client = TestClient(srv.app)
        # The endpoint computes ``now - window`` itself (no override hook),
        # so the events must be anchored close enough to wall-clock time
        # that the 7-day max window includes them.
        resp = client.get("/api/network", params={"window": 7 * 86400, "min_weight": 1})
        assert resp.status_code == 200
        data = resp.json()
        assert "nodes" in data and "edges" in data
        assert len(data["edges"]) == 1
        ids = {n["id"] for n in data["nodes"]}
        assert ids == {100, 101}
        gps_for_100 = [n["has_gps"] for n in data["nodes"] if n["id"] == 100][0]
        assert gps_for_100 is True
    finally:
        srv.attach_event_log(None)  # type: ignore[arg-type]
        log.close()


def test_network_endpoint_empty_index(tmp_path: Path) -> None:
    log = EventLog(jsonl_path=tmp_path / "events.jsonl", capacity=20)
    srv.attach_event_log(log)
    try:
        client = TestClient(srv.app)
        resp = client.get("/api/network")
        assert resp.status_code == 200
        data = resp.json()
        assert data["nodes"] == []
        assert data["edges"] == []
    finally:
        srv.attach_event_log(None)  # type: ignore[arg-type]
        log.close()


def test_network_page_served(tmp_path: Path) -> None:
    client = TestClient(srv.app)
    resp = client.get("/network")
    assert resp.status_code == 200
    assert "Talker Network" in resp.text
    assert "cytoscape" in resp.text
