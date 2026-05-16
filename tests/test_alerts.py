"""Tests for backend.alerts.Evaluator and the /api/alerts endpoints."""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from backend import server as srv
from backend.alerts import (
    CcSilentRule,
    EncryptionRule,
    Evaluator,
    QualitySpikeRule,
    RadioKeyupRule,
    rule_from_dict,
)
from backend.event_log import EventLog
from backend.models import (
    ChannelStatusEvent,
    EncryptionEvent,
    QualityEvent,
    SiteInfoEvent,
    VoiceCallEvent,
)


def _ts(seconds: int = 0) -> datetime:
    return datetime(2026, 5, 10, 21, 0, 0) + timedelta(seconds=seconds)


def _voice(sec: int, src: int, tgt: int, slot: int = 1) -> VoiceCallEvent:
    return VoiceCallEvent(
        timestamp=_ts(sec), raw_line="v", slot=slot, src=src, tgt=tgt,
    )


# ── rule kind: radio_keyup ─────────────────────────────────────────────

def test_radio_keyup_fires_once_per_call_not_per_frame() -> None:
    ev = Evaluator()
    ev.add_rule(RadioKeyupRule(name="watch 2102", radio_ids=[2102], cooldown_seconds=0))
    # Five voice frames = one keyup.
    for s in range(5):
        ev.on_event(_voice(s, 2102, 9))
    assert len(ev.recent_firings()) == 1
    firing = ev.recent_firings()[0]
    assert firing.kind == "radio_keyup"
    assert firing.context["src"] == 2102 and firing.context["tgt"] == 9


def test_radio_keyup_re_fires_on_new_call_after_idle() -> None:
    ev = Evaluator()
    ev.add_rule(RadioKeyupRule(name="watch 2102", radio_ids=[2102], cooldown_seconds=0))
    ev.on_event(_voice(0, 2102, 9))
    # Same src/tgt, far later but same key — Evaluator forgets the key
    # only on a different (slot, src, tgt). Use a different tgt to simulate
    # a fresh keyup.
    ev.on_event(_voice(60, 2102, 1))
    assert len(ev.recent_firings()) == 2


def test_radio_keyup_ignores_radios_not_in_list() -> None:
    ev = Evaluator()
    ev.add_rule(RadioKeyupRule(name="watch 2102", radio_ids=[2102]))
    ev.on_event(_voice(0, 999, 9))
    assert ev.recent_firings() == []


def test_radio_keyup_respects_cooldown() -> None:
    ev = Evaluator()
    ev.add_rule(RadioKeyupRule(name="watch", radio_ids=[2102], cooldown_seconds=120))
    ev.on_event(_voice(0, 2102, 9))
    # New call key (tgt changed) but inside the cooldown window — must not fire.
    ev.on_event(_voice(30, 2102, 1))
    assert len(ev.recent_firings()) == 1


# ── rule kind: encryption ──────────────────────────────────────────────

def test_encryption_rule_joins_against_active_call_for_tg() -> None:
    ev = Evaluator()
    ev.add_rule(EncryptionRule(name="enc on TG 9", tg_ids=[9], cooldown_seconds=0))
    ev.on_event(_voice(0, 2102, 9, slot=1))  # populate slot→(src,tgt) cache
    ev.on_event(EncryptionEvent(
        timestamp=_ts(1), raw_line="e", slot=1, flco="0x04", fid="0x80",
    ))
    firings = ev.recent_firings()
    assert len(firings) == 1
    assert firings[0].context["tg"] == 9
    assert firings[0].context["src"] == 2102


def test_encryption_rule_filters_by_tg() -> None:
    ev = Evaluator()
    ev.add_rule(EncryptionRule(name="enc on TG 1 only", tg_ids=[1]))
    ev.on_event(_voice(0, 2102, 9, slot=1))  # TG 9, not in filter
    ev.on_event(EncryptionEvent(
        timestamp=_ts(1), raw_line="e", slot=1, flco="0x04", fid="0x80",
    ))
    assert ev.recent_firings() == []


def test_encryption_rule_with_empty_tg_filter_matches_any() -> None:
    ev = Evaluator()
    ev.add_rule(EncryptionRule(name="any enc", tg_ids=[]))
    ev.on_event(_voice(0, 2102, 9, slot=2))
    ev.on_event(EncryptionEvent(
        timestamp=_ts(1), raw_line="e", slot=2, flco="0x04", fid="0x80",
    ))
    assert len(ev.recent_firings()) == 1


# ── rule kind: cc_silent ───────────────────────────────────────────────

def test_cc_silent_fires_after_threshold_then_latches() -> None:
    ev = Evaluator()
    ev.add_rule(CcSilentRule(name="cc silent 30s", timeout_seconds=30))
    # Baseline heartbeat.
    ev.on_event(SiteInfoEvent(
        timestamp=_ts(0), raw_line="s", site=2, rest_lsn=4, rs="00",
    ))
    # 15s later — still inside threshold — no firing.
    ev.tick(now=_ts(15))
    assert ev.recent_firings() == []
    # 45s later — over threshold — fires once.
    ev.tick(now=_ts(45))
    assert len(ev.recent_firings()) == 1
    # Another tick during the same silent run — must NOT re-fire.
    ev.tick(now=_ts(60))
    assert len(ev.recent_firings()) == 1


def test_cc_silent_re_arms_when_cc_returns() -> None:
    ev = Evaluator()
    ev.add_rule(CcSilentRule(name="cc silent", timeout_seconds=10))
    ev.on_event(ChannelStatusEvent(
        timestamp=_ts(0), raw_line="cs", fl=3, ts=0, rs=0, rest_lsn=3,
        block_type="Single",
    ))
    ev.tick(now=_ts(15))  # fires
    assert len(ev.recent_firings()) == 1
    # CC traffic comes back.
    ev.on_event(ChannelStatusEvent(
        timestamp=_ts(20), raw_line="cs", fl=3, ts=0, rs=0, rest_lsn=3,
        block_type="Single",
    ))
    # Goes silent again.
    ev.tick(now=_ts(40))  # 20s since last CC — over the 10s threshold → fires again
    assert len(ev.recent_firings()) == 2


def test_cc_silent_does_not_fire_at_cold_start() -> None:
    """Until we've seen at least one CC event, "silent" is meaningless."""
    ev = Evaluator()
    ev.add_rule(CcSilentRule(name="cc silent", timeout_seconds=5))
    ev.tick(now=_ts(1000))
    assert ev.recent_firings() == []


# ── rule kind: quality_spike ───────────────────────────────────────────

def test_quality_spike_fires_when_rate_exceeds_threshold(tmp_path: Path) -> None:
    log = EventLog(jsonl_path=tmp_path / "events.jsonl", capacity=200)
    try:
        # 50 successful CSBKs + 10 CRC errors → ~16.7% rate → over 5%.
        for i in range(50):
            log.append(ChannelStatusEvent(
                timestamp=_ts(i), raw_line="cs", fl=3, ts=0, rs=0, rest_lsn=3,
                block_type="Single",
            ))
        for i in range(10):
            log.append(QualityEvent(
                timestamp=_ts(i), raw_line="q", error_type="CSBK_CRC",
            ))
        ev = Evaluator(event_log=log)
        ev.add_rule(QualitySpikeRule(
            name="link broken", window_seconds=3600,
            rate_threshold=0.05, cooldown_seconds=0,
        ))
        ev.tick(now=_ts(60))
    finally:
        log.close()
    firings = ev.recent_firings()
    assert len(firings) == 1
    assert firings[0].context["rate"] > 0.05


def test_quality_spike_does_not_fire_on_healthy_link(tmp_path: Path) -> None:
    log = EventLog(jsonl_path=tmp_path / "events.jsonl", capacity=200)
    try:
        for i in range(100):
            log.append(ChannelStatusEvent(
                timestamp=_ts(i), raw_line="cs", fl=3, ts=0, rs=0, rest_lsn=3,
                block_type="Single",
            ))
        # One stray error — well under 5%.
        log.append(QualityEvent(
            timestamp=_ts(0), raw_line="q", error_type="CSBK_CRC",
        ))
        ev = Evaluator(event_log=log)
        ev.add_rule(QualitySpikeRule(
            name="link", window_seconds=3600, rate_threshold=0.05,
        ))
        ev.tick(now=_ts(120))
    finally:
        log.close()
    assert ev.recent_firings() == []


# ── persistence ───────────────────────────────────────────────────────

def test_rules_round_trip_through_disk(tmp_path: Path) -> None:
    p = tmp_path / "alerts.json"
    ev = Evaluator(rules_path=p)
    ev.add_rule(RadioKeyupRule(name="alpha", radio_ids=[1, 2]))
    ev.add_rule(CcSilentRule(name="beta", timeout_seconds=20))
    assert p.exists()

    # Fresh process — load and verify shape.
    ev2 = Evaluator(rules_path=p)
    rules = ev2.list_rules()
    assert {r.name for r in rules} == {"alpha", "beta"}
    keyup = next(r for r in rules if r.name == "alpha")
    assert isinstance(keyup, RadioKeyupRule)
    assert keyup.radio_ids == [1, 2]


def test_corrupt_rules_file_is_renamed_aside(tmp_path: Path) -> None:
    p = tmp_path / "alerts.json"
    p.write_text("{not valid json")
    ev = Evaluator(rules_path=p)
    # Empty rule list — the bad file was moved out of the way.
    assert ev.list_rules() == []
    assert p.with_suffix(".json.bad").exists()


def test_rule_from_dict_validates_discriminator() -> None:
    r = rule_from_dict({"kind": "radio_keyup", "name": "x", "radio_ids": [1]})
    assert isinstance(r, RadioKeyupRule)


# ── HTTP / WebSocket endpoints ─────────────────────────────────────────

def test_alerts_rules_crud_via_http(tmp_path: Path) -> None:
    ev = Evaluator(rules_path=tmp_path / "alerts.json")
    srv.attach_evaluator(ev)
    try:
        client = TestClient(srv.app)
        # Initially empty.
        r = client.get("/api/alerts/rules")
        assert r.status_code == 200
        assert r.json() == {"rules": []}
        # Create.
        r = client.post("/api/alerts/rules", json={
            "kind": "radio_keyup", "name": "watch alice", "radio_ids": [2102],
        })
        assert r.status_code == 200
        rid = r.json()["id"]
        # List.
        r = client.get("/api/alerts/rules")
        assert len(r.json()["rules"]) == 1
        # Toggle.
        r = client.post(f"/api/alerts/rules/{rid}/toggle", json={"enabled": False})
        assert r.status_code == 200 and r.json()["enabled"] is False
        # Delete.
        r = client.delete(f"/api/alerts/rules/{rid}")
        assert r.status_code == 204
        r = client.get("/api/alerts/rules")
        assert r.json() == {"rules": []}
    finally:
        srv.attach_evaluator(None)


def test_alerts_rules_post_rejects_invalid(tmp_path: Path) -> None:
    ev = Evaluator(rules_path=tmp_path / "alerts.json")
    srv.attach_evaluator(ev)
    try:
        client = TestClient(srv.app)
        # Missing radio_ids for radio_keyup.
        r = client.post("/api/alerts/rules", json={
            "kind": "radio_keyup", "name": "bad", "radio_ids": [],
        })
        assert r.status_code == 400
    finally:
        srv.attach_evaluator(None)


def test_alerts_recent_returns_firings(tmp_path: Path) -> None:
    ev = Evaluator(rules_path=tmp_path / "alerts.json")
    ev.add_rule(RadioKeyupRule(name="w", radio_ids=[2102], cooldown_seconds=0))
    ev.on_event(_voice(0, 2102, 9))
    srv.attach_evaluator(ev)
    try:
        client = TestClient(srv.app)
        r = client.get("/api/alerts/recent")
        firings = r.json()["firings"]
        assert len(firings) == 1
        assert firings[0]["rule_name"] == "w"
    finally:
        srv.attach_evaluator(None)


def test_alerts_endpoints_503_when_engine_not_attached() -> None:
    srv.attach_evaluator(None)
    client = TestClient(srv.app)
    # Read endpoints still answer with empty data — easier on a UI that
    # doesn't know whether the operator wired the engine.
    assert client.get("/api/alerts/rules").json() == {"rules": []}
    assert client.get("/api/alerts/recent").json() == {"firings": []}
    # Mutations need an engine — 503.
    r = client.post("/api/alerts/rules", json={
        "kind": "radio_keyup", "name": "x", "radio_ids": [1],
    })
    assert r.status_code == 503
