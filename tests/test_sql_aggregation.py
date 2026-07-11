"""Phase-2 tests (v0.19.0): SQL-side aggregation + bounded live state.

* ``compute_talker_pairs`` now consumes ``EventIndex.pair_counts`` (GROUP
  BY in SQLite) instead of pulling up to 500k payload rows into Python.
  The equivalence test below re-derives the graph with the OLD row-by-row
  logic straight from ``index.query`` and asserts identical output.
* Dossier aggregates (radio_bounds, hourly_histogram, count_by_tgt,
  count_encryption_by_slot).
* StateManager LRU eviction (--max-radios).
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from backend.event_index import EventIndex
from backend.models import (
    DataHeaderEvent,
    EncryptionEvent,
    LRRPPositionEvent,
    LRRPRequestEvent,
    PreambleCSBKEvent,
    VoiceCallEvent,
)
from backend.network import _PAIR_TYPES, _classify, compute_talker_pairs
from backend.state import StateManager

_T0 = datetime(2026, 5, 10, 21, 0, 0)


def _ts(seconds: int) -> datetime:
    return _T0 + timedelta(seconds=seconds)


def _voice(sec: int, src: int, tgt: int, slot: int = 1) -> VoiceCallEvent:
    return VoiceCallEvent(
        timestamp=_ts(sec), raw_line="v", slot=slot, src=src, tgt=tgt,
    )


def _seed_mixed_index(tmp_path: Path) -> EventIndex:
    """A busy little net: group traffic on two TGs, private traffic,
    data headers both individual and group, GPS, encryption."""
    idx = EventIndex(tmp_path / "events.db")
    evs = []
    # TG 9: radios 70, 71, 72 with different keyup counts.
    for s in range(6):
        evs.append(_voice(s, 70, 9))
    for s in range(4):
        evs.append(_voice(20 + s, 71, 9))
    for s in range(2):
        evs.append(_voice(40 + s, 72, 9))
    # TG 1: radios 70 and 72.
    for s in range(3):
        evs.append(_voice(60 + s, 70, 1, slot=2))
    for s in range(5):
        evs.append(_voice(80 + s, 72, 1, slot=2))
    # Private: 70 -> 71 CSBKs, 71 -> 70 LRRP requests.
    for s in range(3):
        evs.append(PreambleCSBKEvent(
            timestamp=_ts(100 + s), raw_line="p",
            addressing="Individual", kind="Data", src=70, tgt=71,
        ))
    for s in range(2):
        evs.append(LRRPRequestEvent(
            timestamp=_ts(110 + s), raw_line="l",
            direction="Request", src=71, tgt=70,
        ))
    # Group data header (counts toward group edges).
    evs.append(DataHeaderEvent(
        timestamp=_ts(120), raw_line="d", slot=1,
        addressing="Group", delivery="Unconfirmed Delivery", src=71, tgt=9,
    ))
    # Placeholder src=0 rows must be ignored.
    evs.append(_voice(130, 0, 9))
    # GPS for radio 70 only.
    evs.append(LRRPPositionEvent(
        timestamp=_ts(140), raw_line="g", src=70, lat=32.0, lon=34.0,
    ))
    # Encryption events on slot 2.
    for s in range(3):
        evs.append(EncryptionEvent(
            timestamp=_ts(150 + s), raw_line="e", slot=2,
            flco="0x04", fid="0x80",
        ))
    for ev in evs:
        idx.append(ev.model_dump(mode="json"))
    idx.flush()
    return idx


def _reference_graph(index: EventIndex, window_seconds: int, now: datetime,
                     min_weight: int = 1) -> dict:
    """The pre-0.19.0 row-by-row implementation, kept here as the
    equivalence oracle for the GROUP-BY rewrite."""
    since = now - timedelta(seconds=window_seconds)
    rows = index.query(since=since, types=list(_PAIR_TYPES), limit=500_000)
    radio_total_calls: Counter[int] = Counter()
    radio_last_seen: dict[int, str] = {}
    tg_radio_count: dict[int, Counter[int]] = defaultdict(Counter)
    private_pair: Counter[tuple[int, int]] = Counter()
    for row in rows:
        src, tgt = row.get("src"), row.get("tgt")
        if src is None or tgt is None or src == 0 or tgt == 0:
            continue
        kind = _classify(row.get("type"), row.get("addressing"))
        if kind is None:
            continue
        ts = row.get("timestamp", "")
        radio_total_calls[src] += 1
        if ts > radio_last_seen.get(src, ""):
            radio_last_seen[src] = ts
        if kind == "group":
            tg_radio_count[tgt][src] += 1
        else:
            private_pair[(src, tgt)] += 1
            radio_total_calls[tgt] += 1
            if ts > radio_last_seen.get(tgt, ""):
                radio_last_seen[tgt] = ts
    group_weights: dict[tuple[int, int], int] = defaultdict(int)
    group_shared_tgs: dict[tuple[int, int], set[int]] = defaultdict(set)
    for tg, per_radio in tg_radio_count.items():
        radios = sorted(per_radio)
        for i, a in enumerate(radios):
            for b in radios[i + 1:]:
                group_weights[(a, b)] += min(per_radio[a], per_radio[b])
                group_shared_tgs[(a, b)].add(tg)
    private_weights: dict[tuple[int, int], int] = defaultdict(int)
    for (a, b), c in private_pair.items():
        pair = (a, b) if a < b else (b, a)
        private_weights[pair] += c
    edges = []
    for pair, w in group_weights.items():
        if w >= min_weight:
            a, b = pair
            edges.append({"src_a": a, "src_b": b, "weight": int(w),
                          "kind": "group", "tgs": sorted(group_shared_tgs[pair])})
    for pair, w in private_weights.items():
        if w >= min_weight:
            a, b = pair
            edges.append({"src_a": a, "src_b": b, "weight": int(w),
                          "kind": "private", "tgs": []})
    edges.sort(key=lambda e: e["weight"], reverse=True)
    gps = {r.get("src") for r in index.query(
        since=since, types=["lrrp_position"], limit=500_000,
    ) if r.get("src") is not None}
    ids = {e["src_a"] for e in edges} | {e["src_b"] for e in edges}
    nodes = [{"id": rid,
              "total_calls": int(radio_total_calls.get(rid, 0)),
              "last_seen": radio_last_seen.get(rid),
              "encrypted_call_count": 0,
              "has_gps": rid in gps}
             for rid in sorted(ids)]
    return {"nodes": nodes, "edges": edges}


def _edge_key(e: dict):
    return (e["kind"], e["src_a"], e["src_b"])


def test_network_group_by_matches_row_by_row_reference(tmp_path):
    idx = _seed_mixed_index(tmp_path)
    try:
        now = _ts(3600)
        new = compute_talker_pairs(idx, window_seconds=7200, now=now)
        ref = _reference_graph(idx, window_seconds=7200, now=now)
        assert new["nodes"] == ref["nodes"]
        assert sorted(new["edges"], key=_edge_key) == sorted(
            ref["edges"], key=_edge_key,
        )
        assert len(new["edges"]) > 0  # the fixture actually exercises both kinds
        kinds = {e["kind"] for e in new["edges"]}
        assert kinds == {"group", "private"}
    finally:
        idx.close()


def test_network_min_weight_and_limit_still_apply(tmp_path):
    idx = _seed_mixed_index(tmp_path)
    try:
        now = _ts(3600)
        heavy = compute_talker_pairs(idx, window_seconds=7200, now=now,
                                     min_weight=3)
        all_edges = compute_talker_pairs(idx, window_seconds=7200, now=now)
        assert len(heavy["edges"]) < len(all_edges["edges"])
        assert all(e["weight"] >= 3 for e in heavy["edges"])
        top1 = compute_talker_pairs(idx, window_seconds=7200, now=now, limit=1)
        assert len(top1["edges"]) == 1
        assert top1["edges"][0]["weight"] == all_edges["edges"][0]["weight"]
    finally:
        idx.close()


# ── Dossier aggregate helpers ────────────────────────────────────────


def test_radio_bounds_merges_src_and_tgt_sides(tmp_path):
    idx = _seed_mixed_index(tmp_path)
    try:
        # Radio 71: src rows at 20..23, 100..102 (as tgt), 110..111 (as src),
        # tgt side includes 100..102. First as src/tgt = ts(20); last: the
        # data header at 120 has src=71.
        first, last = idx.radio_bounds(71)
        assert first == _ts(20).isoformat()
        assert last == _ts(120).isoformat()
    finally:
        idx.close()


def test_hourly_histogram_buckets_by_hour(tmp_path):
    idx = EventIndex(tmp_path / "h.db")
    try:
        idx.append(_voice(0, 70, 9).model_dump(mode="json"))          # 21:00
        idx.append(_voice(3700, 70, 9).model_dump(mode="json"))       # 22:01
        idx.append(_voice(3800, 70, 9).model_dump(mode="json"))       # 22:03
        # tgt-side row for radio 70 in hour 23.
        idx.append(PreambleCSBKEvent(
            timestamp=_ts(2 * 3600 + 60), raw_line="p",
            addressing="Individual", kind="Data", src=71, tgt=70,
        ).model_dump(mode="json"))
        idx.flush()
        hourly = idx.hourly_histogram(70)
        assert hourly[21] == 1
        assert hourly[22] == 2
        assert hourly[23] == 1
        assert sum(hourly) == 4
    finally:
        idx.close()


def test_count_by_tgt_groups_voice_calls(tmp_path):
    idx = _seed_mixed_index(tmp_path)
    try:
        counts = idx.count_by_tgt(70, types=["voice_call"])
        assert counts == {9: 6, 1: 3}
    finally:
        idx.close()


def test_count_encryption_by_slot(tmp_path):
    idx = _seed_mixed_index(tmp_path)
    try:
        assert idx.count_encryption_by_slot() == {2: 3}
    finally:
        idx.close()


def test_query_descending_returns_newest_first(tmp_path):
    idx = _seed_mixed_index(tmp_path)
    try:
        newest = idx.query(types=["voice_call"], limit=3, descending=True)
        ts_list = [r["timestamp"] for r in newest]
        assert ts_list == sorted(ts_list, reverse=True)
        assert newest[0]["timestamp"] >= newest[-1]["timestamp"]
    finally:
        idx.close()


# ── StateManager LRU eviction ────────────────────────────────────────


def test_radios_evicted_past_cap_oldest_first():
    sm = StateManager(max_radios=10)
    for i in range(15):
        sm.apply(_voice(i * 10, 100 + i, 9))
    assert len(sm.radios) <= 10
    assert sm.radios_evicted_total >= 5
    # The newest radios always survive; the oldest are gone.
    assert 114 in sm.radios
    assert 100 not in sm.radios
    snap = sm.snapshot()
    assert snap.radios_evicted_total == sm.radios_evicted_total
    assert snap.radios_total == len(sm.radios)


def test_eviction_is_batched_not_per_insert():
    sm = StateManager(max_radios=100)
    for i in range(101):
        sm.apply(_voice(i, 1000 + i, 9))
    # One batch of ~5% fired once we crossed the cap:
    # batch = max(overshoot=1, cap//20=5, 1) = 5.
    assert sm.radios_evicted_total == 5
    assert len(sm.radios) == 96


def test_load_snapshot_enforces_cap(tmp_path):
    big = StateManager(max_radios=10_000)
    for i in range(30):
        big.apply(_voice(i, 100 + i, 9))
    path = tmp_path / "snap.json"
    path.write_text(big.snapshot().model_dump_json())
    small = StateManager(max_radios=10)
    assert small.load_snapshot(path) is True
    assert len(small.radios) <= 10
    assert small.radios_evicted_total > 0


def test_reset_clears_eviction_counter():
    sm = StateManager(max_radios=5)
    for i in range(10):
        sm.apply(_voice(i, 100 + i, 9))
    assert sm.radios_evicted_total > 0
    sm.reset()
    assert sm.radios_evicted_total == 0
