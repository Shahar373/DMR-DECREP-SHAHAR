"""Talker-Pair Graph — pairwise activity on the DMR net.

Two edge kinds:

* **group** — two radios that keyed up on the same TG (or addressed a group
  CSBK to it) inside the same time window. Weight is the sum, across shared
  TGs, of ``min(count_a_on_tg, count_b_on_tg)``. The ``min`` rewards mutual
  participation rather than chatty-lurker pairs.

* **private** — direct radio-to-radio activity: Individual CSBKs/data headers
  and LRRP requests. Weight is the sum of the two directions.

The output is intentionally JSON-shape ready (``nodes``, ``edges`` lists with
plain dicts) so the FastAPI handler can return it as-is.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Optional

from .event_index import EventIndex


_PAIR_TYPES = ("voice_call", "preamble_csbk", "data_header", "lrrp_request")
_PRIVATE_ADDRESSING = {"Individual", "Indiv"}
_GROUP_ADDRESSING = {"Group"}


def _classify(row_type: str, addressing: Optional[str]) -> Optional[str]:
    """Return 'group', 'private', or None for an event row."""
    if row_type == "voice_call":
        return "group"
    if row_type == "lrrp_request":
        return "private"
    if row_type in ("preamble_csbk", "data_header"):
        if addressing in _PRIVATE_ADDRESSING:
            return "private"
        if addressing in _GROUP_ADDRESSING:
            return "group"
    return None


def compute_talker_pairs(
    index: EventIndex,
    window_seconds: int = 3600,
    min_weight: int = 1,
    now: Optional[datetime] = None,
    limit: Optional[int] = None,
) -> dict:
    """Compute the talker-pair graph over a rolling window.

    ``min_weight`` filters edges; ``limit`` truncates to the top-N by weight.
    Nodes returned are exactly the radios that participate in surviving edges
    plus their first-class attributes (total_calls, last_seen, encryption,
    has_gps) — radios that talked but had no co-talker drop out.
    """
    now = now or datetime.now()
    since = now - timedelta(seconds=window_seconds)

    # Pull every pair-ish row once. payload carries the addressing field we
    # need to classify group vs private without a second query.
    rows = index.query(since=since, types=list(_PAIR_TYPES), limit=10_000_000)

    # Per-radio aggregates (for node attributes).
    radio_total_calls: Counter[int] = Counter()
    radio_last_seen: dict[int, str] = {}
    radio_encrypted_calls: Counter[int] = Counter()

    # Group edges: for each TG, count keyups per radio.
    tg_radio_count: dict[int, Counter[int]] = defaultdict(Counter)
    # Private edges: directed pair counts.
    private_pair: Counter[tuple[int, int]] = Counter()

    for row in rows:
        src = row.get("src")
        tgt = row.get("tgt")
        if src is None or tgt is None:
            continue
        # src=0 is DSD-FME's pre-LC placeholder, not a real radio id.
        if src == 0 or tgt == 0:
            continue
        et = row.get("type")
        kind = _classify(et, row.get("addressing"))
        if kind is None:
            continue
        ts = row.get("timestamp", "")
        radio_total_calls[src] += 1
        # Last-seen: ISO strings sort lexicographically.
        if ts > radio_last_seen.get(src, ""):
            radio_last_seen[src] = ts
        if et == "voice_call":
            # Encrypted is tracked per-call elsewhere; we don't have the
            # encryption flag inline. For now, leave encrypted_call_count
            # at 0 unless extended. (Will be populated by a side query.)
            pass
        if kind == "group":
            tg_radio_count[tgt][src] += 1
        elif kind == "private":
            private_pair[(src, tgt)] += 1

    # Build group edges: for each TG, every pair of distinct radios.
    group_weights: dict[tuple[int, int], int] = defaultdict(int)
    group_shared_tgs: dict[tuple[int, int], set[int]] = defaultdict(set)
    for tg, per_radio in tg_radio_count.items():
        radios = sorted(per_radio.keys())
        if len(radios) < 2:
            continue
        for i, a in enumerate(radios):
            ca = per_radio[a]
            for b in radios[i + 1:]:
                cb = per_radio[b]
                pair = (a, b)
                group_weights[pair] += min(ca, cb)
                group_shared_tgs[pair].add(tg)

    # Build private edges: collapse (a→b) and (b→a) into a single undirected.
    private_weights: dict[tuple[int, int], int] = defaultdict(int)
    for (a, b), c in private_pair.items():
        pair = (a, b) if a < b else (b, a)
        private_weights[pair] += c

    # Encryption side-query: how many encryption events did each radio's slot
    # produce in the window? We don't have direct radio→encryption attribution
    # in the schema, so this stays 0 for v0.10.0. (Operator can still see the
    # global encrypted count on /stats.)
    # has_gps: did this radio emit any lrrp_position in window?
    gps_radios = {
        row.get("src")
        for row in index.query(since=since, types=["lrrp_position"], limit=10_000_000)
        if row.get("src") is not None
    }

    edges: list[dict] = []
    for pair, w in group_weights.items():
        if w < min_weight:
            continue
        a, b = pair
        edges.append({
            "src_a": a, "src_b": b, "weight": int(w),
            "kind": "group",
            "tgs": sorted(group_shared_tgs[pair]),
        })
    for pair, w in private_weights.items():
        if w < min_weight:
            continue
        a, b = pair
        edges.append({
            "src_a": a, "src_b": b, "weight": int(w),
            "kind": "private", "tgs": [],
        })

    edges.sort(key=lambda e: e["weight"], reverse=True)
    if limit is not None:
        edges = edges[:limit]

    surviving_ids: set[int] = set()
    for e in edges:
        surviving_ids.add(e["src_a"])
        surviving_ids.add(e["src_b"])

    nodes = [
        {
            "id": rid,
            "total_calls": int(radio_total_calls.get(rid, 0)),
            "last_seen": radio_last_seen.get(rid),
            "encrypted_call_count": int(radio_encrypted_calls.get(rid, 0)),
            "has_gps": rid in gps_radios,
        }
        for rid in sorted(surviving_ids)
    ]

    return {
        "nodes": nodes,
        "edges": edges,
        "window_seconds": window_seconds,
        "generated_at": now.isoformat(),
    }
