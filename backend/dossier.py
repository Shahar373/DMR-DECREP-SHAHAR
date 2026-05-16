"""Per-radio Dossier — lifetime stats, co-talkers, calls, positions.

Reads from the SQLite index (v0.9.0) and optionally augments with the
``RecordingRegistry`` so each recent call links to its playable WAV.

Used by ``GET /api/radio/{radio_id}``.
"""
from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timedelta
from typing import Optional

from .event_index import EventIndex
from .network import compute_talker_pairs


# Two consecutive voice_call frames within this many seconds are treated as
# part of the same call (PTT held). dsd-fme emits one frame ~every 60 ms; a
# 2-second gap reliably separates distinct keyups.
_CALL_GAP_SECONDS = 2.0


def _group_calls(voice_rows: list[dict]) -> list[dict]:
    """Collapse a chronological run of voice_call frames into call sessions.

    Each session is keyed by (slot, src, tgt); a new gap of more than
    ``_CALL_GAP_SECONDS`` starts a new session even if the key is unchanged.
    Returns calls sorted oldest-first.
    """
    sessions: list[dict] = []
    current: Optional[dict] = None
    for row in voice_rows:
        ts_raw = row.get("timestamp", "")
        try:
            ts = datetime.fromisoformat(ts_raw)
        except (TypeError, ValueError):
            continue
        key = (row.get("slot"), row.get("src"), row.get("tgt"))
        if current and current["_key"] == key:
            gap = (ts - current["_last_ts"]).total_seconds()
            if gap <= _CALL_GAP_SECONDS:
                current["_last_ts"] = ts
                current["frames"] += 1
                continue
        if current is not None:
            current.pop("_key", None)
            current.pop("_last_ts", None)
            sessions.append(current)
        current = {
            "_key": key,
            "_last_ts": ts,
            "ts": ts_raw,
            "slot": row.get("slot"),
            "src": row.get("src"),
            "tgt": row.get("tgt"),
            "frames": 1,
        }
    if current is not None:
        current.pop("_key", None)
        current.pop("_last_ts", None)
        sessions.append(current)
    return sessions


def _attach_recording(call: dict, recordings) -> Optional[dict]:
    """Find a CallRecording within ±5s of the call start. Best-effort."""
    if recordings is None:
        return None
    try:
        call_ts = datetime.fromisoformat(call["ts"])
    except (TypeError, ValueError):
        return None
    try:
        for rec in recordings.list_recent():
            if rec.src is not None and rec.src != call["src"]:
                continue
            if rec.tgt is not None and rec.tgt != call["tgt"]:
                continue
            gap = abs((rec.started_at - call_ts).total_seconds())
            if gap <= 5.0:
                return {
                    "filename": rec.filename,
                    "duration_s": rec.duration_seconds,
                }
    except Exception:  # noqa: BLE001
        return None
    return None


def build_dossier(
    index: EventIndex,
    radio_id: int,
    window_seconds: int = 24 * 3600,
    now: Optional[datetime] = None,
    recordings=None,
    radio_state=None,
) -> Optional[dict]:
    """Return the dossier dict, or None if the radio is unknown in the window."""
    now = now or datetime.now()
    since = now - timedelta(seconds=window_seconds)

    # Existence: did this id appear as src OR tgt anywhere in the window?
    if index.count(src=radio_id, since=since) == 0 and index.count(tgt=radio_id, since=since) == 0:
        return None

    # IP / last seen from live state where available; fall back to the index.
    ip = None
    if radio_state is not None:
        ip = getattr(radio_state, "ip", None)
    if ip is None:
        ip_rows = index.query(src=radio_id, types=["ip_mapping"], limit=1)
        if ip_rows:
            ip = ip_rows[0].get("ip")

    # Time bounds across both as-src and as-tgt traffic — cheaper to do via
    # the helper than to pull every payload.
    src_bounds = index.time_bounds(since=since) if False else None  # always recompute below
    rows_for_radio = index.query(src=radio_id, since=since, limit=10_000_000)

    voice_rows = [r for r in rows_for_radio if r.get("type") == "voice_call"]
    voice_rows.sort(key=lambda r: r.get("timestamp", ""))

    tg_counts: Counter[int] = Counter()
    for r in voice_rows:
        tg = r.get("tgt")
        if tg is not None:
            tg_counts[tg] += 1

    # Hourly histogram (24 buckets, indexed by hour-of-day in local time).
    hourly = [0] * 24
    for r in rows_for_radio:
        ts_raw = r.get("timestamp", "")
        try:
            hourly[datetime.fromisoformat(ts_raw).hour] += 1
        except (TypeError, ValueError):
            continue

    # Position history.
    pos_rows = index.query(src=radio_id, types=["lrrp_position"], since=since, limit=10_000)
    position_history = []
    for r in pos_rows:
        lat = r.get("lat"); lon = r.get("lon")
        if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
            position_history.append({"lat": lat, "lon": lon, "at": r.get("timestamp")})

    # Recent calls — collapse into sessions, attach a recording where present.
    sessions = _group_calls(voice_rows)
    recent_calls = []
    for s in sessions[-20:]:
        recent_calls.append({
            "ts": s["ts"], "slot": s["slot"], "tgt": s["tgt"],
            "frames": s["frames"],
            # Approx call duration: ~60 ms per voice frame.
            "duration_s": round(s["frames"] * 0.06, 2),
            "recording": _attach_recording(s, recordings),
        })
    recent_calls.reverse()  # newest first

    # Co-talkers: reuse the network builder filtered to edges involving this id.
    graph = compute_talker_pairs(
        index, window_seconds=window_seconds, min_weight=1, now=now,
    )
    co_talkers = []
    for e in graph["edges"]:
        if e["src_a"] == radio_id or e["src_b"] == radio_id:
            other = e["src_b"] if e["src_a"] == radio_id else e["src_a"]
            co_talkers.append({
                "id": other,
                "weight": e["weight"],
                "kind": e["kind"],
                "shared_tgs": e["tgs"],
            })
    co_talkers.sort(key=lambda c: c["weight"], reverse=True)
    co_talkers = co_talkers[:10]

    # Lifetime bounds and totals — single passes over the in-memory list.
    all_ts = [r.get("timestamp") for r in rows_for_radio if r.get("timestamp")]
    first_seen = min(all_ts) if all_ts else None
    last_seen = max(all_ts) if all_ts else None
    total_calls = len(sessions)
    # Approximate encrypted_calls: count encryption events whose slot is one
    # this radio used in the window. Not exact, but a useful signal.
    used_slots = {s["slot"] for s in sessions if s.get("slot") is not None}
    encrypted_calls = 0
    if used_slots:
        for r in index.query(since=since, types=["encryption"], limit=10_000_000):
            if r.get("slot") in used_slots:
                encrypted_calls += 1

    return {
        "id": radio_id,
        "ip": ip,
        "first_seen": first_seen,
        "last_seen": last_seen,
        "total_calls": total_calls,
        "encrypted_calls": encrypted_calls,
        "tgs_touched": [
            {"tg": tg, "count": cnt}
            for tg, cnt in tg_counts.most_common(20)
        ],
        "top_co_talkers": co_talkers,
        "position_history": position_history,
        "recent_calls": recent_calls,
        "hourly_activity": hourly,
        "window_seconds": window_seconds,
        "generated_at": now.isoformat(),
    }
