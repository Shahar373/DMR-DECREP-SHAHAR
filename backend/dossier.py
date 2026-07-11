"""Per-radio Dossier — lifetime stats, co-talkers, calls, positions.

Reads from the SQLite index (v0.9.0) and optionally augments with the
``RecordingRegistry`` so each recent call links to its playable WAV.

Used by ``GET /api/radio/{radio_id}``.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta
from typing import Optional

from .event_index import EventIndex
from .network import compute_talker_pairs


# Two consecutive voice_call frames within this many seconds are treated as
# part of the same call (PTT held). dsd-fme emits one frame ~every 60 ms; a
# 2-second gap reliably separates distinct keyups.
_CALL_GAP_SECONDS = 2.0

# Session grouping needs ordered voice rows; only the most recent 20
# sessions surface in the dossier, so we bound the pull to the newest N
# frames instead of materialising every voice row in the window (which
# could be hundreds of thousands on a busy 24 h+ net). 5000 frames ≈ 5
# minutes of continuous PTT — far more than 20 sessions' worth.
_RECENT_VOICE_ROWS = 5_000


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


def _attach_recording(call: dict, recordings_list) -> Optional[dict]:
    """Find a CallRecording within ±5s of the call start. Best-effort.

    ``recordings_list`` is a pre-fetched list (one ``list_recent()`` scan
    re-used across all sessions in a dossier — re-scanning per session is
    O(N*M) on a busy day's worth of WAVs).
    """
    if not recordings_list:
        return None
    try:
        call_ts = datetime.fromisoformat(call["ts"])
    except (TypeError, ValueError):
        return None
    for rec in recordings_list:
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

    # Recent voice frames only (newest _RECENT_VOICE_ROWS, restored to
    # chronological order for session grouping). Previously this pulled up
    # to 500k full payload rows per side into Python; every aggregate that
    # used those rows now runs as a GROUP BY inside SQLite instead.
    voice_rows = index.query(
        src=radio_id, types=["voice_call"], since=since,
        limit=_RECENT_VOICE_ROWS, descending=True,
    )
    voice_rows.reverse()

    tg_counts = Counter(index.count_by_tgt(
        radio_id, since=since, types=["voice_call"],
    ))

    # Hourly histogram over src-side + tgt-side rows, grouped in SQL.
    hourly = index.hourly_histogram(radio_id, since=since)

    # Position history.
    pos_rows = index.query(src=radio_id, types=["lrrp_position"], since=since, limit=10_000)
    position_history = []
    for r in pos_rows:
        lat = r.get("lat"); lon = r.get("lon")
        if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
            position_history.append({"lat": lat, "lon": lon, "at": r.get("timestamp")})

    # Recent calls — collapse into sessions, attach a recording where present.
    sessions = _group_calls(voice_rows)
    # Snapshot the recordings list once and re-use across all sessions: each
    # ``list_recent()`` rescans the dir and parses every WAV header.
    recordings_list = None
    if recordings is not None:
        try:
            recordings_list = recordings.list_recent()
        except Exception:  # noqa: BLE001 — recording lookup must never break dossier
            recordings_list = None
    recent_calls = []
    for s in sessions[-20:]:
        recent_calls.append({
            "ts": s["ts"], "slot": s["slot"], "tgt": s["tgt"],
            "frames": s["frames"],
            # Approx call duration: ~60 ms per voice frame.
            "duration_s": round(s["frames"] * 0.06, 2),
            "recording": _attach_recording(s, recordings_list),
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

    # Lifetime bounds via MIN/MAX in SQL (src-side and tgt-side merged).
    first_seen, last_seen = index.radio_bounds(radio_id, since=since)
    total_calls = len(sessions)
    # Approximate encrypted_calls: count encryption events whose slot is one
    # this radio used in the window. Not exact, but a useful signal.
    used_slots = {s["slot"] for s in sessions if s.get("slot") is not None}
    encrypted_calls = 0
    if used_slots:
        enc_by_slot = index.count_encryption_by_slot(since=since)
        encrypted_calls = sum(
            c for slot, c in enc_by_slot.items() if slot in used_slots
        )

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
        # Session stats (total_calls, recent_calls) are derived from the
        # newest N voice frames, not the entire window — documented so UI
        # and API consumers know the bound.
        "recent_calls_window_rows": _RECENT_VOICE_ROWS,
        "generated_at": now.isoformat(),
    }
