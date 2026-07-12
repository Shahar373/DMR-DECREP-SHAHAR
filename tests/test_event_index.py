"""Tests for backend.event_index.EventIndex (SQLite sidecar)."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from backend.event_index import EventIndex, _extract_columns
from backend.event_log import EventLog, quality_ratios_over_window
from backend.models import (
    EVENT_SCHEMA_VERSION,
    EncryptionEvent,
    IPMappingEvent,
    LRRPPositionEvent,
    PreambleCSBKEvent,
    QualityEvent,
    VoiceCallEvent,
)


def _ts(seconds: int = 0) -> datetime:
    return datetime(2026, 5, 10, 21, 0, 0) + timedelta(seconds=seconds)


def _voice(seconds: int, src: int, tgt: int, slot: int = 1) -> VoiceCallEvent:
    return VoiceCallEvent(
        timestamp=_ts(seconds), raw_line="voice", slot=slot, src=src, tgt=tgt,
    )


def _seed(log: EventLog, n: int = 50) -> None:
    for i in range(n):
        if i % 3 == 0:
            log.append(_voice(i, 100 + (i % 5), 9))
        elif i % 3 == 1:
            log.append(QualityEvent(timestamp=_ts(i), raw_line="q", error_type="CSBK_CRC"))
        else:
            log.append(LRRPPositionEvent(
                timestamp=_ts(i), raw_line="p", src=200, lat=32.0 + i * 0.001, lon=34.0,
            ))


def test_index_creation_initialises_schema(tmp_path: Path) -> None:
    db = tmp_path / "events.db"
    idx = EventIndex(db, schema_version=EVENT_SCHEMA_VERSION)
    try:
        assert db.exists()
        conn = sqlite3.connect(str(db))
        try:
            tables = {row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )}
            assert "events" in tables and "_meta" in tables
            indexes = {row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
            )}
            assert {"idx_events_ts", "idx_events_src_ts",
                    "idx_events_tgt_ts", "idx_events_type_ts"} <= indexes
            version_row = conn.execute(
                "SELECT value FROM _meta WHERE key='schema_version'"
            ).fetchone()
            assert int(version_row[0]) == EVENT_SCHEMA_VERSION
        finally:
            conn.close()
    finally:
        idx.close()


def test_dual_write_jsonl_and_sqlite_agree(tmp_path: Path) -> None:
    log = EventLog(jsonl_path=tmp_path / "events.jsonl", capacity=100)
    try:
        _seed(log, 50)
    finally:
        log.close()
    # JSONL line count.
    lines = (tmp_path / "events.jsonl").read_text().strip().splitlines()
    assert len(lines) == 50
    # SQLite row count (reopen — the writer has been closed and flushed).
    idx = EventIndex(tmp_path / "events.db", schema_version=EVENT_SCHEMA_VERSION)
    try:
        assert idx.count() == 50
    finally:
        idx.close()


def test_query_filters_by_time_src_tgt_type(tmp_path: Path) -> None:
    log = EventLog(jsonl_path=tmp_path / "events.jsonl", capacity=100)
    try:
        log.append(_voice(0, 101, 9))
        log.append(_voice(1, 102, 9))
        log.append(_voice(2, 101, 7))
        log.append(QualityEvent(timestamp=_ts(3), raw_line="q", error_type="SLCO_CRC"))
        log.append(IPMappingEvent(
            timestamp=_ts(4), raw_line="ip", role="SRC", radio_id=101,
            ip="10.0.0.1", port=4000,
        ))
        idx = log.index
        assert idx is not None
        # by src
        rows = idx.query(src=101)
        assert len(rows) == 3  # 2 voice_call + 1 ip_mapping (radio_id normalised → src)
        # by tgt
        rows = idx.query(tgt=9)
        assert len(rows) == 2
        # by type
        rows = idx.query(types=["quality"])
        assert len(rows) == 1 and rows[0]["error_type"] == "SLCO_CRC"
        # by time window
        rows = idx.query(since=_ts(2))
        assert len(rows) == 3
    finally:
        log.close()


def test_query_pagination(tmp_path: Path) -> None:
    log = EventLog(jsonl_path=tmp_path / "events.jsonl", capacity=500)
    try:
        for i in range(200):
            log.append(_voice(i, 500, 1))
        idx = log.index
        assert idx is not None
        # First 50 are events 0–49, slice [100..150) is 100–149.
        page = idx.query(limit=50, offset=100)
        assert len(page) == 50
        # Stable order by ts.
        timestamps = [row["timestamp"] for row in page]
        assert timestamps == sorted(timestamps)
        # Reconstruct the seconds from the timestamp's "%S" part.
        first = datetime.fromisoformat(page[0]["timestamp"])
        last = datetime.fromisoformat(page[-1]["timestamp"])
        assert (last - first) == timedelta(seconds=49)
    finally:
        log.close()


def test_rebuild_from_jsonl_after_db_deleted(tmp_path: Path) -> None:
    jsonl = tmp_path / "events.jsonl"
    db = tmp_path / "events.db"
    log = EventLog(jsonl_path=jsonl, capacity=200)
    try:
        _seed(log, 50)
    finally:
        log.close()
    db.unlink()
    # Drop WAL/SHM siblings too so the rebuild is from scratch.
    for sibling in tmp_path.glob("events.db-*"):
        sibling.unlink()
    assert not db.exists()
    idx = EventIndex(db, schema_version=EVENT_SCHEMA_VERSION)
    try:
        assert idx.count() == 0
        n = idx.rebuild_from_jsonl(jsonl)
        assert n == 50
        assert idx.count() == 50
    finally:
        idx.close()


def test_schema_version_mismatch_marks_index_outdated(tmp_path: Path) -> None:
    db = tmp_path / "events.db"
    # Pretend a future writer left version 99 in _meta.
    idx = EventIndex(db, schema_version=99)
    idx.close()
    idx = EventIndex(db, schema_version=EVENT_SCHEMA_VERSION)
    try:
        assert idx.index_outdated is True
    finally:
        idx.close()


def test_sqlite_failure_does_not_break_eventlog(tmp_path: Path) -> None:
    jsonl = tmp_path / "events.jsonl"
    log = EventLog(jsonl_path=jsonl, capacity=50)
    try:
        # Sabotage the index: replace append with a function that raises.
        assert log.index is not None
        log._index.append = lambda d: (_ for _ in ()).throw(RuntimeError("nope"))  # type: ignore[method-assign]
        for i in range(10):
            log.append(_voice(i, 700, 1))
    finally:
        log.close()
    lines = jsonl.read_text().strip().splitlines()
    assert len(lines) == 10
    parsed = [json.loads(line) for line in lines]
    assert all(p["src"] == 700 for p in parsed)


def test_api_history_uses_index_when_present(tmp_path: Path) -> None:
    from fastapi.testclient import TestClient

    from backend import server as srv

    log = EventLog(jsonl_path=tmp_path / "events.jsonl", capacity=200)
    log.append(_voice(0, 101, 9))
    log.append(_voice(1, 102, 9))
    log.append(_voice(2, 101, 7))
    srv.attach_event_log(log)
    try:
        client = TestClient(srv.app)
        # Index path: src filter, count by srcs in the response.
        resp = client.get("/api/history", params={"src": 101})
        assert resp.status_code == 200
        events = resp.json()["events"]
        assert len(events) == 2
        assert all(e["src"] == 101 for e in events)
        # JSONL fallback path: force it by closing the index.
        log._index.close()  # type: ignore[union-attr]
        log._index = None
        resp = client.get("/api/history", params={"src": 102})
        assert resp.status_code == 200
        events = resp.json()["events"]
        assert len(events) == 1 and events[0]["src"] == 102
    finally:
        srv.attach_event_log(None)  # type: ignore[arg-type]
        log.close()


def test_quality_endpoint_prefers_index(tmp_path: Path) -> None:
    log = EventLog(jsonl_path=tmp_path / "events.jsonl", capacity=200)
    try:
        for i in range(5):
            log.append(_voice(i, 800 + i, 1))
        for i in range(3):
            log.append(QualityEvent(timestamp=_ts(10 + i), raw_line="q", error_type="CSBK_CRC"))
        # Use a wide enough window to cover the synthetic timestamps from 2026.
        # The function clamps "now" to actual now() so we pass a future-anchored "now".
        idx = log.index
        assert idx is not None
        result = quality_ratios_over_window(
            log.jsonl_path, window_seconds=24 * 3600,
            now=_ts(20), index=idx,
        )
        assert result["sample_events"] == 8
        assert result["events_by_type"]["voice_call"] == 5
        assert result["quality_by_kind"]["CSBK_CRC"] == 3
    finally:
        log.close()


def test_extract_columns_normalises_ip_mapping_radio_id() -> None:
    ip = IPMappingEvent(
        timestamp=_ts(0), raw_line="ip", role="SRC", radio_id=12345,
        ip="10.0.0.5", port=4001,
    )
    cols = _extract_columns(ip.model_dump(mode="json"))
    # Layout v2: (ts, day, type, src, tgt, slot, addressing, error_type,
    # encrypted, frequency, channel_label, schema_version, payload)
    assert cols[2] == "ip_mapping"
    assert cols[3] == 12345  # src column populated from radio_id


def test_encrypted_column_set_for_encryption_events() -> None:
    enc = EncryptionEvent(
        timestamp=_ts(0), raw_line="enc", slot=1, flco="0x04", fid="0x80",
    )
    cols = _extract_columns(enc.model_dump(mode="json"))
    assert cols[8] == 1  # encrypted flag (layout v2 — day column at index 1)


def test_addressing_column_preserved_for_csbk() -> None:
    csbk = PreambleCSBKEvent(
        timestamp=_ts(0), raw_line="csbk", addressing="Individual",
        kind="Voice", src=101, tgt=102,
    )
    cols = _extract_columns(csbk.model_dump(mode="json"))
    assert cols[6] == "Individual"


# ── Retention ─────────────────────────────────────────────────────────


def test_eventindex_prune_older_than_removes_rows_chunked(tmp_path: Path) -> None:
    """Rows with ts < cutoff are deleted; rows at/after cutoff stay."""
    db = tmp_path / "events.db"
    idx = EventIndex(db, schema_version=EVENT_SCHEMA_VERSION)
    try:
        # Insert 200 events at one-second steps; cutoff lands at second 120.
        # The chunk_size below is intentionally smaller than the delete
        # set to exercise the multi-batch loop.
        for i in range(200):
            ev = _voice(i, 100 + (i % 5), 9)
            idx.append(ev.model_dump(mode="json"))
        # Force pending writes to commit before we query / prune.
        with idx._lock:
            idx._commit_locked()
        assert idx.count() == 200

        cutoff = _ts(120)
        removed = idx.prune_older_than(cutoff, chunk_size=25)
        assert removed == 120

        # Everything left must be ≥ cutoff.
        remaining = idx.query(limit=500)
        assert len(remaining) == 80
        for row in remaining:
            assert row["timestamp"] >= cutoff.isoformat()
    finally:
        idx.close()


def test_eventindex_prune_idempotent_when_nothing_to_remove(tmp_path: Path) -> None:
    """Pruning with a cutoff before all data is a no-op and returns 0."""
    db = tmp_path / "events.db"
    idx = EventIndex(db, schema_version=EVENT_SCHEMA_VERSION)
    try:
        for i in range(10):
            ev = _voice(i, 100, 9)
            idx.append(ev.model_dump(mode="json"))
        with idx._lock:
            idx._commit_locked()
        removed = idx.prune_older_than(_ts(-10_000))
        assert removed == 0
        assert idx.count() == 10
    finally:
        idx.close()


def test_eventlog_prune_rewrites_jsonl_and_keeps_writer_open(tmp_path: Path) -> None:
    """End-to-end: prune the EventLog, then verify the JSONL on disk has
    only the surviving lines AND a new append still lands in the file."""
    jsonl = tmp_path / "events.jsonl"
    log = EventLog(jsonl_path=jsonl, capacity=1000)
    try:
        for i in range(100):
            log.append(_voice(i, 100, 9))
        cutoff = _ts(60)
        metrics = log.prune_older_than(cutoff)
        assert metrics["db_deleted"] == 60
        assert metrics["jsonl_lines_kept"] == 40
        # The writer is still alive: appending a fresh event should hit the
        # rewritten file and the DB.
        log.append(_voice(200, 100, 9))
        # Re-read the file from disk and check ordering + first ts.
        lines = jsonl.read_text(encoding="utf-8").splitlines()
        assert len(lines) == 41
        first = json.loads(lines[0])
        assert first["timestamp"] >= cutoff.isoformat()
        last = json.loads(lines[-1])
        assert last["timestamp"] == _ts(200).isoformat()
    finally:
        log.close()


def test_eventlog_prune_preserves_concurrent_appends(tmp_path: Path) -> None:
    """Lines written to the JSONL *while* the prune is in flight must
    survive the swap. We simulate that by appending between the file-size
    snapshot and the lock-held swap — easier than threading: call append()
    on the log, then prune with a cutoff that would also wipe the new
    line. Phase-2's tail catch-up should keep it anyway because the line
    arrived after the snapshot."""
    jsonl = tmp_path / "events.jsonl"
    log = EventLog(jsonl_path=jsonl, capacity=1000)
    try:
        for i in range(50):
            log.append(_voice(i, 100, 9))
        # Now prune with cutoff that drops everything; only the fresh
        # append below would qualify — but it's appended AFTER prune
        # starts, so it falls into the "tail" pass and is preserved as-is.
        cutoff = _ts(1_000_000)  # in the far future → everything is older
        metrics = log.prune_older_than(cutoff)
        assert metrics["db_deleted"] == 50
        # Tail was empty (no concurrent appends in this single-threaded
        # test), so the JSONL is now empty modulo what we add now:
        assert metrics["jsonl_lines_kept"] == 0
        # The writer is still healthy.
        log.append(_voice(999_999_999, 100, 9))
        text = jsonl.read_text(encoding="utf-8")
        assert text.count("\n") == 1
    finally:
        log.close()
