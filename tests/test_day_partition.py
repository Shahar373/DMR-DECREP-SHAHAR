"""Phase-3 tests: day-partitioned EventLog (v0.20.0).

* lazy rollover by EVENT date (incl. midnight crossing and replay into
  past days)
* today-only priming
* day-granular retention (whole-file unlink + index DELETE)
* stream_history over a partition directory
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from backend.event_log import EventLog, _day_from_filename, stream_history
from backend.models import VoiceCallEvent


def _voice(ts: datetime, src: int = 101, tgt: int = 9) -> VoiceCallEvent:
    return VoiceCallEvent(timestamp=ts, raw_line="v", slot=1, src=src, tgt=tgt)


def _mklog(tmp_path: Path, **kw) -> EventLog:
    return EventLog(
        jsonl_path=tmp_path / "events.jsonl", capacity=100,
        partition=True, **kw,
    )


def test_partition_layout_and_lazy_open(tmp_path):
    log = _mklog(tmp_path, enable_index=False)
    try:
        # No file until the first event.
        assert log.partition_dir == tmp_path / "events"
        assert list(log.partition_dir.glob("*.jsonl")) == []
        log.append(_voice(datetime(2026, 7, 10, 23, 0, 0)))
        day_file = tmp_path / "events" / "events-2026-07-10.jsonl"
        assert day_file.exists()
        assert day_file.read_text().count("\n") == 1
    finally:
        log.close()


def test_rollover_across_midnight_by_event_date(tmp_path):
    log = _mklog(tmp_path, enable_index=False)
    try:
        log.append(_voice(datetime(2026, 7, 10, 23, 59, 59)))
        log.append(_voice(datetime(2026, 7, 11, 0, 0, 1)))
        log.append(_voice(datetime(2026, 7, 11, 0, 0, 2)))
        d1 = tmp_path / "events" / "events-2026-07-10.jsonl"
        d2 = tmp_path / "events" / "events-2026-07-11.jsonl"
        assert d1.read_text().count("\n") == 1
        assert d2.read_text().count("\n") == 2
    finally:
        log.close()


def test_replay_into_past_day_reopens_old_file(tmp_path):
    log = _mklog(tmp_path, enable_index=False)
    try:
        log.append(_voice(datetime(2026, 7, 11, 10, 0, 0)))
        # A replayed capture from last month lands in ITS day, not today's.
        log.append(_voice(datetime(2026, 6, 1, 12, 0, 0)))
        log.append(_voice(datetime(2026, 7, 11, 10, 0, 5)))
        today = (tmp_path / "events" / "events-2026-07-11.jsonl").read_text()
        past = (tmp_path / "events" / "events-2026-06-01.jsonl").read_text()
        assert today.count("\n") == 2
        assert past.count("\n") == 1
    finally:
        log.close()


def test_prime_reads_today_only(tmp_path):
    log = _mklog(tmp_path, enable_index=False)
    now = datetime.now()
    yesterday = now - timedelta(days=1)
    log.append(_voice(yesterday))
    log.append(_voice(now, src=555))
    log.close()

    log2 = _mklog(tmp_path, enable_index=False)
    try:
        primed = log2.prime_from_jsonl()
        assert primed == 1  # only today's event
        assert log2.recent()[0].src == 555
    finally:
        log2.close()


def test_history_path_points_at_partition_dir(tmp_path):
    log = _mklog(tmp_path, enable_index=False)
    try:
        assert log.history_path == tmp_path / "events"
    finally:
        log.close()
    single = EventLog(jsonl_path=tmp_path / "single.jsonl", capacity=10,
                      enable_index=False)
    try:
        assert single.history_path == tmp_path / "single.jsonl"
    finally:
        single.close()


def test_stream_history_over_partition_dir_in_order(tmp_path):
    log = _mklog(tmp_path, enable_index=False)
    log.append(_voice(datetime(2026, 7, 9, 8, 0, 0), src=1))
    log.append(_voice(datetime(2026, 7, 10, 8, 0, 0), src=2))
    log.append(_voice(datetime(2026, 7, 11, 8, 0, 0), src=3))
    log.close()
    out = list(stream_history(tmp_path / "events"))
    assert [o["src"] for o in out] == [1, 2, 3]
    # Day-range filters skip whole files.
    out = list(stream_history(
        tmp_path / "events", day_from="2026-07-10", day_to="2026-07-10",
    ))
    assert [o["src"] for o in out] == [2]


def test_day_retention_unlinks_whole_files_and_prunes_index(tmp_path):
    log = _mklog(tmp_path)
    try:
        log.append(_voice(datetime(2026, 7, 8, 8, 0, 0)))
        log.append(_voice(datetime(2026, 7, 9, 8, 0, 0)))
        log.append(_voice(datetime(2026, 7, 11, 8, 0, 0)))
        log.index.flush()
        metrics = log.prune_days_older_than("2026-07-10")
        assert metrics["files_deleted"] == 2
        assert metrics["db_deleted"] == 2
        remaining = sorted(p.name for p in (tmp_path / "events").glob("*.jsonl"))
        assert remaining == ["events-2026-07-11.jsonl"]
        assert log.index.count() == 1
        # The current (open) day file is never deleted even if somehow old.
        metrics2 = log.prune_days_older_than("2099-01-01")
        assert (tmp_path / "events" / "events-2026-07-11.jsonl").exists()
        assert metrics2["files_deleted"] == 0
    finally:
        log.close()


def test_clear_removes_all_day_files_and_writer_recovers(tmp_path):
    log = _mklog(tmp_path, enable_index=False)
    try:
        log.append(_voice(datetime(2026, 7, 10, 8, 0, 0)))
        log.append(_voice(datetime(2026, 7, 11, 8, 0, 0)))
        log.clear()
        assert list((tmp_path / "events").glob("*.jsonl")) == []
        # Writer reopens lazily and keeps working after clear.
        log.append(_voice(datetime(2026, 7, 11, 9, 0, 0)))
        assert (tmp_path / "events" / "events-2026-07-11.jsonl").exists()
    finally:
        log.close()


def test_day_from_filename():
    assert _day_from_filename(Path("events-2026-07-11.jsonl")) == "2026-07-11"
    assert _day_from_filename(Path("events-unknown-date.jsonl")) is None
    assert _day_from_filename(Path("x.jsonl")) is None
