"""Phase-3 tests: crash-safe monolith → per-day migration (v0.20.0)."""
from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from backend.export import (
    MigrationRefused,
    day_file_for,
    migrate_monolith,
    partition_dir_for,
)
from backend.models import VoiceCallEvent


def _line(ts: datetime, src: int = 101) -> str:
    return VoiceCallEvent(
        timestamp=ts, raw_line="v", slot=1, src=src, tgt=9,
    ).model_dump_json() + "\n"


def _write_monolith(path: Path, days: int = 3, per_day: int = 5) -> int:
    base = datetime(2026, 7, 1, 12, 0, 0)
    lines = []
    for d in range(days):
        for i in range(per_day):
            lines.append(_line(base + timedelta(days=d, seconds=i)))
    path.write_text("".join(lines), encoding="utf-8")
    return len(lines)


def test_happy_path_splits_by_day_and_keeps_legacy_backup(tmp_path):
    mono = tmp_path / "events.jsonl"
    total = _write_monolith(mono, days=3, per_day=5)

    metrics = migrate_monolith(mono)
    assert metrics is not None
    assert metrics["lines_total"] == total
    assert metrics["days"] == 3
    assert metrics["lines_unknown_date"] == 0

    pd = partition_dir_for(mono)
    day_files = sorted(pd.glob("*.jsonl"))
    assert [p.name for p in day_files] == [
        "events-2026-07-01.jsonl",
        "events-2026-07-02.jsonl",
        "events-2026-07-03.jsonl",
    ]
    # Conservation: sum of day-file lines == monolith lines; the legacy
    # file survives as a .migrated backup, never deleted.
    assert sum(p.read_text().count("\n") for p in day_files) == total
    assert not mono.exists()
    assert (tmp_path / "events.jsonl.migrated").exists()
    # Temp dir cleaned up.
    assert not (pd / ".migrate-tmp").exists()


def test_unknown_date_lines_are_never_dropped(tmp_path):
    mono = tmp_path / "events.jsonl"
    good = _line(datetime(2026, 7, 1, 12, 0, 0))
    mono.write_text(
        good + "not json at all\n" + '{"type":"x","timestamp":null}\n',
        encoding="utf-8",
    )
    metrics = migrate_monolith(mono)
    assert metrics["lines_total"] == 3
    assert metrics["lines_unknown_date"] == 2
    unknown = partition_dir_for(mono) / "events-unknown-date.jsonl"
    assert unknown.read_text().count("\n") == 2


def test_nothing_to_do_returns_none(tmp_path):
    assert migrate_monolith(tmp_path / "events.jsonl") is None


def test_refuses_when_partition_dir_already_populated(tmp_path):
    mono = tmp_path / "events.jsonl"
    _write_monolith(mono, days=1)
    pd = partition_dir_for(mono)
    pd.mkdir()
    (pd / "events-2026-07-11.jsonl").write_text(
        _line(datetime(2026, 7, 11, 8, 0, 0)), encoding="utf-8",
    )
    with pytest.raises(MigrationRefused):
        migrate_monolith(mono)
    # Legacy file untouched by the refusal.
    assert mono.exists()


def test_crash_before_marker_restarts_cleanly(tmp_path):
    mono = tmp_path / "events.jsonl"
    total = _write_monolith(mono, days=2, per_day=3)
    # Simulate a crashed first attempt: stale tmp dir with partial junk,
    # NO marker.
    tmp = partition_dir_for(mono) / ".migrate-tmp"
    tmp.mkdir(parents=True)
    (tmp / "events-2026-07-01.jsonl").write_text("partial junk\n")

    metrics = migrate_monolith(mono)
    assert metrics["lines_total"] == total
    day1 = partition_dir_for(mono) / "events-2026-07-01.jsonl"
    # The partial junk was discarded — day 1 has exactly its real lines.
    assert day1.read_text().count("\n") == 3
    for raw in day1.read_text().splitlines():
        json.loads(raw)  # every kept line is valid JSON


def test_crash_after_marker_resumes_renames(tmp_path):
    mono = tmp_path / "events.jsonl"
    total = _write_monolith(mono, days=2, per_day=3)
    pd = partition_dir_for(mono)
    tmp = pd / ".migrate-tmp"
    tmp.mkdir(parents=True)
    # Simulate: bucketing finished (valid buckets + marker), crash before
    # renames.
    base = datetime(2026, 7, 1, 12, 0, 0)
    for d in range(2):
        day = (base + timedelta(days=d)).strftime("%Y-%m-%d")
        lines = "".join(
            _line(base + timedelta(days=d, seconds=i)) for i in range(3)
        )
        (tmp / f"events-{day}.jsonl").write_text(lines, encoding="utf-8")
    (tmp / "COMPLETE").write_text("ok")

    metrics = migrate_monolith(mono)
    assert metrics["resumed"] is True
    day_files = sorted(pd.glob("*.jsonl"))
    assert len(day_files) == 2
    assert sum(p.read_text().count("\n") for p in day_files) == total
    assert not mono.exists()
    assert (tmp_path / "events.jsonl.migrated").exists()
    assert not tmp.exists()


def test_migration_output_matches_day_file_naming(tmp_path):
    mono = tmp_path / "events.jsonl"
    _write_monolith(mono, days=1, per_day=2)
    migrate_monolith(mono)
    # The migrated file name must be exactly what EventLog.day_file uses,
    # so the live writer appends to the same file after migration.
    assert day_file_for(mono, "2026-07-01").exists()
