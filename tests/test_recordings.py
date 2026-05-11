"""Tests for backend.recordings.RecordingRegistry.

Exercises the per-call MP3 metadata registry that backs the dashboard's
"recent calls" panel. The registry only tracks state + file lifecycle —
actual MP3 bytes are written by AudioBroadcaster, so we simulate that by
touching files in `base_dir` with `tmp_path`.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from backend.recordings import RecordingRegistry


class _FakeCall:
    """Minimal stand-in for ActiveCall — only attributes the registry reads."""

    def __init__(
        self,
        src: int,
        tgt: int,
        slot: int,
        started_at: datetime,
        is_cap_plus: bool = False,
        is_encrypted: bool = False,
    ) -> None:
        self.src = src
        self.tgt = tgt
        self.slot = slot
        self.started_at = started_at
        self.is_cap_plus = is_cap_plus
        self.is_encrypted = is_encrypted


def _ts(s: int = 0) -> datetime:
    return datetime(2026, 5, 10, 21, 0, s)


# ===========================================================================
# Basic lifecycle
# ===========================================================================


def test_start_end_list_recent_cycle(tmp_path: Path):
    reg = RecordingRegistry(tmp_path)
    call = _FakeCall(src=2102, tgt=1, slot=2, started_at=_ts(1))

    rec = reg.start(call)
    assert rec.src == 2102 and rec.tgt == 1 and rec.slot == 2
    assert rec.ended_at is None
    assert reg.active_id(2) == rec.id

    # While the call is live, it appears in list_recent with no ended_at.
    live_list = reg.list_recent()
    assert len(live_list) == 1
    assert live_list[0].id == rec.id and live_list[0].ended_at is None

    ended = reg.end(2, _ts(5))
    assert ended is not None
    assert ended.id == rec.id
    assert ended.ended_at == _ts(5)
    assert ended.duration_seconds == 4.0
    assert reg.active_id(2) is None


def test_list_recent_returns_newest_first(tmp_path: Path):
    reg = RecordingRegistry(tmp_path)
    r1 = reg.start(_FakeCall(1, 10, 1, _ts(1)))
    reg.end(1, _ts(2))
    r2 = reg.start(_FakeCall(2, 20, 1, _ts(3)))
    reg.end(1, _ts(4))
    r3 = reg.start(_FakeCall(3, 30, 2, _ts(5)))

    ids = [r.id for r in reg.list_recent()]
    assert ids == [r3.id, r2.id, r1.id]


def test_active_id_returns_none_for_unknown_slot(tmp_path: Path):
    reg = RecordingRegistry(tmp_path)
    assert reg.active_id(1) is None
    reg.start(_FakeCall(1, 10, 2, _ts(1)))
    assert reg.active_id(1) is None
    assert reg.active_id(2) is not None


# ===========================================================================
# end() edge cases
# ===========================================================================


def test_end_without_matching_start_returns_none(tmp_path: Path):
    reg = RecordingRegistry(tmp_path)
    assert reg.end(2, _ts(5)) is None


def test_end_on_wrong_slot_is_noop(tmp_path: Path):
    reg = RecordingRegistry(tmp_path)
    rec = reg.start(_FakeCall(1, 10, 2, _ts(1)))
    # End on slot 1 (the call is on slot 2) — should not touch the slot-2 rec.
    assert reg.end(1, _ts(5)) is None
    assert reg.active_id(2) == rec.id
    fresh = reg.get(rec.id)
    assert fresh is not None and fresh.ended_at is None


# ===========================================================================
# File bytes captured
# ===========================================================================


def test_file_bytes_captured_when_file_exists(tmp_path: Path):
    reg = RecordingRegistry(tmp_path)
    rec = reg.start(_FakeCall(1, 10, 2, _ts(1)))
    # Simulate the broadcaster having written some MP3 bytes for this call.
    reg.file_path(rec.id).write_bytes(b"\xff\xfb" * 100)
    ended = reg.end(2, _ts(2))
    assert ended is not None
    assert ended.file_bytes == 200


def test_file_bytes_zero_when_file_missing(tmp_path: Path):
    reg = RecordingRegistry(tmp_path)
    reg.start(_FakeCall(1, 10, 2, _ts(1)))
    # No file ever created — end() should still succeed with file_bytes=0.
    ended = reg.end(2, _ts(2))
    assert ended is not None
    assert ended.file_bytes == 0


# ===========================================================================
# Pruning
# ===========================================================================


def test_pruning_keeps_only_max_keep(tmp_path: Path):
    reg = RecordingRegistry(tmp_path, max_keep=3)
    for i in range(5):
        rec = reg.start(_FakeCall(i, 10, 1, _ts(i)))
        reg.file_path(rec.id).write_bytes(b"x")
        reg.end(1, _ts(i) + timedelta(milliseconds=500))

    assert len(reg.list_recent()) == 3


def test_pruning_deletes_old_files_from_disk(tmp_path: Path):
    reg = RecordingRegistry(tmp_path, max_keep=2)
    paths: list[Path] = []
    for i in range(4):
        rec = reg.start(_FakeCall(i, 10, 1, _ts(i)))
        p = reg.file_path(rec.id)
        p.write_bytes(b"x")
        paths.append(p)
        reg.end(1, _ts(i) + timedelta(milliseconds=500))

    # First two recordings should have been pruned from both memory and disk.
    assert not paths[0].exists()
    assert not paths[1].exists()
    # Latest two should survive.
    assert paths[2].exists()
    assert paths[3].exists()
    assert len(reg.list_recent()) == 2


def test_pruning_tolerates_missing_files(tmp_path: Path):
    """If a file was already deleted, _prune must not blow up."""
    reg = RecordingRegistry(tmp_path, max_keep=1)
    r1 = reg.start(_FakeCall(1, 10, 1, _ts(1)))
    reg.end(1, _ts(2))
    # The file was never created — pruning should silently skip the unlink.
    reg.start(_FakeCall(2, 10, 1, _ts(3)))
    reg.end(1, _ts(4))
    assert reg.get(r1.id) is None
    assert len(reg.list_recent()) == 1


# ===========================================================================
# get() lookups
# ===========================================================================


def test_get_returns_none_for_unknown_id(tmp_path: Path):
    reg = RecordingRegistry(tmp_path)
    assert reg.get("deadbeef") is None


def test_get_finds_live_and_ended_recordings(tmp_path: Path):
    reg = RecordingRegistry(tmp_path)
    rec = reg.start(_FakeCall(1, 10, 2, _ts(1)))
    assert reg.get(rec.id) is not None
    reg.end(2, _ts(5))
    assert reg.get(rec.id) is not None  # ended recordings still queryable
