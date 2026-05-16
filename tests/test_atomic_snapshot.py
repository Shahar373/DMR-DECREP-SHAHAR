"""Tests for atomic snapshot write + .bak fallback in StateManager."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from backend.models import QualityEvent, VoiceCallEvent
from backend.state import StateManager, atomic_write_text


def _ts(seconds: int = 0) -> datetime:
    return datetime(2026, 5, 10, 21, 0, seconds)


def test_atomic_write_text_creates_file_with_content(tmp_path: Path) -> None:
    p = tmp_path / "snap.json"
    atomic_write_text(p, '{"hello": 1}')
    assert p.read_text() == '{"hello": 1}'


def test_atomic_write_text_keeps_previous_as_bak(tmp_path: Path) -> None:
    p = tmp_path / "snap.json"
    atomic_write_text(p, "first")
    assert p.read_text() == "first"
    atomic_write_text(p, "second")
    assert p.read_text() == "second"
    assert p.with_suffix(p.suffix + ".bak").read_text() == "first"


def test_atomic_write_text_no_backup_when_disabled(tmp_path: Path) -> None:
    p = tmp_path / "snap.json"
    atomic_write_text(p, "first", keep_backup=False)
    atomic_write_text(p, "second", keep_backup=False)
    assert p.read_text() == "second"
    assert not p.with_suffix(p.suffix + ".bak").exists()


def test_load_snapshot_falls_back_to_bak_when_main_corrupt(tmp_path: Path) -> None:
    """Power-yank mid-write leaves snapshot.json truncated. load_snapshot
    should silently fall back to snapshot.json.bak (kept by the previous
    atomic_write_text)."""
    state = StateManager()
    state.apply(QualityEvent(timestamp=_ts(0), raw_line="q", error_type="CSBK_CRC"))
    state.apply(VoiceCallEvent(timestamp=_ts(1), raw_line="v", slot=1, src=101, tgt=9))

    snap_path = tmp_path / "snapshot.json"
    atomic_write_text(snap_path, state.snapshot().model_dump_json())
    # Second write — pretend a state evolution happened — keeps a backup.
    state.apply(QualityEvent(timestamp=_ts(2), raw_line="q", error_type="SLCO_CRC"))
    atomic_write_text(snap_path, state.snapshot().model_dump_json())
    # Corrupt the main file (simulate truncated write).
    snap_path.write_text("{not json")

    restored = StateManager()
    assert restored.load_snapshot(snap_path) is True
    # The .bak only has the first state (1 CSBK_CRC, no SLCO_CRC).
    assert restored.quality.csbk_crc == 1
    assert restored.quality.slco_crc == 0
    assert 101 in restored.radios


def test_load_snapshot_returns_false_when_both_files_bad(tmp_path: Path) -> None:
    snap_path = tmp_path / "snapshot.json"
    snap_path.write_text("{garbage")
    snap_path.with_suffix(".json.bak").write_text("also garbage")
    state = StateManager()
    assert state.load_snapshot(snap_path) is False
