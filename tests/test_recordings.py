"""Tests for the filesystem-based RecordingRegistry.

The registry scans a directory for per-call WAV files written by dsd-fme
(via ``-7 <dir> -P``). We fake those WAVs by writing minimal valid PCM
headers + a tiny payload, then verify duration parsing, filename parsing,
filtering, and ordering.
"""
from __future__ import annotations

import os
import struct
import time
from pathlib import Path

from backend.recordings import RecordingRegistry, _parse_filename, _read_wav_duration


def _write_wav(path: Path, duration_sec: float, sample_rate: int = 8000) -> None:
    """Write a minimal valid PCM-WAV at the given duration."""
    n_samples = int(duration_sec * sample_rate)
    byte_rate = sample_rate * 2  # 16-bit mono
    data_size = n_samples * 2
    header = b"RIFF"
    header += struct.pack("<I", 36 + data_size)
    header += b"WAVE"
    header += b"fmt "
    header += struct.pack("<I", 16)
    header += struct.pack("<HHIIHH", 1, 1, sample_rate, byte_rate, 2, 16)
    header += b"data"
    header += struct.pack("<I", data_size)
    payload = b"\x00\x00" * n_samples
    path.write_bytes(header + payload)


def _age_file(path: Path, seconds: float) -> None:
    """Move mtime back so the file isn't filtered as 'still being written'."""
    t = time.time() - seconds
    os.utime(path, (t, t))


# ── Filename parsing ─────────────────────────────────────────────────

def test_parse_filename_extracts_tg_src_slot():
    tg, src, slot = _parse_filename("2024-01-15_14-30-25_DMR_TG_64250_SRC_2102_slot_1.wav")
    assert tg == 64250
    assert src == 2102
    assert slot == 1


def test_parse_filename_returns_none_when_absent():
    tg, src, slot = _parse_filename("random_filename.wav")
    assert tg is None and src is None and slot is None


def test_parse_filename_handles_variant_separators():
    tg, src, _ = _parse_filename("DMR_TGT123_RID456.wav")
    assert tg == 123
    assert src == 456


# ── WAV header parsing ───────────────────────────────────────────────

def test_read_wav_duration_parses_standard_header(tmp_path: Path):
    p = tmp_path / "x.wav"
    _write_wav(p, duration_sec=1.5)
    d = _read_wav_duration(p)
    assert abs(d - 1.5) < 0.01


def test_read_wav_duration_returns_zero_for_non_wav(tmp_path: Path):
    p = tmp_path / "not_a_wav.wav"
    p.write_bytes(b"garbage data")
    assert _read_wav_duration(p) == 0.0


def test_read_wav_duration_falls_back_to_filesize_when_data_size_unset(tmp_path: Path):
    """If a writer leaves data_size=0 (streaming WAV), use file size."""
    p = tmp_path / "streaming.wav"
    header = b"RIFF" + struct.pack("<I", 0) + b"WAVE"
    header += b"fmt " + struct.pack("<I", 16)
    header += struct.pack("<HHIIHH", 1, 1, 8000, 16000, 2, 16)
    header += b"data" + struct.pack("<I", 0)
    p.write_bytes(header + b"\x00" * 16000)
    d = _read_wav_duration(p)
    assert abs(d - 1.0) < 0.05


# ── Registry behaviour ───────────────────────────────────────────────

def test_list_recent_returns_empty_when_dir_is_empty(tmp_path: Path):
    r = RecordingRegistry(tmp_path)
    assert r.list_recent() == []


def test_list_recent_returns_wavs_meeting_min_duration(tmp_path: Path):
    p = tmp_path / "DMR_TG_1_SRC_2.wav"
    _write_wav(p, duration_sec=1.0)
    _age_file(p, seconds=10)
    r = RecordingRegistry(tmp_path, min_duration=0.2)
    out = r.list_recent()
    assert len(out) == 1
    assert out[0].filename == p.name
    assert out[0].tgt == 1
    assert out[0].src == 2
    assert abs(out[0].duration_seconds - 1.0) < 0.05


def test_list_recent_filters_short_recordings(tmp_path: Path):
    short = tmp_path / "short.wav"
    long_ = tmp_path / "long.wav"
    _write_wav(short, duration_sec=0.1)
    _write_wav(long_, duration_sec=0.5)
    _age_file(short, seconds=10)
    _age_file(long_, seconds=10)
    r = RecordingRegistry(tmp_path, min_duration=0.2)
    out = r.list_recent()
    assert {x.filename for x in out} == {"long.wav"}


def test_list_recent_skips_files_still_being_written(tmp_path: Path):
    """Recent mtime means dsd-fme might still be writing — skip until settled."""
    p = tmp_path / "fresh.wav"
    _write_wav(p, duration_sec=1.0)
    # Don't age it: mtime is now.
    r = RecordingRegistry(tmp_path, min_duration=0.2, ignore_recent_seconds=1.5)
    assert r.list_recent() == []


def test_list_recent_ignores_non_wav_files(tmp_path: Path):
    (tmp_path / "note.txt").write_text("hi")
    (tmp_path / "garbage.bin").write_bytes(b"\x00")
    p = tmp_path / "real.wav"
    _write_wav(p, duration_sec=1.0)
    _age_file(p, seconds=10)
    r = RecordingRegistry(tmp_path, min_duration=0.2)
    out = r.list_recent()
    assert [x.filename for x in out] == ["real.wav"]


def test_list_recent_sorts_newest_first(tmp_path: Path):
    older = tmp_path / "older.wav"
    newer = tmp_path / "newer.wav"
    _write_wav(older, duration_sec=0.5)
    _write_wav(newer, duration_sec=0.5)
    _age_file(older, seconds=60)
    _age_file(newer, seconds=10)
    r = RecordingRegistry(tmp_path, min_duration=0.2)
    out = r.list_recent()
    assert [x.filename for x in out] == ["newer.wav", "older.wav"]


def test_file_path_prevents_path_traversal(tmp_path: Path):
    r = RecordingRegistry(tmp_path)
    p = r.file_path("../../etc/passwd")
    assert p.parent == tmp_path
    assert p.name == "passwd"


# ── Retention ────────────────────────────────────────────────────────

def test_prune_older_than_deletes_old_files_only(tmp_path: Path):
    old = tmp_path / "old.wav"
    young = tmp_path / "young.wav"
    _write_wav(old, duration_sec=0.5)
    _write_wav(young, duration_sec=0.5)
    _age_file(old, seconds=4 * 3600)    # 4h old
    _age_file(young, seconds=10 * 60)   # 10 min old
    r = RecordingRegistry(tmp_path, min_duration=0.2)
    deleted, freed = r.prune_older_than(hours=1.0)
    assert deleted == 1
    assert freed > 0
    assert not old.exists()
    assert young.exists()


def test_prune_older_than_skips_non_wav_files(tmp_path: Path):
    note = tmp_path / "notes.txt"
    note.write_text("important", encoding="utf-8")
    _age_file(note, seconds=10 * 3600)
    r = RecordingRegistry(tmp_path)
    deleted, freed = r.prune_older_than(hours=1.0)
    assert deleted == 0
    assert note.exists()


def test_prune_older_than_zero_hours_is_noop(tmp_path: Path):
    old = tmp_path / "old.wav"
    _write_wav(old, duration_sec=0.5)
    _age_file(old, seconds=10 * 3600)
    r = RecordingRegistry(tmp_path)
    deleted, _ = r.prune_older_than(hours=0)
    assert deleted == 0
    assert old.exists()
