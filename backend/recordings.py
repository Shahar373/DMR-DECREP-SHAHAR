"""Per-call WAV recording registry — scans a directory for WAV files
written by dsd-fme (started with ``-7 <dir> -P``) and exposes them to the
dashboard.

dsd-fme names its per-call WAVs along the lines of::

    YYYY-MM-DD_HH-MM-SS_DMR_TG_<tg>_SRC_<src>.wav

The exact format varies between dsd-fme builds, so we extract whatever
fields we can with flexible regexes and fall back to file mtime for the
timestamp when parsing fails.
"""
from __future__ import annotations

import re
import struct
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from pydantic import BaseModel


class CallRecording(BaseModel):
    filename: str              # basename of the WAV file (used in URLs)
    src: Optional[int] = None  # source radio ID, if parseable
    tgt: Optional[int] = None  # target talkgroup, if parseable
    slot: Optional[int] = None
    started_at: datetime
    duration_seconds: float
    file_bytes: int


# Try to extract TG/SRC/slot from filename in a few different shapes.
_RE_TG = re.compile(r"(?:TG|Talkgroup|TGID|TGT)[_-]?(\d+)", re.I)
_RE_SRC = re.compile(r"(?:SRC|Source|RID)[_-]?(\d+)", re.I)
_RE_SLOT = re.compile(r"(?:slot|TS)[_-]?(\d+)", re.I)


def _parse_filename(name: str) -> tuple[Optional[int], Optional[int], Optional[int]]:
    """Return (tgt, src, slot) extracted from a dsd-fme WAV filename."""
    tg = _RE_TG.search(name)
    src = _RE_SRC.search(name)
    slot = _RE_SLOT.search(name)
    return (
        int(tg.group(1)) if tg else None,
        int(src.group(1)) if src else None,
        int(slot.group(1)) if slot else None,
    )


def _read_wav_duration(path: Path) -> float:
    """Read duration in seconds from a standard PCM WAV file header.

    Returns 0.0 if the header is missing or malformed (e.g. file still
    being written and the data-chunk size field is unset).
    """
    try:
        with open(path, "rb") as f:
            header = f.read(44)
        if len(header) < 44 or header[:4] != b"RIFF" or header[8:12] != b"WAVE":
            return 0.0
        byte_rate = struct.unpack("<I", header[28:32])[0]
        data_size = struct.unpack("<I", header[40:44])[0]
        if byte_rate == 0:
            return 0.0
        if data_size == 0:
            # Closed-while-writing or streaming WAV: fall back to file size.
            try:
                file_size = path.stat().st_size
            except OSError:
                return 0.0
            data_size = max(file_size - 44, 0)
        return data_size / byte_rate
    except Exception:
        return 0.0


class RecordingRegistry:
    """Scan ``base_dir`` for per-call WAV files written by dsd-fme."""

    def __init__(
        self,
        base_dir: Path,
        min_duration: float = 0.2,
        ignore_recent_seconds: float = 1.5,
        memo_ttl_seconds: float = 2.0,
    ) -> None:
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.min_duration = min_duration
        # Skip files whose mtime is within this many seconds — they are
        # probably still being written and the WAV header is incomplete.
        self.ignore_recent_seconds = ignore_recent_seconds
        # The dashboard polls /api/recordings every ~3 s; after weeks of
        # recordings a naive scan re-opens every WAV header each poll.
        # Two-level cache: per-file metadata keyed by (size, mtime) so a
        # header is parsed once per file version, plus a short TTL memo of
        # the whole listing so N concurrent tabs share one directory scan.
        self.memo_ttl_seconds = memo_ttl_seconds
        # name -> ((size, mtime), CallRecording | None)   (None = filtered out)
        self._meta_cache: dict[str, tuple[tuple[int, float], Optional[CallRecording]]] = {}
        self._memo: Optional[tuple[float, list[CallRecording]]] = None

    def file_path(self, filename: str) -> Path:
        """Resolve a filename against base_dir, stripping any path components."""
        safe = Path(filename).name
        return self.base_dir / safe

    def list_recent(self) -> list[CallRecording]:
        if not self.base_dir.exists():
            return []
        now = time.time()
        if (
            self._memo is not None
            and now - self._memo[0] < self.memo_ttl_seconds
        ):
            return list(self._memo[1])
        recs: list[CallRecording] = []
        seen: set[str] = set()
        for p in self.base_dir.iterdir():
            if not p.is_file() or p.suffix.lower() != ".wav":
                continue
            try:
                stat = p.stat()
            except OSError:
                continue
            if now - stat.st_mtime < self.ignore_recent_seconds:
                continue
            seen.add(p.name)
            key = (stat.st_size, stat.st_mtime)
            cached = self._meta_cache.get(p.name)
            if cached is not None and cached[0] == key:
                if cached[1] is not None:
                    recs.append(cached[1])
                continue
            duration = _read_wav_duration(p)
            if duration < self.min_duration:
                # Remember the negative result so the header isn't
                # re-parsed on every poll for a permanently-short file.
                self._meta_cache[p.name] = (key, None)
                continue
            tgt, src, slot = _parse_filename(p.name)
            started_at = datetime.fromtimestamp(stat.st_mtime - duration)
            rec = CallRecording(
                filename=p.name,
                src=src,
                tgt=tgt,
                slot=slot,
                started_at=started_at,
                duration_seconds=duration,
                file_bytes=stat.st_size,
            )
            self._meta_cache[p.name] = (key, rec)
            recs.append(rec)
        # Drop cache entries for files that vanished (retention/manual rm).
        if len(self._meta_cache) > len(seen):
            self._meta_cache = {
                n: v for n, v in self._meta_cache.items() if n in seen
            }
        recs.sort(key=lambda r: r.started_at, reverse=True)
        self._memo = (now, recs)
        return list(recs)

    def prune_older_than(self, hours: float) -> tuple[int, int]:
        """Delete WAV files in ``base_dir`` older than ``hours``.

        Per-call WAVs accumulate forever otherwise — on a Pi 5 with a
        small SD card a busy DMR site fills the disk in a few days. We
        delete files whose mtime is older than the cutoff. Returns
        ``(deleted_count, deleted_bytes)`` so callers can log it.
        """
        if hours <= 0 or not self.base_dir.exists():
            return (0, 0)
        cutoff = time.time() - hours * 3600
        deleted = 0
        freed = 0
        for p in self.base_dir.iterdir():
            if not p.is_file() or p.suffix.lower() != ".wav":
                continue
            try:
                stat = p.stat()
            except OSError:
                continue
            if stat.st_mtime >= cutoff:
                continue
            try:
                p.unlink()
            except OSError:
                continue
            deleted += 1
            freed += stat.st_size
            self._meta_cache.pop(p.name, None)
        if deleted:
            self._memo = None  # listing changed — don't serve a stale memo
        return (deleted, freed)
