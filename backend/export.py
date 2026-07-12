"""Day-partition migration + structured export helpers (v0.20.0).

Two jobs:

1. **Monolith migration** — split a legacy single-file ``events.jsonl``
   into per-day files under ``events/`` (``events-YYYY-MM-DD.jsonl``).
   Crash-safe and idempotent: buckets are written to a temp dir, a
   COMPLETE marker gates the rename phase, and the legacy file is only
   ever *renamed* (to ``events.jsonl.migrated``), never deleted. Lines
   whose timestamp can't be read go to ``events-unknown-date.jsonl`` —
   nothing is dropped.

2. **Export iterators** — NDJSON/CSV generators for ``/api/export``.
   A single-day unfiltered NDJSON export streams the raw day file
   directly (zero parse cost — the file IS the export); everything else
   goes through the SQLite ``stream_query`` or the JSONL fallback.
"""
from __future__ import annotations

import json
import os
import shutil
import sys
from pathlib import Path
from typing import Optional

_DAY_LEN = 10  # YYYY-MM-DD


def is_valid_day(day: str) -> bool:
    return (
        isinstance(day, str) and len(day) == _DAY_LEN
        and day[4] == "-" and day[7] == "-"
        and day[:4].isdigit() and day[5:7].isdigit() and day[8:].isdigit()
    )


def partition_dir_for(jsonl_path: Path) -> Path:
    """``events.jsonl`` → sibling directory ``events/``."""
    return jsonl_path.parent / jsonl_path.stem


def day_file_for(jsonl_path: Path, day: str) -> Path:
    return partition_dir_for(jsonl_path) / f"{jsonl_path.stem}-{day}.jsonl"


def _extract_day(line: str) -> Optional[str]:
    """Day (YYYY-MM-DD) from one JSONL line, or None if unreadable.

    Full ``json.loads`` for correctness — a one-time cost at migration.
    """
    try:
        obj = json.loads(line)
    except (ValueError, TypeError):
        return None
    ts = obj.get("timestamp")
    if isinstance(ts, str) and len(ts) >= _DAY_LEN and is_valid_day(ts[:_DAY_LEN]):
        return ts[:_DAY_LEN]
    return None


class MigrationRefused(RuntimeError):
    """Auto-migration declined to run — operator action needed."""


def migrate_monolith(jsonl_path: Path) -> Optional[dict]:
    """Split a legacy monolithic JSONL into per-day partition files.

    Returns metrics on success, ``None`` when there is nothing to do
    (no legacy file and no half-finished migration). Raises
    ``MigrationRefused`` when the partition dir already contains day
    files without a COMPLETE marker — that means the new writer already
    ran and blindly renaming buckets over its files could clobber fresh
    events; the operator must reconcile (usually: move the stray legacy
    file away, or delete it if it's a duplicate).

    Crash analysis:
    * crash while bucketing (no marker) → temp dir wiped and restarted
      from the untouched legacy file on the next run.
    * crash during renames (marker present) → renames resumed
      (idempotent ``os.replace``), then the legacy rename finishes.
    * the legacy file is renamed to ``<name>.migrated`` as the final
      step and kept as a backup for the operator to delete manually.
    """
    pd = partition_dir_for(jsonl_path)
    tmp = pd / ".migrate-tmp"
    marker = tmp / "COMPLETE"

    if marker.exists():
        # Resume a crash between marker and cleanup.
        metrics = _finish_migration(tmp, pd, jsonl_path)
        metrics["resumed"] = True
        print("# migrate: resumed and finished a previous migration",
              file=sys.stderr)
        return metrics

    if not jsonl_path.exists() or not jsonl_path.is_file():
        return None

    if pd.exists():
        existing = [p for p in pd.glob("*.jsonl")]
        if existing:
            raise MigrationRefused(
                f"partition dir {pd} already contains {len(existing)} day "
                f"file(s) but a legacy {jsonl_path.name} is also present. "
                "Refusing to auto-migrate (renaming buckets could clobber "
                "fresh day files). Move the legacy file away, or run "
                "'--migrate-jsonl' after reconciling."
            )

    # Fresh bucketing pass. A stale tmp dir (crash before marker) is
    # discarded — the legacy file is still the untouched superset.
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir(parents=True)

    stem = jsonl_path.stem
    handles: dict[str, object] = {}
    lines_total = 0
    lines_unknown = 0

    def _handle(day_key: str):
        fh = handles.get(day_key)
        if fh is None:
            fh = open(tmp / f"{stem}-{day_key}.jsonl", "a", encoding="utf-8")
            handles[day_key] = fh
        return fh

    try:
        with open(jsonl_path, "r", encoding="utf-8", errors="replace") as src:
            for line in src:
                if not line.strip():
                    continue
                lines_total += 1
                day = _extract_day(line)
                if day is None:
                    lines_unknown += 1
                    _handle("unknown-date").write(line if line.endswith("\n") else line + "\n")
                else:
                    _handle(day).write(line if line.endswith("\n") else line + "\n")
        for fh in handles.values():
            fh.flush()
            os.fsync(fh.fileno())
    finally:
        for fh in handles.values():
            try:
                fh.close()
            except OSError:
                pass

    marker.write_text("ok", encoding="utf-8")

    metrics = _finish_migration(tmp, pd, jsonl_path)
    metrics.update({
        "lines_total": lines_total,
        "lines_unknown_date": lines_unknown,
        "days": len([k for k in handles if k != "unknown-date"]),
        "resumed": False,
    })
    print(
        f"# migrate: split {lines_total:,} lines into {metrics['days']} day "
        f"file(s) (+{lines_unknown} unknown-date) under {pd}; legacy kept "
        f"as {jsonl_path.name}.migrated",
        file=sys.stderr,
    )
    return metrics


def _finish_migration(tmp: Path, pd: Path, jsonl_path: Path) -> dict:
    """Rename phase — only runs once the COMPLETE marker exists."""
    pd.mkdir(parents=True, exist_ok=True)
    moved = 0
    for f in sorted(tmp.glob("*.jsonl")):
        os.replace(f, pd / f.name)
        moved += 1
    if jsonl_path.exists() and jsonl_path.is_file():
        os.replace(jsonl_path, Path(str(jsonl_path) + ".migrated"))
    try:
        (tmp / "COMPLETE").unlink(missing_ok=True)
        tmp.rmdir()
    except OSError:
        pass
    return {"files_moved": moved}
