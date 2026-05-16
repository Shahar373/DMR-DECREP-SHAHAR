"""SQLite sidecar index over the events JSONL.

The JSONL file remains the source of truth — every line written there is the
canonical record. This module maintains a parallel SQLite database that mirrors
the same events with proper indexes on (ts, src, tgt, type) so the dashboard's
filtered history endpoints don't have to linearly scan the JSONL on every call.

Design notes:

* The index is rebuildable from the JSONL — if it ever gets out of sync or is
  deleted, ``rebuild_from_jsonl()`` regenerates it from scratch. Code that
  depends on the index should always be able to fall back to scanning the
  JSONL.
* Writes are batched: a commit happens every ``_BATCH_SIZE`` inserts OR every
  ``_BATCH_SECONDS`` (whichever first), via a daemon Timer that's re-armed on
  each append. Worst-case data loss on power yank is two seconds of *index*
  rows — the JSONL still has them, so ``--rebuild-index`` recovers fully.
* SQLite is opened in WAL mode with ``synchronous=NORMAL`` for ~10× faster
  appends on slow SD cards. ``check_same_thread=False`` plus an internal lock
  let the index be safely shared between the parser thread and the FastAPI
  worker threads.
* The ``payload`` column holds the full JSON line, so query endpoints never
  need to read the JSONL — one round-trip to SQLite is enough.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import threading
import time
from collections.abc import Iterable, Iterator
from datetime import datetime
from pathlib import Path
from typing import Optional


_SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY,
    ts TEXT NOT NULL,
    type TEXT NOT NULL,
    src INTEGER,
    tgt INTEGER,
    slot INTEGER,
    addressing TEXT,
    error_type TEXT,
    encrypted INTEGER,
    schema_version INTEGER NOT NULL,
    payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
CREATE INDEX IF NOT EXISTS idx_events_src_ts ON events(src, ts);
CREATE INDEX IF NOT EXISTS idx_events_tgt_ts ON events(tgt, ts);
CREATE INDEX IF NOT EXISTS idx_events_type_ts ON events(type, ts);

CREATE TABLE IF NOT EXISTS _meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

_INSERT_SQL = (
    "INSERT INTO events(ts, type, src, tgt, slot, addressing, "
    "error_type, encrypted, schema_version, payload) "
    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

_BATCH_SIZE = 100
_BATCH_SECONDS = 2.0


def _extract_columns(ev: dict) -> tuple:
    """Pull the indexed columns out of a JSON-serialised event dict.

    ``radio_id`` on ip_mapping events is normalised into ``src`` so all
    "which radio touched this row" queries share the same column.
    """
    et = ev.get("type", "")
    src = ev.get("src")
    if src is None and et == "ip_mapping":
        src = ev.get("radio_id")
    tgt = ev.get("tgt")
    slot = ev.get("slot")
    addressing = ev.get("addressing")
    error_type = ev.get("error_type") if et == "quality" else None
    encrypted = 1 if et == "encryption" else None
    return (
        ev.get("timestamp", ""),
        et,
        src,
        tgt,
        slot,
        addressing,
        error_type,
        encrypted,
        int(ev.get("schema_version", 1)),
        json.dumps(ev, separators=(",", ":")),
    )


class EventIndex:
    """Thin sqlite3 wrapper that mirrors the events JSONL."""

    def __init__(self, db_path: Path, schema_version: int = 1) -> None:
        self.db_path = Path(db_path)
        self.schema_version = schema_version
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA temp_store=MEMORY")
        self._conn.execute("PRAGMA cache_size=-2000")
        self._conn.executescript(_SCHEMA_DDL)
        self._lock = threading.Lock()
        self._pending = 0
        self._last_commit = time.monotonic()
        self._timer: Optional[threading.Timer] = None
        self._closed = False
        self.index_outdated = False
        self._record_schema_version()

    def _record_schema_version(self) -> None:
        cur = self._conn.execute("SELECT value FROM _meta WHERE key='schema_version'")
        row = cur.fetchone()
        if row is None:
            self._conn.execute(
                "INSERT INTO _meta(key, value) VALUES('schema_version', ?)",
                (str(self.schema_version),),
            )
            self._conn.execute(
                "INSERT OR REPLACE INTO _meta(key, value) VALUES('built_at', ?)",
                (datetime.now().isoformat(),),
            )
            self._conn.commit()
            return
        stored = int(row[0])
        if stored > self.schema_version:
            self.index_outdated = True
            print(
                f"# event index: schema_version mismatch "
                f"(db={stored}, code={self.schema_version}) — "
                "consider --rebuild-index",
                file=sys.stderr,
            )

    # --- write path ---

    def append(self, event_dict: dict) -> None:
        cols = _extract_columns(event_dict)
        with self._lock:
            if self._closed:
                return
            self._conn.execute(_INSERT_SQL, cols)
            self._pending += 1
            now = time.monotonic()
            if self._pending >= _BATCH_SIZE or (now - self._last_commit) >= _BATCH_SECONDS:
                self._commit_locked()
            elif self._timer is None:
                self._timer = threading.Timer(_BATCH_SECONDS, self._timed_commit)
                self._timer.daemon = True
                self._timer.start()

    def _commit_locked(self) -> None:
        if self._timer is not None:
            try:
                self._timer.cancel()
            except Exception:  # noqa: BLE001
                pass
            self._timer = None
        if self._pending == 0:
            self._last_commit = time.monotonic()
            return
        self._conn.commit()
        self._pending = 0
        self._last_commit = time.monotonic()

    def _timed_commit(self) -> None:
        with self._lock:
            if not self._closed:
                self._timer = None  # the firing timer is us; mark as already cancelled
                self._commit_locked()

    # --- read path ---

    @staticmethod
    def _build_where(
        since: Optional[datetime],
        until: Optional[datetime],
        src: Optional[int],
        tgt: Optional[int],
        types: Optional[Iterable[str]],
    ) -> tuple[str, list]:
        clauses: list[str] = []
        params: list = []
        if since is not None:
            clauses.append("ts >= ?")
            params.append(since.isoformat())
        if until is not None:
            clauses.append("ts <= ?")
            params.append(until.isoformat())
        if src is not None:
            clauses.append("src = ?")
            params.append(src)
        if tgt is not None:
            clauses.append("tgt = ?")
            params.append(tgt)
        if types:
            tlist = [str(t) for t in types]
            placeholders = ",".join("?" * len(tlist))
            clauses.append(f"type IN ({placeholders})")
            params.extend(tlist)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        return where, params

    def query(
        self,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        src: Optional[int] = None,
        tgt: Optional[int] = None,
        types: Optional[Iterable[str]] = None,
        limit: int = 500,
        offset: int = 0,
    ) -> list[dict]:
        where, params = self._build_where(since, until, src, tgt, types)
        sql = f"SELECT payload FROM events{where} ORDER BY ts LIMIT ? OFFSET ?"
        params.extend([int(limit), int(offset)])
        with self._lock:
            self._commit_locked()
            rows = self._conn.execute(sql, params).fetchall()
        return [json.loads(r[0]) for r in rows]

    def iter_query(
        self,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        src: Optional[int] = None,
        tgt: Optional[int] = None,
        types: Optional[Iterable[str]] = None,
    ) -> Iterator[dict]:
        where, params = self._build_where(since, until, src, tgt, types)
        sql = f"SELECT payload FROM events{where} ORDER BY ts"
        with self._lock:
            self._commit_locked()
            rows = self._conn.execute(sql, params).fetchall()
        for r in rows:
            yield json.loads(r[0])

    def count(
        self,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        src: Optional[int] = None,
        tgt: Optional[int] = None,
        types: Optional[Iterable[str]] = None,
    ) -> int:
        where, params = self._build_where(since, until, src, tgt, types)
        sql = f"SELECT COUNT(*) FROM events{where}"
        with self._lock:
            self._commit_locked()
            row = self._conn.execute(sql, params).fetchone()
        return int(row[0]) if row else 0

    def count_by_type(
        self,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> dict[str, int]:
        where, params = self._build_where(since, until, None, None, None)
        sql = f"SELECT type, COUNT(*) FROM events{where} GROUP BY type"
        with self._lock:
            self._commit_locked()
            return {row[0]: int(row[1]) for row in self._conn.execute(sql, params)}

    def count_quality_by_kind(
        self,
        since: Optional[datetime] = None,
    ) -> dict[str, int]:
        where, params = self._build_where(since, None, None, None, ["quality"])
        sql = f"SELECT error_type, COUNT(*) FROM events{where} GROUP BY error_type"
        with self._lock:
            self._commit_locked()
            return {row[0] or "": int(row[1]) for row in self._conn.execute(sql, params)}

    def time_bounds(
        self,
        since: Optional[datetime] = None,
    ) -> tuple[Optional[str], Optional[str]]:
        where, params = self._build_where(since, None, None, None, None)
        sql = f"SELECT MIN(ts), MAX(ts) FROM events{where}"
        with self._lock:
            self._commit_locked()
            row = self._conn.execute(sql, params).fetchone()
        if row is None:
            return None, None
        return row[0], row[1]

    # --- maintenance ---

    def rebuild_from_jsonl(self, jsonl_path: Path, progress_every: int = 100_000) -> int:
        """Drop the events table and replay every line from the JSONL.

        Used by ``--rebuild-index`` and by recovery flows when the index is
        out of sync. Returns the number of rows inserted.

        Performance notes for slow SD cards (Pi):
        * inserts are batched via ``executemany`` (5 000 rows per batch)
        * the JSONL's raw line is stored verbatim as the ``payload`` column
          (no second ``json.dumps`` round-trip)
        * progress is printed to stderr every ``progress_every`` rows so the
          operator sees that it's working
        """
        import sys as _sys
        import time as _time

        jsonl_path = Path(jsonl_path)
        BATCH = 5_000
        batch: list[tuple] = []
        with self._lock:
            self._commit_locked()
            self._conn.execute("DROP TABLE IF EXISTS events")
            self._conn.executescript(_SCHEMA_DDL)
            self._conn.execute(
                "INSERT OR REPLACE INTO _meta(key, value) VALUES('schema_version', ?)",
                (str(self.schema_version),),
            )
            self._conn.execute(
                "INSERT OR REPLACE INTO _meta(key, value) VALUES('built_at', ?)",
                (datetime.now().isoformat(),),
            )
            self._conn.execute(
                "INSERT OR REPLACE INTO _meta(key, value) VALUES('built_from_jsonl', ?)",
                (str(jsonl_path),),
            )
            self.index_outdated = False
            count = 0
            t0 = _time.monotonic()
            if jsonl_path.exists():
                with open(jsonl_path, "r", encoding="utf-8", errors="replace") as f:
                    for raw in f:
                        stripped = raw.strip()
                        if not stripped:
                            continue
                        try:
                            obj = json.loads(stripped)
                        except json.JSONDecodeError:
                            continue
                        if not obj.get("timestamp") or not obj.get("type"):
                            continue
                        et = obj.get("type", "")
                        src = obj.get("src")
                        if src is None and et == "ip_mapping":
                            src = obj.get("radio_id")
                        error_type = obj.get("error_type") if et == "quality" else None
                        encrypted = 1 if et == "encryption" else None
                        # Reuse the raw line verbatim — no re-serialisation cost.
                        batch.append((
                            obj.get("timestamp", ""),
                            et,
                            src,
                            obj.get("tgt"),
                            obj.get("slot"),
                            obj.get("addressing"),
                            error_type,
                            encrypted,
                            int(obj.get("schema_version", 1)),
                            stripped,
                        ))
                        if len(batch) >= BATCH:
                            self._conn.executemany(_INSERT_SQL, batch)
                            count += len(batch)
                            batch.clear()
                            if count % progress_every == 0:
                                elapsed = _time.monotonic() - t0
                                rate = count / elapsed if elapsed else 0
                                print(
                                    f"# rebuild: {count:>9,} rows ({rate:,.0f}/s)",
                                    file=_sys.stderr,
                                )
                    if batch:
                        self._conn.executemany(_INSERT_SQL, batch)
                        count += len(batch)
                        batch.clear()
            self._conn.commit()
        return count

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            try:
                self._commit_locked()
            finally:
                try:
                    self._conn.close()
                except sqlite3.Error:
                    pass
