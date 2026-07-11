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


# Layout v2 (0.20.0): ``day`` (= ts[:10], indexed — day-granular queries
# and retention), plus nullable ``frequency`` / ``channel_label`` so the
# future multi-frequency capture phase needs no second schema migration.
# ``idx_events_day`` is created by ``_migrate_schema`` (not here) because
# CREATE INDEX on a pre-v2 table without the column would fail before the
# ALTERs run.
_SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY,
    ts TEXT NOT NULL,
    day TEXT,
    type TEXT NOT NULL,
    src INTEGER,
    tgt INTEGER,
    slot INTEGER,
    addressing TEXT,
    error_type TEXT,
    encrypted INTEGER,
    frequency REAL,
    channel_label TEXT,
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

_LAYOUT_VERSION = 2

_INSERT_SQL = (
    "INSERT INTO events(ts, day, type, src, tgt, slot, addressing, "
    "error_type, encrypted, frequency, channel_label, schema_version, payload) "
    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

_BATCH_SIZE = 100
_BATCH_SECONDS = 2.0


def stream_query(
    db_path: Path,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    src: Optional[int] = None,
    tgt: Optional[int] = None,
    types: Optional[Iterable[str]] = None,
    batch_size: int = 500,
    day_from: Optional[str] = None,
    day_to: Optional[str] = None,
) -> Iterator[dict]:
    """Stream matching events from the index with O(batch_size) memory.

    Unlike ``EventIndex.iter_query`` (which materialises the entire result
    set with ``fetchall()`` before yielding — a whole-history CSV export
    used to load every matching row into RAM at once), this opens its OWN
    read-only connection (WAL allows concurrent readers alongside the
    writer) and walks the cursor with ``fetchmany``. No writer lock is
    held across yields, so a slow download can't stall appends.

    ``check_same_thread=False`` because StreamingResponse iterates sync
    generators from a threadpool whose thread may differ per chunk.

    Raises ``sqlite3.OperationalError`` if the DB file doesn't exist —
    callers fall back to the JSONL scan path, same as an empty index.
    """
    where, params = EventIndex._build_where(
        since, until, src, tgt, types, day_from=day_from, day_to=day_to,
    )
    sql = f"SELECT payload FROM events{where} ORDER BY ts"
    conn = sqlite3.connect(
        f"file:{Path(db_path)}?mode=ro", uri=True, check_same_thread=False,
    )
    try:
        cur = conn.execute(sql, params)
        while True:
            rows = cur.fetchmany(batch_size)
            if not rows:
                break
            for r in rows:
                yield json.loads(r[0])
    finally:
        conn.close()


def _day_of(ts: str) -> Optional[str]:
    """``ts[:10]`` when it looks like a date, else None. Timestamps are
    naive-local ISO strings, so the day prefix is always the local day."""
    if isinstance(ts, str) and len(ts) >= 10 and ts[4] == "-" and ts[7] == "-":
        return ts[:10]
    return None


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
    ts = ev.get("timestamp", "")
    return (
        ts,
        _day_of(ts),
        et,
        src,
        tgt,
        slot,
        addressing,
        error_type,
        encrypted,
        ev.get("frequency"),
        ev.get("channel_label"),
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
        self._migrate_schema()
        self._record_schema_version()

    def _migrate_schema(self) -> None:
        """Bring a pre-v2 DB up to the current layout (day/frequency/
        channel_label columns + day index) and backfill ``day``.

        Runs once per DB (guarded by the ``layout_version`` meta key).
        The backfill is chunked (50k rows/transaction) with progress to
        stderr — a multi-GB legacy DB on a Pi SD card can take minutes,
        and this runs before serving starts.
        """
        cur = self._conn.execute("SELECT value FROM _meta WHERE key='layout_version'")
        row = cur.fetchone()
        if row is not None and int(row[0]) >= _LAYOUT_VERSION:
            return
        existing = {
            r[1] for r in self._conn.execute("PRAGMA table_info(events)")
        }
        for col, decl in (
            ("day", "TEXT"),
            ("frequency", "REAL"),
            ("channel_label", "TEXT"),
        ):
            if col not in existing:
                self._conn.execute(f"ALTER TABLE events ADD COLUMN {col} {decl}")
        self._conn.commit()
        # Chunked day backfill for pre-v2 rows.
        total = 0
        while True:
            cur = self._conn.execute(
                "UPDATE events SET day = substr(ts, 1, 10) WHERE rowid IN ("
                "  SELECT rowid FROM events WHERE day IS NULL LIMIT 50000"
                ")"
            )
            self._conn.commit()
            if cur.rowcount <= 0:
                break
            total += cur.rowcount
            print(
                f"# event index: day backfill {total:,} rows...",
                file=sys.stderr,
            )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_day ON events(day)"
        )
        self._conn.execute(
            "INSERT OR REPLACE INTO _meta(key, value) VALUES('layout_version', ?)",
            (str(_LAYOUT_VERSION),),
        )
        self._conn.commit()
        if total:
            print(
                f"# event index: layout v{_LAYOUT_VERSION} migration done "
                f"({total:,} rows backfilled)",
                file=sys.stderr,
            )

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

    def flush(self) -> None:
        """Commit any batched-but-uncommitted appends.

        Called before ``stream_query`` opens its read-only connection so
        the export sees rows appended in the last ``_BATCH_SECONDS``.
        """
        with self._lock:
            if not self._closed:
                self._commit_locked()

    # --- read path ---

    @staticmethod
    def _build_where(
        since: Optional[datetime],
        until: Optional[datetime],
        src: Optional[int],
        tgt: Optional[int],
        types: Optional[Iterable[str]],
        day_from: Optional[str] = None,
        day_to: Optional[str] = None,
    ) -> tuple[str, list]:
        clauses: list[str] = []
        params: list = []
        if since is not None:
            clauses.append("ts >= ?")
            params.append(since.isoformat())
        if until is not None:
            clauses.append("ts <= ?")
            params.append(until.isoformat())
        if day_from is not None:
            clauses.append("day >= ?")
            params.append(day_from)
        if day_to is not None:
            clauses.append("day <= ?")
            params.append(day_to)
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
        descending: bool = False,
        day_from: Optional[str] = None,
        day_to: Optional[str] = None,
    ) -> list[dict]:
        where, params = self._build_where(
            since, until, src, tgt, types, day_from=day_from, day_to=day_to,
        )
        order = "DESC" if descending else "ASC"
        sql = f"SELECT payload FROM events{where} ORDER BY ts {order} LIMIT ? OFFSET ?"
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
        """Deprecated for large result sets — materialises everything with
        ``fetchall()`` before yielding. Prefer the module-level
        ``stream_query`` which streams from a separate read-only
        connection with bounded memory."""
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
        day_from: Optional[str] = None,
        day_to: Optional[str] = None,
    ) -> int:
        where, params = self._build_where(
            since, until, src, tgt, types, day_from=day_from, day_to=day_to,
        )
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
        return row[0], row[1]

    # --- aggregates for the network graph / dossier ---
    # These replace what used to be "pull up to 500k payload rows into
    # Python and Counter() over them" — the single biggest memory spike in
    # the server (~120 MB per /api/network call on a wide window). SQLite
    # does the grouping; Python only sees the (small) grouped result.

    def pair_counts(
        self,
        since: Optional[datetime] = None,
        types: Optional[Iterable[str]] = None,
    ) -> list[tuple]:
        """Grouped pair activity: (src, tgt, type, addressing, count, max_ts).

        Cardinality is the number of distinct (src, tgt, type, addressing)
        combinations in the window — thousands, not hundreds of thousands.
        """
        where, params = self._build_where(since, None, None, None, types)
        sql = (
            "SELECT src, tgt, type, addressing, COUNT(*), MAX(ts) "
            f"FROM events{where} GROUP BY src, tgt, type, addressing"
        )
        with self._lock:
            self._commit_locked()
            return self._conn.execute(sql, params).fetchall()

    def distinct_gps_radios(self, since: Optional[datetime] = None) -> set[int]:
        """Radio ids that emitted at least one lrrp_position in the window."""
        where, params = self._build_where(since, None, None, None, ["lrrp_position"])
        sql = f"SELECT DISTINCT src FROM events{where}"
        with self._lock:
            self._commit_locked()
            rows = self._conn.execute(sql, params).fetchall()
        return {int(r[0]) for r in rows if r[0] is not None}

    def count_by_tgt(
        self,
        src: int,
        since: Optional[datetime] = None,
        types: Optional[Iterable[str]] = None,
    ) -> dict[int, int]:
        """{tgt: count} for one radio's events, grouped in SQL."""
        where, params = self._build_where(since, None, src, None, types)
        # ``src`` is always present, so ``where`` is never empty here.
        sql = (
            f"SELECT tgt, COUNT(*) FROM events{where} "
            "AND tgt IS NOT NULL GROUP BY tgt"
        )
        with self._lock:
            self._commit_locked()
            return {
                int(row[0]): int(row[1])
                for row in self._conn.execute(sql, params)
            }

    def radio_bounds(
        self,
        radio_id: int,
        since: Optional[datetime] = None,
    ) -> tuple[Optional[str], Optional[str]]:
        """(first_ts, last_ts) where the radio appears as src OR tgt.

        Two index-friendly MIN/MAX queries (src=?, tgt=?) merged in Python
        — a single ``src=? OR tgt=?`` predicate would defeat both partial
        indexes and scan the table.
        """
        bounds: list[tuple[Optional[str], Optional[str]]] = []
        for col in ("src", "tgt"):
            clauses = [f"{col} = ?"]
            params: list = [radio_id]
            if since is not None:
                clauses.append("ts >= ?")
                params.append(since.isoformat())
            sql = f"SELECT MIN(ts), MAX(ts) FROM events WHERE {' AND '.join(clauses)}"
            with self._lock:
                self._commit_locked()
                row = self._conn.execute(sql, params).fetchone()
            bounds.append((row[0], row[1]))
        firsts = [b[0] for b in bounds if b[0] is not None]
        lasts = [b[1] for b in bounds if b[1] is not None]
        return (min(firsts) if firsts else None, max(lasts) if lasts else None)

    def hourly_histogram(
        self,
        radio_id: int,
        since: Optional[datetime] = None,
    ) -> list[int]:
        """24-bucket histogram of the radio's activity (src or tgt rows).

        ``substr(ts, 12, 2)`` is the HH of a naive ISO timestamp. Built as
        a UNION ALL of two per-column GROUP BYs so each side uses its
        partial index. Known approximation vs the old Python de-dup: an
        event where the radio is BOTH src and tgt counts twice here —
        rare, and the histogram is illustrative.
        """
        since_clause = " AND ts >= ?" if since is not None else ""
        sql = (
            "SELECT hh, SUM(c) FROM ("
            f"  SELECT substr(ts, 12, 2) AS hh, COUNT(*) AS c FROM events"
            f"  WHERE src = ?{since_clause} GROUP BY hh"
            "  UNION ALL"
            f"  SELECT substr(ts, 12, 2) AS hh, COUNT(*) AS c FROM events"
            f"  WHERE tgt = ?{since_clause} GROUP BY hh"
            ") GROUP BY hh"
        )
        params: list = [radio_id]
        if since is not None:
            params.append(since.isoformat())
        params.append(radio_id)
        if since is not None:
            params.append(since.isoformat())
        hourly = [0] * 24
        with self._lock:
            self._commit_locked()
            for hh, c in self._conn.execute(sql, params):
                try:
                    hour = int(hh)
                except (TypeError, ValueError):
                    continue
                if 0 <= hour <= 23:
                    hourly[hour] = int(c)
        return hourly

    def count_encryption_by_slot(
        self,
        since: Optional[datetime] = None,
    ) -> dict[int, int]:
        """{slot: count} of encryption events in the window."""
        where, params = self._build_where(since, None, None, None, ["encryption"])
        sql = f"SELECT slot, COUNT(*) FROM events{where} GROUP BY slot"
        with self._lock:
            self._commit_locked()
            return {
                int(row[0]): int(row[1])
                for row in self._conn.execute(sql, params)
                if row[0] is not None
            }

    # --- day partitioning ---

    def days_summary(self) -> list[dict]:
        """Per-day counts for ``/api/days`` — fast on ``idx_events_day``.

        Returns ``[{day, events, voice_events, first_ts, last_ts}, ...]``
        oldest day first.
        """
        sql = (
            "SELECT day, COUNT(*), "
            "SUM(CASE WHEN type='voice_call' THEN 1 ELSE 0 END), "
            "MIN(ts), MAX(ts) "
            "FROM events WHERE day IS NOT NULL GROUP BY day ORDER BY day"
        )
        with self._lock:
            self._commit_locked()
            rows = self._conn.execute(sql).fetchall()
        return [
            {
                "day": r[0],
                "events": int(r[1]),
                "voice_events": int(r[2] or 0),
                "first_ts": r[3],
                "last_ts": r[4],
            }
            for r in rows
        ]

    def day_stats(self, day: str) -> dict:
        """Aggregates for one local day, shaped like ``EventLog.stats()``
        so the stats page reuses its chart code for historical days.

        Semantics note: ``calls_by_src`` / ``calls_by_tg`` count voice
        FRAMES here (SQL GROUP BY), not distinct keyups like the live
        ring-buffer stats — relative proportions are what the charts
        show, and those are preserved.
        """
        def _grouped(sql: str, params: tuple) -> dict:
            return {
                str(row[0]): int(row[1])
                for row in self._conn.execute(sql, params)
                if row[0] is not None
            }

        with self._lock:
            self._commit_locked()
            by_type = _grouped(
                "SELECT type, COUNT(*) FROM events WHERE day=? GROUP BY type",
                (day,),
            )
            calls_by_src = _grouped(
                "SELECT src, COUNT(*) FROM events WHERE day=? AND "
                "type='voice_call' AND src > 0 GROUP BY src "
                "ORDER BY COUNT(*) DESC LIMIT 20",
                (day,),
            )
            calls_by_tg = _grouped(
                "SELECT tgt, COUNT(*) FROM events WHERE day=? AND "
                "type='voice_call' AND src > 0 GROUP BY tgt "
                "ORDER BY COUNT(*) DESC LIMIT 20",
                (day,),
            )
            positions_by_src = _grouped(
                "SELECT src, COUNT(*) FROM events WHERE day=? AND "
                "type='lrrp_position' GROUP BY src "
                "ORDER BY COUNT(*) DESC LIMIT 20",
                (day,),
            )
            quality_by_kind = _grouped(
                "SELECT error_type, COUNT(*) FROM events WHERE day=? AND "
                "type='quality' GROUP BY error_type",
                (day,),
            )
            # Same key shape as EventLog.stats(): "YYYY-MM-DD HH:00".
            hourly = _grouped(
                "SELECT substr(ts, 1, 10) || ' ' || substr(ts, 12, 2) || ':00', "
                "COUNT(*) FROM events WHERE day=? GROUP BY 1 ORDER BY 1",
                (day,),
            )
            row = self._conn.execute(
                "SELECT COUNT(*), MIN(ts), MAX(ts) FROM events WHERE day=?",
                (day,),
            ).fetchone()
        total = int(row[0]) if row else 0
        return {
            "window_size": total,
            "window_capacity": total,
            "first_event_at": row[1] if row else None,
            "last_event_at": row[2] if row else None,
            "events_by_type": by_type,
            "calls_by_src": calls_by_src,
            "calls_by_tg": calls_by_tg,
            "positions_by_src": positions_by_src,
            "hourly": hourly,
            "encrypted_calls": by_type.get("encryption", 0),
            "quality_by_kind": quality_by_kind,
        }

    def prune_days_older_than(
        self, cutoff_day: str, chunk_size: int = 50_000,
    ) -> int:
        """Delete all rows with ``day < cutoff_day``. Returns rows removed.

        Day-granular sibling of ``prune_older_than`` — an indexed range
        DELETE on the ``day`` column, chunked like the ts-based prune so
        readers get a chance between batches.
        """
        total = 0
        sql = (
            "DELETE FROM events WHERE rowid IN ("
            "  SELECT rowid FROM events WHERE day < ? LIMIT ?"
            ")"
        )
        while True:
            with self._lock:
                if self._closed:
                    break
                self._commit_locked()
                cur = self._conn.execute(sql, (cutoff_day, chunk_size))
                removed = cur.rowcount
                self._conn.commit()
            if removed <= 0:
                break
            total += removed
            if removed < chunk_size:
                break
        return total

    # --- maintenance ---

    def rebuild_from_jsonl(self, jsonl_path: Path, progress_every: int = 100_000) -> int:
        """Drop the events table and replay every line from the JSONL.

        ``jsonl_path`` may be a single file OR a day-partition directory —
        a directory rebuilds from every ``*.jsonl`` inside it in sorted
        (chronological) filename order.

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
        if jsonl_path.is_dir():
            sources = sorted(jsonl_path.glob("*.jsonl"))
        elif jsonl_path.exists():
            sources = [jsonl_path]
        else:
            sources = []
        BATCH = 5_000
        batch: list[tuple] = []
        with self._lock:
            self._commit_locked()
            self._conn.execute("DROP TABLE IF EXISTS events")
            self._conn.executescript(_SCHEMA_DDL)
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_day ON events(day)"
            )
            self._conn.execute(
                "INSERT OR REPLACE INTO _meta(key, value) VALUES('schema_version', ?)",
                (str(self.schema_version),),
            )
            self._conn.execute(
                "INSERT OR REPLACE INTO _meta(key, value) VALUES('layout_version', ?)",
                (str(_LAYOUT_VERSION),),
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
            for source in sources:
                with open(source, "r", encoding="utf-8", errors="replace") as f:
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
                        ts = obj.get("timestamp", "")
                        # Reuse the raw line verbatim — no re-serialisation cost.
                        batch.append((
                            ts,
                            _day_of(ts),
                            et,
                            src,
                            obj.get("tgt"),
                            obj.get("slot"),
                            obj.get("addressing"),
                            error_type,
                            encrypted,
                            obj.get("frequency"),
                            obj.get("channel_label"),
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

    def clear(self) -> None:
        """Delete every row from the events table."""
        with self._lock:
            if self._closed:
                return
            self._commit_locked()
            self._conn.execute("DELETE FROM events")
            self._conn.commit()

    # --- retention ---

    def prune_older_than(
        self, cutoff: datetime, chunk_size: int = 50_000,
    ) -> int:
        """Delete all rows with ``ts < cutoff``. Returns total rows removed.

        Done in chunks so the write transaction doesn't lock the DB for
        minutes when the first prune has millions of rows to remove. After
        each chunk the WAL is committed, giving readers a chance to
        proceed between batches.
        """
        cutoff_iso = cutoff.isoformat()
        total = 0
        # rowid-IN is required because vanilla SQLite builds don't include
        # the SQLITE_ENABLE_UPDATE_DELETE_LIMIT compile flag.
        sql = (
            "DELETE FROM events WHERE rowid IN ("
            "  SELECT rowid FROM events WHERE ts < ? LIMIT ?"
            ")"
        )
        while True:
            with self._lock:
                if self._closed:
                    break
                self._commit_locked()  # flush pending writes first
                cur = self._conn.execute(sql, (cutoff_iso, chunk_size))
                removed = cur.rowcount
                self._conn.commit()
            if removed <= 0:
                break
            total += removed
            if removed < chunk_size:
                break
        return total

    def vacuum(self) -> None:
        """Rebuild the DB file to reclaim space freed by prune_older_than.

        VACUUM takes an exclusive lock; with WAL + synchronous=NORMAL this
        still queues writes for the duration. After the first big prune
        (multi-GB → tens of MB) it can run for tens of seconds; on
        subsequent prunes the working set is small and VACUUM is fast.
        """
        with self._lock:
            if self._closed:
                return
            self._commit_locked()
            # VACUUM cannot run inside an explicit transaction.
            self._conn.isolation_level = None
            try:
                self._conn.execute("VACUUM")
            finally:
                self._conn.isolation_level = ""
