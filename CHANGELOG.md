# Changelog

Versioning follows [Semantic Versioning](https://semver.org/):

* **patch** — bug fix, internal cleanup, no UI/API change
* **minor** — new feature, backwards-compatible
* **major** — breaking change to CLI / HTTP API / event schema

Source of truth: `backend/__init__.py` (`__version__`). The dashboard
footer shows the running build's version and `/api/version` exposes it.

## [0.23.0] — 2026-07-11

SDRplay direct tuning — the RSP1B can now be driven straight from the
monitor via SoapySDR, removing the manual SDRconnect GUI + virtual audio
cable. First step toward the fully-automatic, multi-frequency capture in
later phases.

### Added

- **`--rf-backend {pulse,soapy}`** (default `pulse` for backward
  compatibility). With `soapy`, `dsd-fme` opens the SDRplay RSP directly
  (`-i soapy:driver=sdrplay:<freq>:<gain>:<ppm>:<bw>`) and the monitor
  tunes it from new flags: `--frequency` (Hz or `NNN.NM` MHz),
  `--sdr-driver`, `--sdr-device-args`, `--gain`, `--ppm`,
  `--bandwidth-khz`. No SDRconnect, no `dmr_capture` sink.
- `build_dsd_command()` / `build_soapy_input()` / `normalize_frequency()`
  — pure, unit-tested command construction (hardware not required);
  startup fails fast with a clear message if `soapy` is selected without
  a valid `--frequency`.
- **`scripts/setup-sdrplay.sh`** — installs/verifies the SoapySDR
  runtime, the SoapySDRPlay3 driver, and device discovery
  (`SoapySDRUtil --find`), with exact instructions for the proprietary
  SDRplay API service. `scripts/check_env.sh` now checks the SDR chain
  (API service + SoapySDR device) instead of just SDRconnect;
  `scripts/install-deps.sh` installs `soapysdr-tools`/`libsoapysdr-dev`.

### Changed

- `scripts/dmr-monitor.service` defaults to the `soapy` backend (with the
  legacy pulse invocation kept as an inline comment); README pipeline
  diagram documents both backends.

### Notes

- The exact SoapySDR arg string can differ between `dsd-fme` forks
  (lwvmobile vs the newer dsd-neo) — verify against `dsd-fme -h` on the
  target if a live spawn fails. The event schema already carries optional
  `frequency`/`channel_label` (added in 0.20.0), so the upcoming
  multi-frequency phase needs no further migration.

## [0.22.0] — 2026-07-11

Day-based navigation — the collected history is now browsable and
exportable by day from the UI, with linkable URLs.

### Added

- **Day picker** (shared component, `◀ [Today — live / recorded days] ▶`
  fed by `/api/days`) on the Debrief and Stats pages. The selected day
  lives in the URL (`?day=YYYY-MM-DD`) so day views are shareable links;
  deep links auto-select the day on load.
- **Debrief day view**: picking a day queries `/api/history?day=` with
  real pagination, disables the free-form time range, and exposes
  per-day export buttons — CSV via `/api/export` and raw NDJSON (the
  day file itself, byte-for-byte).
- **`GET /api/stats/day/{day}`** — whole-day statistics shaped exactly
  like `/api/stats` (per-type counts, top talkers/talkgroups, hourly
  buckets, quality ratios computed over the full day) so the Stats page
  reuses its chart rendering unchanged. Live mode is now explicitly
  labeled "live (in-memory window)" vs "day YYYY-MM-DD"; historical days
  don't auto-refresh.
- Dossier `first_seen`/`last_seen` timestamps deep-link into the Debrief
  day view for that date.

### Fixed

- The Stats page no longer dies entirely when the Chart.js CDN is
  unreachable (offline Pi / proxy hiccup) — charts stay empty but the
  quality analyzer, tables, and day navigation keep working.

## [0.21.0] — 2026-07-11

Responsive frontend foundation — the five dashboard pages now adapt to
phone / tablet / desktop instead of being desktop-only, on a shared
design-token + component layer.

### Added

- **`frontend/assets/style.css`** — shared design tokens (the dark
  palette, a rem type scale, spacing) and components (nav, buttons,
  hamburger) extracted from the 5× copy-pasted per-page `<style>` blocks,
  plus per-page responsive overrides at three breakpoints (<640px,
  640–1023px, ≥1024px). `@media (pointer: coarse)` enforces ≥44px touch
  targets.
- **`frontend/assets/shared.js`** — the site nav is now injected from one
  place (`data-shared-nav` placeholder on every page, active link derived
  from the pathname, hamburger below 640px) and `/api/version` is fetched
  once for the version tag instead of per-page copies.
- Live header shows **"sent/total" radios** when the broadcast snapshot
  is trimmed, with the archived count in the tooltip.

### Changed

- **Phones get real layouts**: the live dashboard's fixed
  `100vh/overflow:hidden` two-column shell becomes a single scrolling
  column (map 45dvh on top, then calls/history/debrief, radios table with
  its own horizontal scroll); Debrief's 7-column table renders as labeled
  cards; Stats/Alerts grids collapse (min(420px,100%) minimums); Network
  stacks the graph above the edge list with dvh sizing.
- **Radios table renders incrementally** — rows are keyed by radio id and
  only re-render when their content signature changes; DOM order is
  touched only when the sort order changes. Previously the whole tbody
  was rebuilt via innerHTML on every WS frame plus an O(rows)
  querySelectorAll pass injecting Dossier buttons (now built into the row
  template).
- Cytoscape skips the expensive cose re-layout when the graph topology is
  unchanged between refreshes (weights update in place) — the biggest
  mobile CPU win on the Network page.
- Debrief page-size options aligned with the server clamp
  (100/500/1000/2000, default 500).

## [0.20.0] — 2026-07-11

Day-partitioned data layer — collected data is now organised by local
day with rollover at midnight, structured per-day export, and retention
that drops whole days instead of rewriting a multi-GB file.

### Added

- **Per-day JSONL partitioning** (live CLI mode): events are written to
  `events/events-YYYY-MM-DD.jsonl`. Rollover is *lazy by event date* — the
  day file is picked from each event's own timestamp inside `append()`,
  never by a wall-clock timer, so replaying a historical capture lands in
  the capture's own days and no event can straddle a rotation.
- **Automatic monolith migration**: a legacy single-file `events.jsonl` is
  split into per-day files at startup (or explicitly via
  `--migrate-jsonl`). Crash-safe and idempotent — buckets are staged in a
  temp dir behind a COMPLETE marker, unreadable lines go to an
  `-unknown-date` bucket (never dropped), and the legacy file is only ever
  *renamed* to `events.jsonl.migrated` as a backup.
- **`--retention-days N`** — day-granular retention: whole day files are
  unlinked and their index rows deleted (indexed `DELETE WHERE day < ?`).
  Mutually exclusive with the now-deprecated `--event-retention-hours`
  (which, in partition mode, also degrades gracefully to whole-day
  unlinks — the multi-GB JSONL rewrite is gone).
- **`GET /api/days`** — days with data + per-day event/voice counts and
  time bounds (indexed `GROUP BY day`), for the upcoming day-picker UI.
- **`GET /api/export?day=YYYY-MM-DD&format=ndjson|csv`** (and
  `from=`/`to=` ranges, plus `types`/`src`/`tgt` filters) — streaming
  structured export. A single unfiltered day as NDJSON streams the raw
  partition file byte-for-byte; everything else goes through the
  read-only SQLite cursor with O(batch) memory. `day=` filter also added
  to `/api/history` and `/api/history.csv`.
- **SQLite layout v2**: `day` column (= `ts[:10]`, indexed) plus nullable
  `frequency`/`channel_label` columns and matching CSV tail columns —
  pre-provisioned for the SDRplay multi-frequency phase so no second
  migration will be needed. Existing DBs are ALTERed and backfilled in
  chunked transactions at startup (progress logged; minutes on a multi-GB
  Pi SD card, one time).

### Changed

- Startup priming reads **today's** day file only, instead of pydantic-
  parsing the entire history under the buffer lock on every boot.
  `/api/health` reports the summed size of all day files.

## [0.19.0] — 2026-07-11

Load hardening, part 2 of 2 — the heavy analytical endpoints now aggregate
inside SQLite instead of materialising raw event rows in Python, and live
state is bounded.

### Changed

- **`/api/network` runs as GROUP BY in SQLite.** New
  `EventIndex.pair_counts()` / `distinct_gps_radios()` aggregates replace
  the two up-to-500k-row pulls (~120 MB of Python dicts per call — the
  single most likely OOM trigger on a Pi). The graph output is
  arithmetically identical; an equivalence test pins the old row-by-row
  logic as the oracle.
- **Dossier (`/api/radio/{id}`) uses SQL aggregates** — `radio_bounds()`
  (MIN/MAX), `hourly_histogram()` (UNION ALL of two indexed GROUP BYs),
  `count_by_tgt()`, `count_encryption_by_slot()`. Call-session grouping now
  reads only the newest 5000 voice frames (`descending=True` query) instead
  of every voice row in the window; the bound is surfaced as
  `recent_calls_window_rows` in the response. Known approximation: an event
  where the radio is both src and tgt double-counts in the histogram.

### Added

- **`--max-radios`** (default 2000) — LRU cap on live radios by
  `last_seen`, evicted in ~5% batches; enforced on snapshot restore too, so
  a fat legacy `snapshot.json` can't resurrect an oversized dict. Evicted
  radios stay fully queryable via `/api/history` and `/api/radio/{id}`;
  additive `radios_evicted_total` snapshot field lets the UI show
  "N archived".
- `EventIndex.query(descending=True)` for newest-first bounded pulls.

## [0.18.0] — 2026-07-11

Load hardening, part 1 of 2 — closes the biggest "dashboard freezes or
crashes under load" holes found in a full audit of the server layer.
Groundwork for the day-partitioned data layer and the responsive UI that
follow.

### Added

- **Trimmed broadcast snapshots.** The 1 Hz WebSocket payload (and
  `/api/snapshot`) is now serialised **once per tick** and fanned out,
  instead of being rebuilt per client/request on the event loop. The
  broadcast view caps radios at 2000 (newest `last_seen` first) and drops
  GPS trails older than 15 minutes; new additive `radios_total` field lets
  the UI show "N of M". The persisted `snapshot.json` remains the full,
  untrimmed view.
- **`--snapshot-persist-interval`** (default 30 s) — full `snapshot.json`
  writes now happen on their own cadence instead of at 1 Hz, cutting SD-card
  wear and per-tick CPU. Worst case on power-yank: the last 30 s of
  radio-state freshness; events themselves are still bounded by the 5 s
  JSONL fsync.
- **`stream_query()`** in `backend.event_index` — streams query results
  from a dedicated read-only SQLite connection with `fetchmany`, O(batch)
  memory. `/api/history.csv` now uses it; previously `iter_query()`
  materialised the entire filtered history in RAM before the first byte
  went out.
- **Heavy-endpoint concurrency guard** — `/api/history`, `/api/network`,
  `/api/radio/{id}` and `/api/quality` now run at most 2 at a time with a
  short queue; excess requests get `503 + Retry-After: 2` instead of
  exhausting the worker-thread pool.
- **`--reset-token`** — `POST /api/reset` (destructive, previously
  unauthenticated) now requires the `X-Reset-Token` header when a token is
  configured, and is loopback-only otherwise. The dashboard prompts for the
  token on 403.
- **`--dev-reload-html`** — opt-out of the new in-memory HTML page cache
  during frontend development.

### Changed

- `/api/history` `limit` ceiling lowered 20000 → 2000. Bulk pulls belong to
  the streaming CSV export; a single 20k-dict JSON response was a real
  memory spike on a Pi.
- Frontend HTML pages are cached in memory after first read instead of
  hitting the SD card on every request; shared static assets (when present)
  are served from `/assets` via `StaticFiles`.
- `/api/recordings` directory scans are cached: per-file WAV metadata keyed
  by `(size, mtime)` plus a 2 s TTL memo of the full listing, so N dashboard
  tabs polling every 3 s share one scan and headers are parsed once per
  file version.

## [0.17.0] — 2026-06-06

Fresh-Raspberry-Pi install hardening — a full audit (Python packaging, ARM
wheel availability, system deps, runtime robustness, docs) of what it takes
to bring the monitor up cleanly on a brand-new Pi.

### Added

- **`scripts/install-deps.sh`** — one-shot apt installer for every system
  dependency on a fresh 64-bit Raspberry Pi OS (Bookworm): the Python
  toolchain, the full dsd-fme build-dependency set, `pulseaudio-utils`
  (`pactl`, daemon-agnostic — works with the PipeWire-pulse shim that ships
  on Bookworm), and the optional Phase 4b packages (ffmpeg, icecast2).
  Idempotent; dsd-fme itself stays a documented source build.
- **`scripts/setup-pulseaudio.sh`** — creates the `dmr_capture` null sink
  that `--input pulse:dmr_capture.monitor` depends on, verifies the
  `.monitor` source, and installs a `systemd --user` oneshot so the sink is
  recreated on every boot. Closes the classic "the sink existed on the old
  Pi but not the new one" failure.

### Fixed

- **Cryptic crash when dsd-fme is missing or the wrong architecture.**
  `--live` now preflights `shutil.which(--dsd-bin)` and exits with a clear,
  actionable message instead of letting a bare `FileNotFoundError` bubble up
  and systemd restart-loop on it. `stream_subprocess_with_retry` also now
  catches `OSError` (e.g. an armv7 dsd-fme on a 64-bit Pi → "Exec format
  error") and surfaces a throttled, readable reason rather than taking the
  whole service down.
- **README drift that broke a literal fresh install:** removed the
  non-existent `git pull origin claude/dmr-monitoring-dashboard-00IUG`
  branch step (replaced with a real clone-from-`main` flow), removed the
  `--audio-out` flag that no longer exists in the CLI, and rewrote the
  install section around the new scripts.

### Changed

- **README** — status table now reflects reality (Phase 4a dashboard/WS UI
  is `done`, not "next"); the CLI-options list is synced to the actual
  argparse flags (`--serve`, `--port`, `--calls-dir`, `--event-retention-hours`,
  `--rebuild-index`, …); the Layout section lists the real module/test/script
  set.
- **`scripts/check_env.sh`** — reports CPU architecture, warns when a found
  dsd-fme binary isn't aarch64 on a 64-bit OS, points the `dmr_capture` sink
  check at `setup-pulseaudio.sh`, and downgrades ffmpeg/icecast2 from FAIL to
  WARN (they're optional Phase 4b deps the current code never invokes).
- **`.gitignore`** — ignore `snapshot.json*` (the atomic writer's `.bak`
  sidecar) so runtime artifacts stop showing up as untracked.

## [0.16.2] — 2026-06-06

### Fixed

- **Missing `httpx` dependency** — `requirements.txt` did not list `httpx`,
  which `fastapi.testclient.TestClient` (via Starlette) needs as its test
  transport. On a fresh install (e.g. a new Raspberry Pi) the five
  server/API test modules — `test_alerts`, `test_health`, `test_network`,
  `test_radio_dossier`, `test_server_events` — failed to even collect with
  `RuntimeError: The starlette.testclient module requires the httpx
  package to be installed`. Added `httpx>=0.27,<1` so a clean
  `pip install -r requirements.txt` makes the whole suite runnable.

### Changed

- README: corrected the post-install test-count hint from `60/60` to the
  current `177/177` so a fresh checkout knows what a green run looks like.

## [0.16.1] — 2026-06-05

### Added

- **Reset / Clear All Data** — new `POST /api/reset` endpoint that wipes all
  in-memory state (radios, active calls, quality counters), truncates the
  on-disk JSONL event log, clears the SQLite index, and removes the
  `snapshot.json` file so a subsequent service restart also starts clean.
  A red **↺ Reset** button in the dashboard header triggers the endpoint
  with a confirmation prompt, then reloads the page to reflect the blank
  slate. Useful when switching analysis sessions or discarding data from a
  previous shift.

## [0.16.0] — 2026-05-20

### Fixed — second stability pass: concurrency, durability, observability

A second deep audit (concurrency + memory + async-I/O + crash-recovery
domains, four parallel agents) surfaced five real bugs that this
release closes.

**Evaluator caches were unlocked between the parser thread and the
tick worker thread (alerts.py)**

`evaluator.tick()` runs via `asyncio.to_thread(...)` so it does not
share the asyncio loop with `evaluator.on_event()`. Despite the
docstring's "thread-safe" claim, the side caches `_slot_call`,
`_cc_silent_fired`, `_last_cc_at`, `_last_call_key`, and `_last_fired`
were mutated in `on_event` / `_record` outside `_lock`, while `tick →
_evaluate_tick` read them from a different OS thread. The observable
symptom was duplicate / missed alerts under load — `_cooldown_ok`
could read a stale `_last_fired`, and `_cc_silent_fired` could fire
twice for the same outage.

Fix: every read and write of those caches now goes through `_lock`.
`_evaluate_tick` snapshots `_last_cc_at` into a local before doing the
arithmetic, and re-checks the latch under the lock before adding —
classic double-checked locking. The slow `quality_ratios_over_window`
JSONL/SQLite scan still happens outside the lock so a busy quality
spike rule can't block the parser path.

**JSONL writes had no fsync — power-loss could lose tens of seconds
of events (event_log.py + cli.py)**

`open(..., buffering=1)` line-buffers into the Python writer, which
hands each newline-terminated line to the kernel. But the kernel can
hold those pages in the page cache for ~30 s before flushing — a
power yank in that window silently loses every event in the cache.

Fix: a new `_periodic_fsync` asyncio task calls `event_log.fsync_to_disk()`
every 5 s, which runs `os.fsync(self._fh.fileno())` on a worker thread.
`close()` also fsyncs on graceful shutdown. Worst-case loss on a power
yank is now bounded to a 5 s window of events instead of whatever the
kernel happened to be holding.

**Silent disk-full on event append (event_log.py)**

The append path swallowed every `OSError` with `except Exception:
pass` — "never let logging take down the pipeline". Good intent,
terrible visibility: a full disk caused events to silently stop
persisting while the in-memory buffer kept ticking, so a crash later
in the day looked like a clean run with mysteriously truncated
history.

Fix: the exception handler now counts write errors and stashes the
last error message. The first failure prints a one-shot warning to
stderr (with diagnosis hint: "check disk space / perms"). A new
`writer` block in `/api/health` exposes `write_errors` and
`last_write_error` so operators / external watchdogs can alert on
non-zero values.

**alerts.json had no `.bak` fallback (alerts.py)**

`atomic_write_text` already creates a `.bak` next to the new file on
every save, but `Evaluator.load_rules` never tried it on
corruption — it just renamed the bad file to `.bad` and started with
zero rules. A power yank during rule persistence therefore
permanently lost every configured alert.

Fix: `load_rules` now mirrors `state.load_snapshot` — it iterates
`(rules_path, rules_path + ".bak")` and uses the first one that parses.
A recovery from `.bak` is logged so the operator notices.

**Background tasks died silently when they raised (cli.py)**

`snap_task`, `retention_task`, `event_retention_task` were created
with `asyncio.create_task(...)` and never had their exceptions
surfaced — an unhandled error in any of them only became visible
when Python finally GC'd the Task, often minutes after the subsystem
had silently stopped working. The new `fsync_task` would have had the
same problem.

Fix: a `_surface_task_exception(name)` factory generates a
done-callback for each background task that prints
"`name` exited with `<exc>` — this subsystem is now dead until restart"
the instant the task crashes, so retention / snapshot / fsync
failures are visible immediately.

## [0.15.1] — 2026-05-19

### Fixed — WebSocket task leak and systemd crash-loop hardening

**WebSocket dead-client leak (server.py)**

Both `/ws` and `/ws/alerts` used a bare `await q.get()` that blocks
forever when a client disappears without a clean TCP FIN (network drop,
proxy timeout, NAT eviction). The specific failure modes:

* `/ws` — once a dead client's queue fills up, it is evicted from
  `_subscribers` by `push_snapshot()`, but its asyncio task keeps
  running, blocked on a queue that nobody will ever push to again.
* `/ws/alerts` — alerts are infrequent so the queue rarely fills;
  a dead client's task ran until process restart (hours).

Fix: both endpoints now use `asyncio.wait_for(q.get(), timeout=30)`.
On timeout the eviction case breaks out immediately; the alive-but-quiet
case sends a `{"type":"_ping"}` keepalive frame and loops.  If the
underlying socket is dead the send raises, and the task exits cleanly.
All three frontend onmessage handlers (`index.html` ×2, `alerts.html`)
now guard against `type === "_ping"` so no garbage is rendered.

**systemd crash-loop prevention (dmr-monitor.service)**

`Restart=on-failure` with no `StartLimitBurst` means a service that
crashes on startup (wrong audio device, port already bound, etc.) can
restart hundreds of times, filling the journal and keeping the system
busy.  Added:

* `StartLimitBurst=5` + `StartLimitIntervalSec=120s` — systemd stops
  restarting after 5 attempts in 2 minutes and enters a failed state,
  making the problem visible without log-spam.
* `RestartSec=10s` (was 5 s) — gives the audio device or port a few
  extra seconds to settle between attempts.
* `MemoryMax=384M` — caps RSS so a slow memory leak can't OOM the Pi.
* `StandardOutput/StandardError=journal` — logs now appear in
  `journalctl -u dmr-monitor` for easy operator inspection.
* Comment added warning that `--calls-dir /tmp/dmr_calls` is wiped on
  reboot; operators who need persistent recordings should point it at
  `/var/lib/dmr-monitor/calls` or similar.

## [0.15.0] — 2026-05-17

### Added — event retention (bounded working set, scales for the long haul)

Until now the on-disk event store grew without bound: the JSONL was
append-only forever and the SQLite index just kept inserting. On this
operator's Pi after a week of uptime that meant ``events.jsonl`` =
1.3 GB and ``events.db`` = 2.3 GB with 5.4 M rows, which (a) made
heavy queries slow even with the index, (b) made every restart
re-open a multi-GB DB, and (c) was hours away from filling the
remaining SD-card free space.

The fix is a new background pruner that drops everything older than
a configurable window. Default is **0 (disabled)** so existing
deployments don't change behaviour; the project's ``dmr-monitor.service``
unit ships with ``--event-retention-hours 48`` for the deployed Pi.

**Mechanics**

1. New ``EventIndex.prune_older_than(cutoff, chunk_size=50_000)`` does a
   chunked ``DELETE FROM events WHERE rowid IN (SELECT rowid … LIMIT
   ?)`` loop so the write transaction never locks the DB for minutes
   when the first prune has millions of rows to remove. After each
   chunk the WAL commits, giving readers a chance to interleave.
2. New ``EventIndex.vacuum()`` reclaims the freed pages — invoked once
   per prune cycle whenever any rows were removed. The first VACUUM
   after the big initial delete is slow (multi-GB → tens of MB), but
   it runs once; subsequent VACUUMs are tiny.
3. New ``EventLog.prune_older_than(cutoff)`` orchestrates both layers
   and rewrites the JSONL in **two phases**:
   * **Phase 1 — lock-free.** Read the JSONL through a fresh handle
     up to its current EOF and write the survivors to ``events.jsonl.new``.
     Concurrent ``append()`` calls keep going to the original file with
     no contention, so the asyncio loop stays responsive even though
     we're walking a 1 GB file.
   * **Phase 2 — brief lock-hold.** Take the writer lock, append any
     tail bytes that arrived during phase 1 (guaranteed newer than the
     cutoff because they were just appended), atomic-rename the new
     file over the old, and reopen the writer. Typical lock-hold time
     is well under 100 ms.
4. New ``--event-retention-hours N`` CLI flag and matching background
   task ``_periodic_event_retention`` that runs once an hour. The
   *first* pass is deferred by 5 minutes after boot so the dashboard
   warms up before the (potentially multi-GB) first rewrite starts.
   Each pass executes on a worker thread via
   ``asyncio.to_thread``, so HTTP / WS keep flowing through the
   entire chunked DELETE + VACUUM + JSONL rewrite.
5. Successful passes log a single line to stderr / journal:
   ``# event-retention: pruned <N> DB rows, kept <M> JSONL lines,
   freed <X.X> MiB (cutoff < …)``.

**Expected effect on the deployed Pi after the first prune at 48 h
retention**

- ``events.db``: 2.3 GB → ~50 MB (data) → ~50 MB on disk after VACUUM
- ``events.jsonl``: 1.3 GB → ~30 MB
- Service restart re-open of the index: from tens of seconds → instant
- ``/api/network`` and ``/api/radio/{id}`` on a 24 h window:
  materialise tens of thousands of rows instead of half a million

Four new tests cover the prune path:
``test_eventindex_prune_older_than_removes_rows_chunked``,
``test_eventindex_prune_idempotent_when_nothing_to_remove``,
``test_eventlog_prune_rewrites_jsonl_and_keeps_writer_open``,
``test_eventlog_prune_preserves_concurrent_appends``.

### Fixed — `test_network_endpoint_smoke` was wall-clock-sensitive

The test seeded events at a fixed ``datetime(2026, 5, 10, ...)`` baseline
and asked the live endpoint for a 7-day network graph. Once real time
moved past 2026-05-17 the events fell outside ``now − 7 d`` and the
test started failing. Anchored ``_ts`` on ``datetime.now() − 1 h`` so
the synthetic events always land inside the window the endpoint will
accept.

## [0.14.5] — 2026-05-17

### Added — in-process dsd-fme respawn (no more visible "crashes" every ~30 min)

The watchdog in ``stream_subprocess`` (added in v0.12.0) is intentional —
when dsd-fme produces no stderr output for ``liveness_timeout`` seconds
(60 s by default) the child is killed and a ``RuntimeError`` is raised.
That signal was meant for systemd's ``Restart=on-failure`` to bring
the service back up, and from a process-lifecycle POV it works fine.

But on this operator's site the watchdog fires roughly every 30 minutes
because the PulseAudio source + dsd-fme combination genuinely stalls
that often. Each fire means: full Python process exits, systemd waits
``RestartSec=5s``, the new process boots, re-opens the SQLite index
(5 M+ rows), restores state from ``snapshot.json``, re-binds the HTTP
socket. From the dashboard's POV that's 5–10 s of "site is down" every
half hour. Operationally unacceptable for live monitoring.

**New ``stream_subprocess_with_retry``** in ``backend/wrapper.py``
wraps the existing ``stream_subprocess``: when the inner generator
raises (watchdog fires) or returns (clean EOF — child exited), we log
the reason, sleep ``backoff_seconds`` (default 2 s), and respawn the
child. The yielded line stream is continuous from the caller's POV.

Net effect:
- Python process stays up across dsd-fme stalls — uvicorn keeps
  serving, ``/ws`` clients stay connected, the event index stays
  warm, snapshots keep being written.
- Live event stream has a ~``liveness_timeout + backoff_seconds`` gap
  (≈ 62 s with the defaults) but no visible restart, no "site is
  down" outage.
- ``stop_event`` is still honoured before every retry attempt, so
  clean shutdown via SIGTERM behaves exactly as before.
- If dsd-fme is genuinely broken (binary missing, persistent crash
  loop), it just keeps respawning every 62 s — operator still sees
  the empty event feed and the recorded errors in the journal, but
  the dashboard itself stays up so they can investigate.

``backend/cli.py`` now calls ``stream_subprocess_with_retry`` instead
of ``stream_subprocess`` for the ``--live`` path. The CLI flag
``--liveness-timeout`` and its default (60 s) are unchanged — the
watchdog still fires at the same cadence, it just no longer kills
the whole service.

Two new tests cover the retry path: stall→respawn→fresh-output, and
stop_event honoured mid-stream.

## [0.14.4] — 2026-05-17

### Fixed — six lurking crash / freeze risks found in a full backend audit
After fixing the v0.14.3 ``UnboundLocalError``, a follow-up sweep across
``backend/`` looked for anything else that could either hard-crash the
process or starve the asyncio loop long enough for the dashboard to
feel frozen. Six items were addressed:

1. **Heavy endpoints off the event loop.** Following the v0.14.2
   pattern for ``/api/network`` and ``/api/radio/{id}``, the remaining
   synchronous endpoints — ``/api/stats``, ``/api/quality``,
   ``/api/history`` — are now dispatched via ``asyncio.to_thread``.
   ``event_stats`` iterates the in-memory ring under a lock,
   ``quality_window`` can fall through to a full JSONL scan on a
   cold-start day, and ``history`` materialises up to 20 000 JSON-
   decoded dicts per request. ``/api/history.csv`` already streams via
   a regular generator (Starlette ``StreamingResponse`` runs those in
   its threadpool), so it was left as-is.
2. **`evaluator.tick()` off the event loop.** Every snapshot tick
   (~1 s) the time-based alert rules run. With a ``QualitySpikeRule``
   enabled, ``tick()`` issues three SQLite queries against the index;
   on a Pi those can stall for tens of ms under writer contention.
   Now wrapped in ``asyncio.to_thread`` from
   ``cli.py::_periodic_snapshot``.
3. **Recordings off the event loop.** ``/api/recordings`` is polled
   every 3 s by the dashboard and calls
   ``RecordingRegistry.list_recent()``, which iterdir's the calls dir
   and opens every WAV header. After a week of uptime that's a sync
   scan of thousands of files on the loop. Same for ``/api/debug``'s
   stat-everything pass and the hourly
   ``_periodic_wav_retention.prune_older_than()``. All three are now
   ``asyncio.to_thread``'d.
4. **Race on `Evaluator.subscribers`.** The set was mutated without
   ``self._lock`` from ``subscribe()`` / ``unsubscribe()`` (asyncio
   thread, via the WS handlers) and from ``_record()`` 's
   dead-queue eviction (parser thread). Under contention this could
   raise ``RuntimeError: Set changed size during iteration`` or
   silently drop a fresh subscriber's first firing. All mutations now
   take ``self._lock``, and ``_record`` collects dead queues into a
   list and evicts them in a single critical section.
5. **Uvicorn server task exception surfacing.** ``asyncio.create_task
   (uv_server.serve())`` had no reference retained and no done
   callback, so any unhandled exception in uvicorn (port-in-use,
   socket error, internal bug) was swallowed when the task was GC'd
   — the dashboard would go dark while the rest of the process kept
   writing snapshots forever, with no operator signal. Now retains
   the task and registers a callback that prints the exception to
   stderr and sets ``stop_event`` so systemd can restart cleanly.
6. **`state.tick()` defensive guard.** Wrapped in try/except in
   ``_periodic_snapshot`` so any future regression in
   ``_expire_idle_calls`` can't kill the snapshot loop and freeze the
   dashboard.

The audit also confirmed there are *no other* compound-assignment-on-
bare-global traps anywhere in ``backend/`` — the v0.14.3 bug was
unique. File-descriptor, subprocess, and WebSocket cleanup paths all
release their resources on every exit path.

## [0.14.3] — 2026-05-17

### Fixed — UnboundLocalError that turned every clean shutdown into a real crash
- **`backend/server.py::push_snapshot`** used the compound assignment
  ``_subscribers -= dead`` to evict WebSocket clients whose queue had
  filled up. Python treats *any* assignment to a bare name as a local
  binding, so the earlier read ``if _state is None or not
  _subscribers:`` in the same function would raise
  ``UnboundLocalError`` the moment that path was taken — i.e. the
  first time a slow/stuck WS client triggered ``QueueFull``. Replaced
  with an in-place ``_subscribers.difference_update(dead)`` (pure
  method call, no rebinding) and added a comment so nobody repeats the
  trick.

  In practice this surfaced during the *intentional* watchdog-driven
  restart: ``stream_subprocess`` raises ``RuntimeError("subprocess
  liveness timeout …")`` after 60 s with no output from ``dsd-fme``;
  the periodic snapshot loop then tries one last
  ``push_snapshot()`` while shutting down, hits ``QueueFull`` on the
  stuck client's queue, and instead of cleanly draining trips the
  ``UnboundLocalError``. Result: what should have been a graceful
  restart was logged as ``code=exited, status=1/FAILURE`` and felt to
  the operator like a crash with no explanation. With this fix the
  shutdown path stays clean; systemd's auto-restart still kicks in
  but the dashboard recovers within seconds instead of looking like
  it died.

## [0.14.2] — 2026-05-17

### Fixed — dashboard freezing on big windows (production crash)
- **`/api/network` and `/api/radio/{id}` no longer block the event
  loop.** Both endpoints called fully-synchronous aggregations
  (`compute_talker_pairs`, `build_dossier`) directly from `async def`
  handlers. On a busy 24 h window this could take many seconds; while
  the work ran, every other coroutine — including the live
  snapshot-broadcast WebSocket — was starved. The dashboard appeared to
  "freeze" and on at least one Pi the systemd watchdog/health check
  killed the process and re-spawned it (`restart counter at 2`).
  The two heavy calls are now off-loaded with
  `asyncio.to_thread(...)`, so the event loop stays responsive and the
  live feed keeps streaming even while the heavy query is running.
- **Row materialisation hard cap.** Both `network.py` and
  `dossier.py` previously asked the SQLite index for `limit=10_000_000`
  rows when accumulating the window — an effective "no limit". On a
  4 GB Pi with a 2.3 GB events DB this had room to OOM if traffic ever
  spiked. Capped at `500_000` rows per query (≈120 MB of payload at
  current row size) and documented the cap with a comment in
  `network.py`. The cap is well above the steady-state workload, so
  it's invisible in normal use but stops a runaway window from
  exhausting the Pi.

## [0.14.1] — 2026-05-17

### Fixed — self-review polish on the v0.14.0 a11y pack
- **`index.html`** `:focus-visible` rule dropped its `border-radius: 2px`
  declaration. The intent was to round the *outline*, but the rule was
  actually rounding the *element* itself — meaning every focused
  rectangular button/select briefly grew rounded corners on tab. The
  outline already follows whatever radius the element has on its own.
- **`index.html`** "Play recording" button now flips its `aria-label`
  between `"Play recording"` and `"Pause recording"` to match the
  glyph (`▶` / `⏸`). The previous fixed label misled screen readers
  whenever a recording was playing.

## [0.14.0] — 2026-05-17

### Fixed — UI bugs (second cosmetic pass)
- **`stats.html`** — Hourly-activity chart: `precision: 0` was being
  applied to the X axis (string labels) so the value axis could render
  fractional tick labels like `1.5`. Moved the integer-ticks override to
  the Y axis where the numeric data now lives after the `indexAxis: 'x'`
  flip.
- **`index.html`** Dossier panel — `top_co_talkers[].shared_tgs` is
  optional in the API but the chip-tooltip code dereferenced
  `.length` unconditionally, throwing for any co-talker that lacked the
  field. Now guarded with `Array.isArray` + safe defaults.
- **`index.html`** Alerts toast bar — when more than 200 firings had
  been seen, the de-dupe set was *fully cleared* (`Set.clear()`),
  meaning replayed firings could re-toast. Switched to FIFO eviction so
  the most recent 200 keys are always remembered.
- **`index.html`** Recordings list — `playRecording(filename)` was
  passed via inline `onclick` with single-quote escaping only. Any
  filename containing `<`, `>`, `"`, or a backslash would have broken
  the markup (and provided an XSS surface). Now the button binds via
  `addEventListener` and captures the filename through closure — zero
  string interpolation into HTML.
- **`alerts.html`** — Alerts WS reconnect had a fixed 2 s retry on
  every disconnect, unlike `index.html` which already uses
  exponential backoff up to 16 s. Aligned with the same backoff
  policy so a flapping server isn't hammered.
- **`alerts.html`** — `cooldown_seconds` is clamped to `>= 0` before
  POST so a user can't slip a negative value past the `min="0"` UI
  hint via DOM tinkering.
- **`index.html`** Dossier panel — the embedded Leaflet map
  (`_dossierMap`) was only torn down on the next `openDossier()`
  call, leaking tile listeners between sessions. Now released in
  `closeDossier()`.
- **`index.html`** LSN-chip class — chips for states other than
  `Active` / `Rest` were assigned a class string with a trailing
  space (`"lsn-chip "`). Harmless visually but it broke
  `classList.contains('lsn-chip')` if any caller ever tried to
  query it. Cleaned up.

### Added — small a11y / operator polish (design-council pack)
- **`index.html`** — `Escape` now closes the Dossier slide-in panel
  (previously keyboard users had to mouse to the "close" button or the
  backdrop). Focus is restored to the element that opened the panel,
  and `aria-hidden` is toggled.
- **`index.html`** — Global `:focus-visible` ring (yellow, matches the
  existing "selected row" accent) so keyboard tabbers can see where
  they are without intruding on mouse users.
- **`index.html`** — `prefers-reduced-motion: reduce` honored for the
  toast slide-in and the Dossier panel transitions.
- **`index.html`** — Toast bar is now a `role="status"` /
  `aria-live="polite"` region so screen readers announce each new
  alert without any visual change.
- **`index.html`** — Footer now shows a `Uptime: HH:MM:SS` clock for
  the current dashboard session, ticking every second. Lets the
  operator correlate "how long have I been on this watch?" against the
  buffered event count already in the same row.

## [0.13.0] — 2026-05-16

### Added — Alerts Engine
- **`backend/alerts.py`** — rule-based notifications driven by the live
  event stream. Four rule kinds (discriminated union, pydantic):
  * `radio_keyup` — fires when a watched radio id starts a new voice
    call (per-call, not per-frame; tracks slot/src/tgt to suppress
    duplicates).
  * `encryption` — fires on any `EncryptionEvent`, optionally filtered
    by talkgroup. Joins against the per-slot active call so the
    notification can carry SRC + TG.
  * `cc_silent` — fires once when no `SiteInfoEvent` /
    `ChannelStatusEvent` / `LSNStatusEvent` has been seen for the
    configured number of seconds; re-arms when CC traffic resumes.
  * `quality_spike` — fires when the overall CRC error rate over a
    rolling window crosses a threshold (reuses
    `compute_quality_ratios`).
  Every rule has a `cooldown_seconds` to prevent flooding.
- **`Evaluator`** — threadsafe holder for the active rule set. Hooks:
  `on_event(ev)` for stream-driven rules, `tick(now)` for time-based
  rules. Both wired from `cli.py` (the printer chain feeds events; the
  periodic snapshot loop calls `tick`).
- **Rule persistence**: rules round-trip through `alerts.json` via the
  atomic write helper. A corrupt file is renamed aside (`.bad`) so a
  hand-edit typo can't take the service down. New
  `--alerts-rules PATH` CLI flag (empty string disables).
- **REST API**:
  * `GET    /api/alerts/rules`              — list rules
  * `POST   /api/alerts/rules`              — add rule (validates kind)
  * `DELETE /api/alerts/rules/{id}`         — remove rule
  * `POST   /api/alerts/rules/{id}/toggle`  — enable / disable
  * `GET    /api/alerts/recent?limit=N`     — last N firings (in-memory)
- **`WS /ws/alerts`** — push channel for `AlertFiring` JSON. Sends the
  last 5 firings on connect so a freshly-loaded UI shows context.
- **Toast bar** on the Live dashboard — auto-subscribes to
  `/ws/alerts`, renders each firing as a colored toast (top-right,
  auto-dismiss after 12 s, color by rule kind). Requests browser
  Notification permission once so alerts surface even when the tab is
  backgrounded.
- **`/alerts` page** for rule management — kind-specific form
  (radio IDs / TG IDs / silent timeout / window+rate), enabled toggle,
  delete, live recent-firings panel.
- `Alerts` link added to the nav on every page.

## [0.12.0] — 2026-05-16

### Added — Foundation block ("walk-away-safe on a Pi")
- **`GET /api/health`** — stable JSON snapshot of the running process:
  `version`, `build_date`, `uptime_seconds`, `last_event_age_seconds`,
  `last_voice_age_seconds`, file sizes (`events.jsonl`, `events.db`,
  `snapshot.json`), free disk on the JSONL filesystem, and a
  `calls_dir` summary (`count`, `total_bytes`, `oldest_age_seconds`).
  Designed for cron `curl` / healthchecks.io / prometheus-textfile
  watchdogs — every probe degrades to `null` on failure rather than
  taking the endpoint down. New `backend/health.py` module.
- **dsd-fme liveness watchdog** in `stream_subprocess`: when
  `--liveness-timeout` seconds pass with no stderr output (default 60s),
  the child is terminated and the process exits non-zero so systemd's
  `Restart=on-failure` brings the service back. Catches "PulseAudio
  dropped" / "SDRconnect crashed" / "USB power dipped" silent stalls
  that `Restart=on-failure` alone wouldn't notice. `0` disables.
- **WAV retention janitor** — hourly background task in CLI deletes
  per-call WAVs in `--calls-dir` older than `--wav-retention-hours`
  (default 72 h). Otherwise dsd-fme's per-call output fills the SD
  card in a few busy days. `0` disables. New `prune_older_than(hours)`
  on `RecordingRegistry`.
- `note_voice_event()` hook + `attach_snapshot_path()` on the FastAPI
  server so `/api/health` can answer "are radios actually talking,
  not just is the CC alive" and report the snapshot file's size.

### Changed
- **Snapshot writes are now atomic.** `cli.py`'s periodic and final
  snapshot writes go through `atomic_write_text` (write to `.tmp`,
  `fsync`, `os.replace`) so a power-yank mid-write can't leave
  `snapshot.json` truncated. The previous file is preserved as
  `snapshot.json.bak`.
- `StateManager.load_snapshot` now falls back to `snapshot.json.bak`
  if the main file is missing or unparseable — covers the case where
  a power-yank corrupted the current snapshot but the previous one is
  still good.
- New CLI flags: `--liveness-timeout SECONDS`, `--wav-retention-hours HOURS`.

## [0.11.1] — 2026-05-16

### Fixed
- **Dossier for target-only radios**: a radio that appeared only as the
  TGT of private events (data headers / LRRP requests addressed to it)
  passed the existence check but came back with `first_seen=None`,
  `last_seen=None` and an empty hourly histogram, because the lifetime
  pass only queried `src=radio_id`. The hourly histogram and lifetime
  bounds now union the src-side and tgt-side rows (de-duped).
- **Dossier recordings scan**: `_attach_recording` was calling
  `recordings.list_recent()` once per call session (up to 20×),
  rescanning the WAV directory and parsing every header each time.
  Snapshot the list once and re-use across sessions.
- **Per-call audio src in the Dossier panel** now `encodeURIComponent`s
  the WAV filename, matching the live-history button — filenames with
  spaces / `+` / `&` no longer break the `<audio>` URL.
- **Subprocess shutdown**: the SIGTERM-on-stop watcher task in
  `stream_subprocess` is now awaited after cancellation so asyncio
  doesn't emit "Task was destroyed but it is pending".

### Added
- `CLAUDE.md` with the project's per-change rules: every commit bumps
  `__version__` / `__build_date__` and prepends a `CHANGELOG.md` entry.

## [0.11.0] — 2026-05-16

### Added
- **Per-radio Dossier** as a slide-in side panel on the dashboard. Click
  the `D` button on any radio row (or visit `/?dossier=<id>`) to slide in
  a 440px panel from the right with:
  * lifetime stats (first/last seen, total calls, encryption events on
    used slots)
  * Top Co-talkers chips (clicking a chip re-renders the panel for that
    radio in place)
  * Talkgroups touched, with counts
  * 24-bar hourly activity histogram
  * Position track on Leaflet (start/latest markers + polyline)
  * Recent calls (up to 20) with `<audio>` players when a WAV is matched
    in `RecordingRegistry` (matched by src/tgt and ±5 s of start)
- `GET /api/radio/{radio_id}?window=SECONDS` — returns the dossier JSON
  or 404 if the radio is unknown in the window.
- `backend/dossier.py` with `build_dossier(index, radio_id, window, …)` —
  collapses voice_call frames into call sessions (PTT gap > 2 s starts a
  new session) and reuses `compute_talker_pairs()` for co-talker math.
- SRC cells on the Debrief table are now click-through links that
  `/?dossier=<src>` deep-link into the panel.
- Cytoscape nodes on `/network` deep-link to `/?dossier=<id>` (the
  forward-declaration from v0.10.0 is now live).

## [0.10.0] — 2026-05-16

### Added
- **Talker-Pair Graph** at `/network` — interactive cytoscape.js graph of
  which radios talk to which. Two edge kinds, coloured separately:
  * **group co-presence** (blue) — two radios that keyed up on the same TG
    or sent group CSBKs to it. Weight is the sum over shared TGs of
    `min(count_a_on_tg, count_b_on_tg)`, which rewards mutual participation.
  * **private direct** (orange, dashed) — Individual CSBKs, Individual data
    headers, and LRRP requests. Weight collapses both directions.
- `GET /api/network?window=SECONDS&min_weight=N&limit=N` — JSON
  `{nodes, edges, window_seconds, generated_at}`. Nodes carry
  `total_calls`, `last_seen`, `has_gps`; edges carry `weight`, `kind`,
  shared `tgs`. `src=0` (DSD-FME pre-LC placeholder) is filtered out.
- `backend/network.py` with `compute_talker_pairs(index, …)` — uses the
  SQLite sidecar from v0.9.0, so a graph render is one indexed SELECT.
- Window dropdown (5m / 15m / 1h / 6h / 24h) and min-weight slider on
  the page; the right-side panel lists the top 25 edges as a non-graph
  fallback. Clicking a node deep-links to `/?dossier=<id>` (Phase 3
  forward-declared).
- `/network` link added to the nav on every page.

## [0.9.0] — 2026-05-16

### Added
- **SQLite sidecar index** over the events JSONL. Every `EventLog.append()`
  now dual-writes the event to both the JSONL (canonical, append-only) and
  a parallel SQLite database with indexes on `(ts, src, tgt, type)`.
  `/api/history`, `/api/history.csv`, and `/api/quality` automatically use
  the index when present and fall back to scanning the JSONL otherwise —
  no API shape changes.
- `schema_version` field on every event (`EVENT_SCHEMA_VERSION = 1`). Bumped
  only on backwards-incompatible changes; the index records the value at
  build time and surfaces `index_outdated` if a newer-versioned event lands
  in an older index.
- `--event-db PATH` CLI flag (defaults to `--event-log` with `.db` suffix).
- `--no-event-db` CLI flag to disable indexing entirely (JSONL-only mode).
- `--rebuild-index` CLI mode — drops and replays the JSONL into a fresh
  SQLite database, prints row count, and exits.
- Startup banner reports the event index row count and database path.

### Design notes
- JSONL remains the **source of truth**. SQLite is rebuildable from it.
- Writes are batched (every 100 inserts or 2 s, whichever first) — worst-case
  data loss on power yank is ~2 s of *index* rows, recoverable by
  `--rebuild-index`.
- Opened in WAL mode with `synchronous=NORMAL` and `check_same_thread=False`
  + internal lock, so the parser thread and FastAPI workers can share it.
- SQLite append failures are logged once per session and swallowed — the
  index must never break the live monitor.

## [0.8.0] — 2026-05-15

### Added
- **State persistence across restart** — `StateManager.load_snapshot()`
  restores radios, IPs, GPS positions, and lifetime quality counters
  from `snapshot.json` at startup. Active calls are intentionally not
  restored (they expired during downtime).
- **Event buffer priming** — `EventLog.prime_from_jsonl()` refills the
  in-memory ring buffer from the persisted JSONL on startup, so the
  Debrief panel doesn't go blank for the first few minutes after a
  service restart.
- **Quality window selector** on `/stats` — dropdown to pick the time
  window (5m / 15m / 1h / 6h / 24h, default 1h). Quality ratios now
  show the exact sample period and event count, so it's obvious what
  "1.5%" was averaged over.
- `GET /api/quality?window=SECONDS` — new endpoint that computes
  quality ratios over a real time window from the on-disk JSONL,
  independent of the in-memory ring buffer size.

### Changed
- Quality Analyzer card now sources its data from `/api/quality`
  (window-aware, from disk) rather than `/api/stats` (buffer-aware,
  from memory). This fixes the "1.5% never changes" feel — the rate
  now reflects the chosen rolling window honestly.

## [0.7.0] — 2026-05-15

### Added
- **Quality Analyzer card** on `/stats` — computes real CRC/FEC error
  *ratios* per channel class (CSBK / CACH / SLCO) so you can tell at a
  glance whether the SNR is healthy. Big colour-coded overall rate +
  verdict (excellent / good / marginal / poor / unusable) with a
  tuning hint. Per-channel table breaks down errors / decoded count /
  rate, with a heat-bar that fills as the rate climbs.
- `compute_quality_ratios()` in `event_log.py` — pure function that
  takes counters and returns the JSON shape the UI renders. The
  denominators are the count of *successfully decoded* frames of the
  same class (CSBK successes for CSBK errors, voice frames for SLCO
  CRC, etc.), so the ratio is meaningful regardless of how busy the
  channel is.
- `/api/stats` response now includes a `quality_ratios` block.

### Changed
- The big stats grid auto-fits, but the new Quality Analyzer card
  spans 2 columns so it's the visual focal point on `/stats`.

## [0.6.0] — 2026-05-15

### Added
- **Stats page (`/stats`)** — Chart.js dashboard fed by `/api/stats`:
  doughnut of events_by_type, top-15 talkers, talkgroup distribution,
  hourly histogram, plus key/value panels for quality + GPS counts.
  Auto-refreshes every 5s.
- **Debrief browser (`/debrief`)** — full-page event explorer that
  reads from the on-disk JSONL (not just the in-memory ring buffer),
  so it can query historical events from previous sessions.
  Filters: time range, SRC radio, target/TG, event types, page size.
  Streaming CSV export of the current filter slice. Pagination via
  offset.
- `GET /api/history?since=&until=&src=&tgt=&types=&limit=&offset=` —
  JSON API behind the debrief browser
- `GET /api/history.csv` — CSV export of the same slice
- `stream_history()` and `iter_history_csv()` in `event_log.py`
- Nav links (Live / Debrief / Stats) on every page

### Changed
- Refactored CSV row generation around `_row_from_dict` so the live and
  historical exports share a single column-mapping implementation

## [0.5.1] — 2026-05-15

### Fixed
- Debrief / Recent / Active rows collapsed to ~1px tall when the list
  filled up. Added `flex-shrink: 0` to the row elements so the flex
  column overflows and scrolls instead of squashing rows together.

## [0.5.0] — 2026-05-15

### Added
- `EventLog` — in-memory ring buffer + append-only JSONL persistence for
  every parsed event (`backend/event_log.py`)
- `GET /api/events.csv` — streaming CSV download with `?since=` and
  `?types=` filters; "⤓ Export CSV" button in the dashboard header
- `GET /api/events` — recent events feed for the Debrief panel
- `GET /api/stats` — distributions for the future statistics panel
- `GET /api/version` — `{version, build_date}` endpoint; version tag in
  the dashboard footer
- "Show: last 1m/5m/15m/1h/All" filter — hides stale radios from the map
  and table, drops stale points from the trail
- Debrief panel in the left column with per-type filter (default
  "traffic" preset — voice + positions + encryption + errors)
- Recent Voice Calls filters — by SRC radio and minimum duration
  (default ≥2s) to skip 1-second SNR-noise calls
- CLI: `--event-log PATH` (default `events.jsonl`), `--event-buffer N`,
  `--version`

### Changed
- Left column widened 280px → 320px
- Active Calls panel capped at 180px tall so the panels below stay
  visible
- Debrief font 10px → 12px, with descriptions for control-channel event
  types and a `raw_line` fallback

## Pre-0.5.0

Pre-versioning history is in the git log (phases 1A → 4b).
