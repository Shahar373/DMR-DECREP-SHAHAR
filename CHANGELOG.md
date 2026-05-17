# Changelog

Versioning follows [Semantic Versioning](https://semver.org/):

* **patch** — bug fix, internal cleanup, no UI/API change
* **minor** — new feature, backwards-compatible
* **major** — breaking change to CLI / HTTP API / event schema

Source of truth: `backend/__init__.py` (`__version__`). The dashboard
footer shows the running build's version and `/api/version` exposes it.

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
