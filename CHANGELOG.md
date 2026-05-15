# Changelog

Versioning follows [Semantic Versioning](https://semver.org/):

* **patch** — bug fix, internal cleanup, no UI/API change
* **minor** — new feature, backwards-compatible
* **major** — breaking change to CLI / HTTP API / event schema

Source of truth: `backend/__init__.py` (`__version__`). The dashboard
footer shows the running build's version and `/api/version` exposes it.

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
