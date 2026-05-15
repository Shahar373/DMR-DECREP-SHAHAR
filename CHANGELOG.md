# Changelog

Versioning follows [Semantic Versioning](https://semver.org/):

* **patch** — bug fix, internal cleanup, no UI/API change
* **minor** — new feature, backwards-compatible
* **major** — breaking change to CLI / HTTP API / event schema

Source of truth: `backend/__init__.py` (`__version__`). The dashboard
footer shows the running build's version and `/api/version` exposes it.

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
