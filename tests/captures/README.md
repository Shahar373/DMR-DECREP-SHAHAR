# tests/captures

Real DSD-FME stderr captures for regression / replay tests.

## What's here

| File | Source | Notes |
|---|---|---|
| `dmr_night_sample.log` | 168.500 MHz, Site 2, 2026-05-09 night | ~5,300 lines (~400 KB) slice from a 30 MB capture. Contains every Phase 1A and 1B event type. The slice was chosen for density: starts around the first LRRP fix and includes a 300-line tail around the first Protected LC event so the encryption parser is exercised. |

## Captures are gitignored by size

Full multi-megabyte captures are **not** committed. The `.gitignore` in this
folder ships only the small representative `*_sample.log` files. Drop large
local recordings here freely — they'll be ignored.

## Adding new sample slices

When extending the parser for new event types, grab a representative slice:

```bash
# Find an interesting line (e.g. the new pattern you're adding)
grep -n '<pattern>' big_capture.log | head -3

# Cut a ~5000-line window around it
sed -n '<start>,<end>p' big_capture.log > tests/captures/<name>_sample.log
```

Then re-run the replay test to confirm the new event type fires:
```bash
.venv/bin/pytest tests/test_parser.py::test_sample_log_parses_with_known_event_types_only -v
```

## Privacy / legal

These captures contain real radio IDs and GPS coordinates from a live Cap+
system the project owner has authorization to monitor. They are kept only
because the parser regressions hinge on exact byte-level format. Do not share
this folder outside the project.
