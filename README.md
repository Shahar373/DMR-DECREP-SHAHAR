# DMR Cap+ Monitor

Live monitoring of a Motorola Capacity Plus DMR system: who's talking, where
they are, and what's happening on the trunked control / payload channels.

Pipeline:
```
SDR (RSP1B) → SDRconnect → PulseAudio loopback → dsd-fme → parser → state → UI
```

## Beta status (Phase 3)

| Phase | Status | What works |
|---|---|---|
| 1A | done | Parser for Cap+ control channel events |
| 1B | done | Parser for payload-channel events (voice w/ SRC, LRRP GPS, IP map, encryption) |
| 2  | done | StateManager — radios, active calls, system, quality |
| 3  | **beta** | CLI runner: live (`dsd-fme` subprocess) or replay (captured log) |
| 4a | next | FastAPI + WebSocket + browser UI (map + table) |
| 4b | next | Audio streaming via ffmpeg → Icecast |

## Install (Raspberry Pi 5)

Prereqs: `python3.11+`, `dsd-fme` built and on PATH (see
`scripts/check_env.sh` for the full deps list).

```bash
cd ~/DMR-DECREP-SHAHAR
git pull origin claude/dmr-monitoring-dashboard-00IUG
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/pytest tests/        # 60/60 should pass
```

To update later just `git pull && .venv/bin/pip install -r requirements.txt`.

## Smoke-test (no RF needed)

Replay the bundled real-capture sample through the full pipeline:

```bash
.venv/bin/python -m backend.cli --replay tests/captures/dmr_night_sample.log
```

You should see lines like:
```
[09:00:20] site     id=2  rest_lsn=3
[09:00:20] voice    slot2 SRC=2102   TGT=1      Conv
[09:00:51] position radio=68  (32.10332, 34.87087)
[09:05:02] preamble Individual/Data  SRC=65 -> 64250 rest_lsn=3
[09:05:02] data-hdr slot2 SRC=65 -> 64250 Unconfirmed Delivery
```

…and a final summary with discovered radios + their last positions. A
`snapshot.json` file is also written and refreshed every second — Phase 4
will replace this with a WebSocket feed.

## Run live (with RF)

```bash
.venv/bin/python -m backend.cli --live \
    --input pulse:dmr_capture.monitor \
    --audio-out /tmp/dmr_audio.wav \
    --snapshot /var/tmp/dmr_snapshot.json
```

Then in another terminal:
```bash
watch -n1 'jq ".radios | length, .active_calls" /var/tmp/dmr_snapshot.json'
```

Ctrl+C stops dsd-fme cleanly and writes a final snapshot.

## CLI options

```
--live                      spawn dsd-fme and stream live
--replay FILE               replay a captured dsd-fme stderr log
--input DEV                 dsd-fme -i input device (live mode)
--audio-out PATH            dsd-fme -o WAV output path (live mode)
--dsd-bin PATH              path to dsd-fme binary
--snapshot PATH             where to write periodic JSON state
--snapshot-interval SEC     snapshot write interval
--replay-delay SEC          per-line sleep in replay mode (simulate live timing)
--verbose                   print every parsed event, including CC heartbeats
```

## Layout

```
backend/
  models.py        # pydantic event types
  parser.py        # line → typed event
  state.py         # event stream → DashboardSnapshot
  wrapper.py       # async LineRunner + subprocess / file sources
  cli.py           # `python -m backend.cli`
tests/
  captures/       # gitignored except small *_sample.log files
  test_parser.py  test_state_manager.py  test_wrapper.py
scripts/
  check_env.sh    # one-shot environment check on the Pi
```
