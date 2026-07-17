# DMR Cap+ Monitor

Live monitoring of a Motorola Capacity Plus DMR system: who's talking, where
they are, and what's happening on the trunked control / payload channels.

Pipeline (two RF backends):
```
# --rf-backend soapy (default going forward): direct SDR control, fully automatic
SDR (RSP1B) → SDRplay API service → SoapySDR → dsd-fme → parser → state → UI

# --rf-backend pulse (legacy): manual SDRconnect + virtual audio cable
SDR (RSP1B) → SDRconnect (GUI, manual tune) → PulseAudio loopback → dsd-fme → …
```

The `soapy` backend lets `dsd-monitor` tune the RSP1B itself
(`--frequency`), so there's no SDRconnect GUI and no virtual cable to set
up. Run `bash scripts/setup-sdrplay.sh` once to install the chain, then:

```bash
python -m backend.cli --live --rf-backend soapy --frequency 168.5M --serve
```

### Multi-frequency capture (one RSP, several channels at once)

If the site's Cap+ channels all fit within one RSP's ~10 MHz, a single
wideband capture is channelized in software and each channel is decoded
in parallel. Describe the channels in a JSON plan (see
`backend/channel_plan.py`) and pass `--channel-plan`:

```bash
python -m backend.cli --live --channel-plan site.json --serve
# only decode channels that are actually active (saves CPU):
python -m backend.cli --live --channel-plan site.json --follow-traffic --serve
```

`--follow-traffic` keeps a decoder only on the control channel plus
channels the control-channel grants (or RF energy) show as active — the
system "goes where the traffic is" instead of decoding every channel all
the time. Each event, radio, and call is tagged with its channel.

## Status

| Phase | Status | What works |
|---|---|---|
| 1A | done | Parser for Cap+ control channel events |
| 1B | done | Parser for payload-channel events (voice w/ SRC, LRRP GPS, IP map, encryption) |
| 2  | done | StateManager — radios, active calls, system, quality |
| 3  | done | CLI runner: live (`dsd-fme` subprocess) or replay (captured log) |
| 4a | done | FastAPI + WebSocket + browser UI (live dashboard, debrief, stats, network, alerts) |
| 4b | next | Audio streaming via ffmpeg → Icecast (optional) |

## Install (Raspberry Pi 5, 64-bit Raspberry Pi OS / Bookworm)

```bash
# 1. Clone
git clone https://github.com/Shahar373/DMR-DECREP-SHAHAR.git ~/DMR-DECREP-SHAHAR
cd ~/DMR-DECREP-SHAHAR

# 2. System dependencies (apt packages + dsd-fme build deps + audio utils)
bash scripts/install-deps.sh

# 3. Verify the environment (Python, dsd-fme, audio, CPU architecture)
bash scripts/check_env.sh

# 4. Create the PulseAudio capture sink dsd-fme reads from
bash scripts/setup-pulseaudio.sh

# 5. Python virtualenv + deps + tests
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/pytest tests/        # 177/177 should pass
```

`scripts/install-deps.sh` installs everything `apt` can provide (including
the build dependencies for `dsd-fme`). **`dsd-fme` itself is not in apt** —
build it from source (<https://github.com/lwvmobile/dsd-fme>); the deps are
already installed for you. `scripts/check_env.sh` tells you exactly what is
still missing.

To update later: `git pull origin main && .venv/bin/pip install -r requirements.txt`.

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
    --calls-dir /tmp/dmr_calls \
    --serve --port 8081
```

dsd-fme records each call as a WAV under `--calls-dir` automatically; the
dashboard is served at `http://<pi>:8081/`. (This is the same invocation the
systemd unit in `scripts/dmr-monitor.service` uses.) Port 8081, not 8080:
this repo is now the reference/engine source for a merge into the `DMR`
project, whose `dmr-web.service` is the always-on production app on 8080
when both run on the same Pi.

To watch state without the dashboard, in another terminal:
```bash
watch -n1 'jq ".radios | length, .active_calls" snapshot.json'
```

Ctrl+C stops dsd-fme cleanly and writes a final snapshot.

## CLI options

```
--live                      spawn dsd-fme and stream live
--replay FILE               replay a captured dsd-fme stderr log
--rebuild-index             rebuild the SQLite event index from --event-log and exit
--input DEV                 dsd-fme -i input device (live, default pulse:dmr_capture.monitor)
--dsd-bin PATH              path to dsd-fme binary (default: dsd-fme)
--serve                     start the FastAPI WebSocket server + browser UI
--port N                    HTTP/WebSocket port for --serve (default 8081)
--calls-dir DIR             where dsd-fme writes per-call WAVs (default /tmp/dmr_calls)
--snapshot PATH             where to write periodic JSON state (default snapshot.json)
--snapshot-interval SEC     snapshot write interval (default 1.0)
--event-log PATH            append-only JSONL of every parsed event (default events.jsonl)
--event-db PATH             SQLite index path (default: --event-log with .db suffix)
--no-event-db               disable the SQLite index sidecar (JSONL-only)
--event-retention-hours H   drop events older than H hours (default 0 = keep forever)
--wav-retention-hours H     delete per-call WAVs older than H hours (default 72)
--liveness-timeout SEC      respawn dsd-fme if silent this long (default 60, 0 disables)
--alerts-rules PATH         Alerts Engine rules file (default alerts.json, empty disables)
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
  event_log.py     # append-only JSONL + in-memory ring buffer
  event_index.py   # SQLite index sidecar
  alerts.py        # Alerts Engine (rules → WebSocket alerts)
  health.py  network.py  dossier.py  recordings.py
  server.py        # FastAPI + WebSocket + REST API
  cli.py           # `python -m backend.cli`
frontend/
  index.html  debrief.html  stats.html  network.html  alerts.html
tests/
  captures/        # gitignored except small *_sample.log files
  test_*.py        # 12 test modules, 177 tests
scripts/
  install-deps.sh      # apt deps + audio utils (run first)
  check_env.sh         # environment + CPU-architecture check
  setup-pulseaudio.sh  # create the dmr_capture capture sink
  install-service.sh   # install the systemd --user service
  dmr-monitor.service  # systemd unit template
```
