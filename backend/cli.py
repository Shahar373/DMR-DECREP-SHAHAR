"""DMR Cap+ live monitor — Phase 4a CLI.

Two modes:

  python -m backend.cli --live [--serve]
      Spawn dsd-fme, stream its stderr through parser + state manager, write
      a periodic snapshot to snapshot.json. With --serve also runs a FastAPI
      server (WebSocket + browser UI) on --port (default 8080).

  python -m backend.cli --replay FILE [--serve]
      Same pipeline, but read lines from a captured log instead.

Pass --serve to enable the browser dashboard at http://<host>:<port>/.
"""
from __future__ import annotations

import argparse
import asyncio
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from .models import Event, EventType
from .state import StateManager, atomic_write_text
from .wrapper import LineRunner, stream_file, stream_subprocess_with_retry


# Events that are worth showing live. The control-channel "heartbeat" events
# (channel_status, lsn_status, bank_call) fire dozens of times per second and
# would drown out the interesting traffic; they still update state silently.
_INTERESTING_TYPES = {
    EventType.VOICE_CALL,
    EventType.PREAMBLE_CSBK,
    EventType.DATA_HEADER,
    EventType.IP_MAPPING,
    EventType.LRRP_POSITION,
    EventType.LRRP_REQUEST,
    EventType.ENCRYPTION,
    EventType.SITE_INFO,
    EventType.QUALITY,
}


def _make_event_printer(state: StateManager, verbose: bool):
    """Return a callback that pretty-prints events to stdout.

    De-dupe rules (so the live view doesn't drown in heartbeats):
      * voice_call: print only on new (slot, src, tgt) tuple
      * site_info:  print only when (site, rest_lsn) actually changes
      * SRC=0 voice frames are skipped (DSD-FME pre-LC placeholder)
    """
    last_call_key: dict[int, tuple[int, int]] = {}
    last_site: tuple[Optional[int], Optional[int]] = (None, None)

    def printer(ev: Event) -> None:
        nonlocal last_site
        if not verbose and ev.type not in _INTERESTING_TYPES:
            return
        ts = ev.timestamp.strftime("%H:%M:%S")

        if ev.type == EventType.VOICE_CALL:
            if ev.src == 0:
                return  # placeholder pre-LC SRC
            key = (ev.src, ev.tgt)
            if last_call_key.get(ev.slot) == key:
                return  # already announced this call
            last_call_key[ev.slot] = key
            flavor = "Cap+" if ev.is_cap_plus else "Conv"
            txi = " TXI" if ev.is_txi else ""
            call = state.active_calls.get(ev.slot)
            enc = " [ENC]" if call and call.is_encrypted else ""
            print(f"[{ts}] voice    slot{ev.slot} SRC={ev.src:<6} TGT={ev.tgt:<6} {flavor}{txi}{enc}")

        elif ev.type == EventType.LRRP_POSITION:
            print(f"[{ts}] position radio={ev.src}  ({ev.lat}, {ev.lon})")

        elif ev.type == EventType.PREAMBLE_CSBK:
            rest = f" rest_lsn={ev.rest_lsn}" if ev.rest_lsn is not None else ""
            print(f"[{ts}] preamble {ev.addressing}/{ev.kind:<5} SRC={ev.src:<6} -> {ev.tgt}{rest}")

        elif ev.type == EventType.DATA_HEADER:
            tail = " [response-requested]" if ev.response_requested else ""
            print(f"[{ts}] data-hdr slot{ev.slot} SRC={ev.src:<6} -> {ev.tgt:<6} {ev.delivery}{tail}")

        elif ev.type == EventType.IP_MAPPING:
            print(f"[{ts}] ip-map   {ev.role}  radio={ev.radio_id:<6} ip={ev.ip}:{ev.port}")

        elif ev.type == EventType.LRRP_REQUEST:
            print(f"[{ts}] lrrp-req {ev.direction:<8} SRC={ev.src:<6} -> TGT={ev.tgt}")

        elif ev.type == EventType.ENCRYPTION:
            print(f"[{ts}] crypto   slot{ev.slot} FLCO={ev.flco} FID={ev.fid}")

        elif ev.type == EventType.SITE_INFO:
            site_key = (ev.site, ev.rest_lsn)
            if site_key == last_site:
                return
            last_site = site_key
            print(f"[{ts}] site     id={ev.site}  rest_lsn={ev.rest_lsn}")

        elif ev.type == EventType.QUALITY:
            print(f"[{ts}] err      {ev.error_type}")

        elif verbose:
            print(f"[{ts}] {ev.type.value}")

    return printer


async def _periodic_wav_retention(
    recordings,
    hours: float,
    stop_event: asyncio.Event,
    interval_seconds: float = 3600.0,
) -> None:
    """Hourly background task that deletes per-call WAVs older than the
    retention window. Logs each pass to stderr so the operator sees the
    janitor doing its job."""
    if recordings is None or hours <= 0:
        return
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
            break
        except asyncio.TimeoutError:
            pass
        try:
            # Walks every WAV in calls_dir under stat + unlink. Off-loop so a
            # janitor pass on a directory with thousands of files doesn't stall
            # the snapshot/WS pipeline that shares the same event loop.
            deleted, freed = await asyncio.to_thread(
                recordings.prune_older_than, hours,
            )
            if deleted:
                print(
                    f"# wav-retention: deleted {deleted} files "
                    f"({freed/1024/1024:.1f} MiB) older than {hours}h",
                    file=sys.stderr,
                )
        except Exception as e:  # noqa: BLE001
            print(f"# wav-retention: pass failed: {e}", file=sys.stderr)


async def _periodic_event_retention(
    event_log,
    hours: float,
    stop_event: asyncio.Event,
    interval_seconds: float = 3600.0,
    startup_delay_seconds: float = 300.0,
) -> None:
    """Hourly background task that deletes events older than the retention
    window from both the SQLite index and the on-disk JSONL.

    First pass waits ``startup_delay_seconds`` after boot so the dashboard
    becomes responsive before the (potentially multi-GB) first rewrite
    starts. Each pass runs entirely on a worker thread, so the asyncio
    loop keeps serving HTTP/WS during the slow chunked DELETE + VACUUM +
    JSONL rewrite.
    """
    if event_log is None or hours <= 0:
        return
    # Give the dashboard a few minutes of warm-up before the first big prune.
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=startup_delay_seconds)
        return  # stop signalled during the delay
    except asyncio.TimeoutError:
        pass
    from datetime import timedelta as _td
    while not stop_event.is_set():
        cutoff = datetime.now() - _td(hours=hours)
        try:
            metrics = await asyncio.to_thread(event_log.prune_older_than, cutoff)
            if metrics["db_deleted"] or metrics["jsonl_bytes_freed"]:
                print(
                    f"# event-retention: pruned {metrics['db_deleted']} DB rows, "
                    f"kept {metrics['jsonl_lines_kept']} JSONL lines, freed "
                    f"{metrics['jsonl_bytes_freed']/1024/1024:.1f} MiB "
                    f"(cutoff < {cutoff.isoformat(timespec='seconds')})",
                    file=sys.stderr,
                )
        except Exception as e:  # noqa: BLE001
            print(f"# event-retention: pass failed: {e}", file=sys.stderr)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
            break
        except asyncio.TimeoutError:
            pass


async def _periodic_day_retention(
    event_log,
    days: int,
    stop_event: asyncio.Event,
    interval_seconds: float = 3600.0,
) -> None:
    """Hourly day-granular retention: unlink whole day files older than
    ``days`` local days and DELETE their index rows. Cheap — no JSONL
    rewrite — so no startup delay is needed."""
    if event_log is None or days <= 0:
        return
    from datetime import date as _date, timedelta as _td
    while not stop_event.is_set():
        cutoff_day = (_date.today() - _td(days=days)).isoformat()
        try:
            metrics = await asyncio.to_thread(
                event_log.prune_days_older_than, cutoff_day,
            )
            if metrics["files_deleted"] or metrics["db_deleted"]:
                print(
                    f"# day-retention: dropped {metrics['files_deleted']} day "
                    f"file(s) ({metrics['bytes_freed']/1024/1024:.1f} MiB) and "
                    f"{metrics['db_deleted']} index rows (day < {cutoff_day})",
                    file=sys.stderr,
                )
        except Exception as e:  # noqa: BLE001
            print(f"# day-retention: pass failed: {e}", file=sys.stderr)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
            break
        except asyncio.TimeoutError:
            pass


async def _periodic_fsync(
    event_log,
    stop_event: asyncio.Event,
    interval_seconds: float = 5.0,
) -> None:
    """Force the events JSONL to disk every ``interval_seconds``.

    With ``buffering=1`` the writer puts every newline-terminated event
    into the kernel page cache immediately, but the kernel itself may
    hold those pages for tens of seconds before flushing.  On a sudden
    power loss anything still in the cache is lost — so we drive an
    explicit fsync on a fixed cadence to bound that loss.
    """
    if event_log is None:
        return
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
            break
        except asyncio.TimeoutError:
            pass
        try:
            await asyncio.to_thread(event_log.fsync_to_disk)
        except Exception as e:  # noqa: BLE001 — never let durability take the service down
            print(f"# event-log: fsync pass failed: {e}", file=sys.stderr)


def _surface_task_exception(name: str):
    """Done-callback factory for background asyncio.create_task() calls.

    Without this an unhandled exception in a background task is only
    visible when Python eventually GCs the Task — operators see no
    indication that retention/snapshot/fsync has silently stopped.
    """
    def _cb(task: asyncio.Task) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            print(
                f"# background task {name!r} exited with {exc!r} — "
                "this subsystem is now dead until restart",
                file=sys.stderr,
            )
    return _cb


async def _periodic_snapshot(
    state: StateManager,
    path: Path,
    interval: float,
    stop_event: asyncio.Event,
    serve: bool,
    evaluator=None,
    persist_interval: float = 30.0,
) -> None:
    import time as _time

    from . import server as srv  # lazy import — only needed when --serve is active

    # Broadcast (trimmed, serialised once, fanned out) happens every tick;
    # the full snapshot.json persist happens only every ``persist_interval``
    # seconds. Writing the full state to the SD card at 1 Hz was both a
    # wear concern and a growing CPU cost (payload grows with every radio
    # ever heard). Worst case on power-yank: the last ``persist_interval``
    # seconds of radio-state freshness — events themselves are bounded by
    # the 5 s JSONL fsync cadence, so nothing is unrecoverable.
    last_persist = 0.0  # monotonic; 0 → first tick persists immediately
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
            break
        except asyncio.TimeoutError:
            pass
        now = datetime.now()
        # state.tick walks active_calls to expire idles — defensive guard so
        # any future regression in _expire_idle_calls can't kill the loop.
        try:
            state.tick(now)
        except Exception as e:  # noqa: BLE001
            print(f"# state.tick failed: {e}", file=sys.stderr)
        # Time-based alerts (cc_silent, quality_spike) live on this same
        # cadence — keeps us from spinning up yet another background task.
        # quality_spike runs 3 SQLite queries against the index; with the
        # writer thread flushing in parallel this can stall for tens of ms,
        # so it goes to a worker thread.
        if evaluator is not None:
            try:
                await asyncio.to_thread(evaluator.tick, now)
            except Exception as e:  # noqa: BLE001
                print(f"# alerts: tick failed: {e}", file=sys.stderr)
        mono = _time.monotonic()
        if last_persist == 0.0 or mono - last_persist >= persist_interval:
            try:
                # Atomic write — power-yank mid-write must not leave a
                # truncated snapshot. ``atomic_write_text`` also preserves
                # the previous file as ``snapshot.json.bak`` so
                # ``StateManager.load_snapshot`` can fall back to it.
                # Full (untrimmed) view — this is restart persistence.
                atomic_write_text(path, state.snapshot().model_dump_json(indent=2))
                last_persist = mono
            except Exception as e:  # noqa: BLE001
                print(f"# snapshot write failed: {e}", file=sys.stderr)
        if serve:
            try:
                payload = await asyncio.to_thread(
                    lambda: state.snapshot(trim=True).model_dump_json()
                )
            except Exception as e:  # noqa: BLE001
                print(f"# broadcast snapshot failed: {e}", file=sys.stderr)
                continue
            await srv.push_snapshot(payload)


def _print_summary(state: StateManager) -> None:
    snap = state.snapshot()
    q = snap.quality
    print()
    print("# Summary:")
    print(f"#   radios:       {len(snap.radios)}")
    print(f"#   active calls: {len(snap.active_calls)}")
    print(f"#   site:         {snap.system.site}")
    print(f"#   quality errs: {q.csbk_crc + q.csbk_fec + q.cach_burst_fec + q.slco_crc}")
    print(f"#   total events: {q.total_events_seen}")
    positions = [(rid, r.last_position) for rid, r in snap.radios.items() if r.last_position]
    if positions:
        print("# Radios with last known position:")
        for rid, p in sorted(positions):
            print(f"#   {rid:>6}  ({p.lat}, {p.lon})  at {p.at.strftime('%H:%M:%S')}")


def normalize_frequency(value: str) -> str:
    """Normalise a user frequency into dsd-fme's 'NNN.NNNM' MHz form.

    Accepts plain Hz (``168500000``), MHz with an M suffix (``168.5M``),
    or a bare small number treated as MHz (``168.5``). Raises ValueError
    on garbage so the CLI can fail fast with a clear message.
    """
    s = str(value).strip()
    if not s:
        raise ValueError("empty frequency")
    if s[-1] in ("M", "m"):
        mhz = float(s[:-1])
    else:
        n = float(s)
        # Heuristic: anything ≥ 10 000 is Hz, below that is MHz.
        mhz = n / 1e6 if n >= 10_000 else n
    if not (0.001 <= mhz <= 3000):
        raise ValueError(f"frequency out of range: {value!r} → {mhz} MHz")
    return f"{mhz:g}M"


def build_soapy_input(args: argparse.Namespace) -> str:
    """Build dsd-fme's SoapySDR input string.

    Verified against dsd-neo's documented form
    ``soapy[:args]:freq[:gain[:ppm[:bw]]]`` — e.g.
    ``soapy:driver=sdrplay:168.5M:22:-2:24``. NOTE: the exact accepted
    keys can differ between dsd-fme forks/builds; check ``dsd-fme -h``
    on the target machine if the spawn fails.
    """
    device = f"driver={args.sdr_driver}"
    if args.sdr_device_args:
        device += f",{args.sdr_device_args}"
    freq = normalize_frequency(args.frequency)
    gain = f"{args.gain:g}"
    return (
        f"soapy:{device}:{freq}:{gain}:{args.ppm}:{args.bandwidth_khz}"
    )


def build_dsd_command(args: argparse.Namespace) -> list[str]:
    """Assemble the dsd-fme spawn command. Pure — unit-testable without
    hardware; the only inputs are parsed CLI args.

    ``-fs`` = DMR/Cap+ decode; ``-7 <dir>`` must come BEFORE ``-P``
    (per-call WAV recording) per the dsd-fme help.
    """
    if args.rf_backend == "soapy":
        input_str = build_soapy_input(args)
    else:
        input_str = args.input
    return [
        args.dsd_bin, "-fs",
        "-i", input_str,
        "-7", str(Path(args.calls_dir)),
        "-P",
    ]


async def _run_multichannel_live(  # pragma: no cover - needs an RSP + SoapySDR
    args: argparse.Namespace,
    state: StateManager,
    event_log,
    on_event,
    stop_event: asyncio.Event,
) -> None:
    """Wideband capture → channelizer → N TCP audio feeds → N dsd-fme.

    Hardware path (SoapySDR + RSP). The channel plumbing, scheduler, and
    channelizer are unit-tested separately; this is the assembly.
    """
    from .channel_plan import load_channel_plan
    from .rf.bridge import AudioTcpServer, run_capture_pump
    from .rf.capture import WidebandCapture
    from .rf.channelizer import Channelizer
    from .rf.multiplex import default_source_factory, run_multichannel

    plan = load_channel_plan(Path(args.channel_plan))
    print(
        f"# channel-plan: {len(plan.channels)} channels, span "
        f"{plan.span_hz()/1e6:.3f} MHz, center {(plan.center_hz() or 0)/1e6:.4f} MHz",
        file=sys.stderr,
    )

    servers: dict = {}
    for i, ch in enumerate(plan.channels):
        srv = AudioTcpServer(port=args.audio_base_port + i)
        await srv.start()
        servers[ch.label] = srv

    capture = WidebandCapture(
        plan, driver=args.sdr_driver, device_args=args.sdr_device_args,
        gain=(args.gain or None),
    )
    channelizer = Channelizer(plan, capture.sample_rate, audio_rate=args.audio_rate)

    # Phase 8: only decode channels the scheduler marks active.
    active_fn = None
    scheduler = None
    wrapped_on_event = on_event
    if args.follow_traffic:
        from .rf.scheduler import TrafficScheduler
        scheduler = TrafficScheduler(plan)
        active_fn = scheduler.active_labels

        def wrapped_on_event(ev, _sched=scheduler, _next=on_event):
            try:
                _sched.on_event(ev)
            except Exception:  # noqa: BLE001
                pass
            if _next is not None:
                _next(ev)

    pump = asyncio.create_task(
        run_capture_pump(capture, channelizer, servers, stop_event, active_fn)
    )
    pump.add_done_callback(_surface_task_exception("capture-pump"))

    factory = default_source_factory(
        plan, args.dsd_bin, Path(args.calls_dir), stop_event,
        base_port=args.audio_base_port,
        liveness_timeout=(args.liveness_timeout if args.liveness_timeout > 0 else None),
    )
    try:
        await run_multichannel(
            plan, state, factory, event_log=event_log,
            on_event=wrapped_on_event, stop_event=stop_event,
        )
    finally:
        pump.cancel()
        try:
            await pump
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass
        for srv in servers.values():
            await srv.close()


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    from . import __version__
    p = argparse.ArgumentParser(
        prog="dmr-monitor",
        description="DMR Cap+ live monitor.",
    )
    p.add_argument("--version", action="version", version=f"dmr-monitor {__version__}")
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--live", action="store_true", help="spawn dsd-fme and stream live")
    mode.add_argument("--replay", metavar="FILE", help="replay a captured dsd-fme stderr log")
    mode.add_argument("--rebuild-index", action="store_true",
                      help="rebuild the SQLite event index from --event-log "
                           "(single file or day-partition dir) and exit")
    mode.add_argument("--migrate-jsonl", action="store_true",
                      help="split a legacy monolithic events.jsonl into "
                           "per-day partition files and exit (also runs "
                           "automatically at startup when safe)")

    p.add_argument("--input", default="pulse:dmr_capture.monitor",
                   help="dsd-fme -i input device when --rf-backend=pulse "
                        "(live mode, default: %(default)s)")
    p.add_argument("--dsd-bin", default="dsd-fme",
                   help="path to dsd-fme binary (default: %(default)s)")

    # ── RF backend (v0.23.0) ──────────────────────────────────────────
    # 'pulse' is the legacy chain (SDRconnect GUI → virtual audio cable →
    # dsd-fme). 'soapy' cuts both out: dsd-fme opens the SDRplay RSP
    # directly through SoapySDR and tunes it from these flags.
    p.add_argument("--rf-backend", choices=("pulse", "soapy"), default="pulse",
                   help="how dsd-fme gets RF: 'pulse' = audio from the "
                        "dmr_capture sink (legacy, needs SDRconnect); "
                        "'soapy' = direct SDR control via SoapySDR "
                        "(default: %(default)s)")
    p.add_argument("--frequency", default=None,
                   help="tune frequency for --rf-backend=soapy — Hz "
                        "(e.g. 168500000) or MHz with an M suffix "
                        "(e.g. 168.5M). Required with soapy.")
    p.add_argument("--sdr-driver", default="sdrplay",
                   help="SoapySDR driver name (default: %(default)s)")
    p.add_argument("--sdr-device-args", default="",
                   help="extra SoapySDR device args appended after the "
                        "driver, e.g. 'serial=123456' (default: none)")
    p.add_argument("--gain", type=float, default=0,
                   help="tuner gain in dB for soapy (0 = driver auto, "
                        "default: %(default)s)")
    p.add_argument("--ppm", type=int, default=0,
                   help="frequency correction in ppm for soapy "
                        "(default: %(default)s)")
    p.add_argument("--bandwidth-khz", type=int, default=24,
                   help="channel bandwidth in kHz for soapy — 24 suits a "
                        "12.5 kHz DMR channel (default: %(default)s)")

    # ── Multi-frequency capture (Phase 7) ─────────────────────────────
    p.add_argument("--channel-plan", default=None,
                   help="JSON channel-plan file → decode several Cap+ "
                        "channels at once from one wideband RSP capture "
                        "(implies --rf-backend soapy). See "
                        "backend/channel_plan.py for the format.")
    p.add_argument("--audio-rate", type=int, default=48000,
                   help="per-channel audio rate for the channelizer TCP "
                        "feed in multi-frequency mode (default: %(default)s)")
    p.add_argument("--audio-base-port", type=int, default=7355,
                   help="first localhost TCP port for channelized audio; "
                        "channel N uses base+N (default: %(default)s)")
    p.add_argument("--follow-traffic", action="store_true",
                   help="Phase 8: only run decoders on channels that are "
                        "active (control-channel grants + energy), to save "
                        "CPU when channels outnumber the CPU budget")

    p.add_argument("--snapshot", default="snapshot.json",
                   help="periodic JSON snapshot path (default: %(default)s)")
    p.add_argument("--snapshot-interval", type=float, default=1.0,
                   help="broadcast tick interval in seconds — trimmed snapshot "
                        "pushed to WS clients (default: %(default)s)")
    p.add_argument("--snapshot-persist-interval", type=float, default=30.0,
                   help="full snapshot.json write interval in seconds; the "
                        "broadcast keeps ticking at --snapshot-interval "
                        "(default: %(default)s)")

    p.add_argument("--replay-delay", type=float, default=0.0,
                   help="per-line sleep in replay mode to simulate live timing")
    p.add_argument("--verbose", action="store_true",
                   help="print every parsed event, including noisy control-channel heartbeats")

    p.add_argument("--serve", action="store_true",
                   help="start FastAPI WebSocket server and browser UI")
    p.add_argument("--port", type=int, default=8080,
                   help="HTTP/WebSocket port when --serve is used (default: %(default)s)")
    p.add_argument("--calls-dir", default="/tmp/dmr_calls",
                   help="directory dsd-fme writes per-call WAVs into (default: %(default)s)")
    p.add_argument("--event-log", default="events.jsonl",
                   help="append-only JSONL of every parsed event (default: %(default)s)")
    p.add_argument("--event-buffer", type=int, default=20000,
                   help="in-memory event ring buffer size (default: %(default)s)")
    p.add_argument("--event-db", default=None,
                   help="SQLite index path (default: --event-log with .db suffix)")
    p.add_argument("--no-event-db", action="store_true",
                   help="disable the SQLite index sidecar (JSONL-only)")
    p.add_argument("--liveness-timeout", type=float, default=60.0,
                   help="exit (so systemd restarts us) if dsd-fme produces no "
                        "stderr output for this many seconds (live mode only; "
                        "default: %(default)s, 0 disables)")
    p.add_argument("--wav-retention-hours", type=float, default=72.0,
                   help="delete per-call WAVs in --calls-dir older than this "
                        "many hours; checked once an hour (default: %(default)s, "
                        "0 disables)")
    p.add_argument("--event-retention-hours", type=float, default=0.0,
                   help="DEPRECATED — prefer --retention-days. Delete events "
                        "older than this many hours; checked once an hour "
                        "(default: %(default)s, 0 disables — events kept "
                        "forever). Mutually exclusive with --retention-days.")
    p.add_argument("--retention-days", type=int, default=0,
                   help="keep this many whole local days of events; older "
                        "day files are unlinked and their index rows "
                        "deleted, checked once an hour (default: "
                        "%(default)s, 0 disables — events kept forever)")
    p.add_argument("--alerts-rules", default="alerts.json",
                   help="path to the Alerts Engine rules file "
                        "(default: %(default)s, empty string disables)")
    p.add_argument("--max-radios", type=int, default=2000,
                   help="cap on live radios kept in memory; oldest by "
                        "last_seen are evicted in batches past the cap — "
                        "their history stays queryable via the SQLite "
                        "index (default: %(default)s)")
    p.add_argument("--reset-token", default=None,
                   help="shared secret for POST /api/reset (sent as the "
                        "X-Reset-Token header); when unset, reset is "
                        "allowed from localhost only")
    p.add_argument("--dev-reload-html", action="store_true",
                   help="re-read frontend HTML from disk on every request "
                        "instead of caching at first hit (frontend dev)")
    return p.parse_args(argv)


async def _run(args: argparse.Namespace) -> None:
    from . import __build_date__, __version__
    print(f"# dmr-monitor v{__version__} (build {__build_date__})", file=sys.stderr)

    # Fail fast with a clear, actionable message if live mode is requested
    # but dsd-fme isn't installed. Otherwise the first spawn raises a bare
    # FileNotFoundError that systemd just restart-loops on — opaque to the
    # operator setting up a fresh Pi who simply hasn't built dsd-fme yet.
    # (A wrong-architecture binary still passes this check but is caught at
    # spawn time by the OSError guard in stream_subprocess_with_retry.)
    if args.live:
        import shutil
        if shutil.which(args.dsd_bin) is None:
            print(
                f"# FATAL: dsd-fme binary {args.dsd_bin!r} not found on PATH.\n"
                f"#   Live mode needs dsd-fme. Install system deps with\n"
                f"#   'bash scripts/install-deps.sh', then build dsd-fme (see the\n"
                f"#   hints in 'bash scripts/check_env.sh'), or pass\n"
                f"#   --dsd-bin /path/to/dsd-fme. To run with no RF hardware at\n"
                f"#   all, replay a captured log instead: --replay <logfile>.",
                file=sys.stderr,
            )
            raise SystemExit(2)

    stop_event = asyncio.Event()

    calls_dir = Path(args.calls_dir)
    recordings = None
    if args.serve:
        from .recordings import RecordingRegistry
        recordings = RecordingRegistry(calls_dir)

    from .event_log import EventLog
    jsonl_path = Path(args.event_log) if args.event_log else None
    db_path = Path(args.event_db) if args.event_db else None
    # One-time monolith → per-day migration, BEFORE the writer opens so
    # there are no concurrent appends. Crash-safe: the legacy file is only
    # renamed (never deleted) and a marker gates the rename phase.
    if jsonl_path is not None:
        from .export import MigrationRefused, migrate_monolith
        try:
            migrate_monolith(jsonl_path)
        except MigrationRefused as exc:
            print(f"# FATAL: {exc}", file=sys.stderr)
            raise SystemExit(2)
    event_log = EventLog(
        jsonl_path=jsonl_path,
        capacity=args.event_buffer,
        db_path=db_path,
        enable_index=not args.no_event_db,
        partition=True,
    )
    if event_log.index is not None:
        try:
            rows = event_log.index.count()
            print(
                f"# event index: {rows} rows at {event_log.index.db_path}",
                file=sys.stderr,
            )
        except Exception:  # noqa: BLE001
            pass

    state = StateManager(max_radios=args.max_radios)
    snapshot_path = Path(args.snapshot)
    if state.load_snapshot(snapshot_path):
        print(
            f"# restored {len(state.radios)} radios from {snapshot_path}",
            file=sys.stderr,
        )
    primed = event_log.prime_from_jsonl()
    if primed:
        print(f"# primed event buffer with {primed} events from JSONL", file=sys.stderr)

    # ── Alerts Engine ────────────────────────────────────────────────
    evaluator = None
    if args.alerts_rules:
        from .alerts import Evaluator
        evaluator = Evaluator(
            rules_path=Path(args.alerts_rules),
            event_log=event_log,
        )
        if evaluator.list_rules():
            print(
                f"# alerts: loaded {len(evaluator.list_rules())} rule(s) "
                f"from {args.alerts_rules}",
                file=sys.stderr,
            )

    printer = _make_event_printer(state, args.verbose)
    if args.serve or evaluator is not None:
        # Wrap the printer so we can:
        #   * mark the last voice event (for /api/health)
        #   * feed each event through the Alerts evaluator
        # without making LineRunner aware of either dependency.
        _printer = printer
        srv = None
        if args.serve:
            from . import server as _srv
            srv = _srv

        def printer_chain(ev: Event) -> None:
            if srv is not None and ev.type == EventType.VOICE_CALL:
                srv.note_voice_event(ev.timestamp)
            if evaluator is not None:
                try:
                    evaluator.on_event(ev)
                except Exception as exc:  # noqa: BLE001 — alerts must never break pipeline
                    print(f"# alerts: on_event failed: {exc}", file=sys.stderr)
            _printer(ev)

        printer = printer_chain
    runner = LineRunner(state, on_event=printer, event_log=event_log)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    # ── Optional FastAPI / WebSocket server ──────────────────────────
    if args.serve:
        import uvicorn
        from . import server as srv
        srv.attach_state(state)
        if recordings is not None:
            srv.attach_recordings(recordings)
        srv.attach_event_log(event_log)
        srv.attach_snapshot_path(snapshot_path)
        srv.attach_evaluator(evaluator)
        srv.attach_reset_token(args.reset_token)
        srv.set_dev_reload_html(args.dev_reload_html)
        config = uvicorn.Config(
            srv.app,
            host="0.0.0.0",
            port=args.port,
            log_level="warning",
            loop="none",
        )
        uv_server = uvicorn.Server(config)
        uv_server.install_signal_handlers = lambda: None  # we handle signals
        uv_task = asyncio.create_task(uv_server.serve())

        # Without a callback, an unhandled exception in uvicorn.serve()
        # (port-in-use, internal bug, etc.) is silently swallowed when the
        # task is garbage-collected and the dashboard goes dark while the
        # rest of the process keeps running. Surface it loudly and ask the
        # main loop to shut down so systemd can restart us cleanly.
        def _uv_done(t: asyncio.Task) -> None:
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                print(f"# uvicorn: server task exited with {exc!r}",
                      file=sys.stderr)
                stop_event.set()

        uv_task.add_done_callback(_uv_done)
        print(f"# dashboard → http://0.0.0.0:{args.port}/", file=sys.stderr)

    snap_task = asyncio.create_task(
        _periodic_snapshot(
            state, snapshot_path, args.snapshot_interval, stop_event,
            args.serve, evaluator=evaluator,
            persist_interval=args.snapshot_persist_interval,
        )
    )
    snap_task.add_done_callback(_surface_task_exception("snapshot"))

    fsync_task = asyncio.create_task(_periodic_fsync(event_log, stop_event))
    fsync_task.add_done_callback(_surface_task_exception("fsync"))

    retention_task: Optional[asyncio.Task] = None
    if recordings is not None and args.wav_retention_hours > 0:
        retention_task = asyncio.create_task(
            _periodic_wav_retention(recordings, args.wav_retention_hours, stop_event)
        )
        retention_task.add_done_callback(_surface_task_exception("wav-retention"))

    event_retention_task: Optional[asyncio.Task] = None
    if args.event_retention_hours > 0:
        event_retention_task = asyncio.create_task(
            _periodic_event_retention(
                event_log, args.event_retention_hours, stop_event,
            )
        )
        event_retention_task.add_done_callback(_surface_task_exception("event-retention"))

    day_retention_task: Optional[asyncio.Task] = None
    if args.retention_days > 0:
        day_retention_task = asyncio.create_task(
            _periodic_day_retention(event_log, args.retention_days, stop_event)
        )
        day_retention_task.add_done_callback(_surface_task_exception("day-retention"))

    consume = None
    if args.live and args.channel_plan:
        # Multi-frequency mode: one wideband capture, N channelized
        # decoders. Runs its own set of per-channel LineRunners, so the
        # single `runner` above is unused on this path.
        calls_dir.mkdir(parents=True, exist_ok=True)
        consume = _run_multichannel_live(
            args, state, event_log, printer, stop_event,
        )
    elif args.live:
        # dsd-fme writes per-call WAVs to --calls-dir via `-7 <dir> -P`.
        calls_dir.mkdir(parents=True, exist_ok=True)
        cmd = build_dsd_command(args)
        print(f"# starting: {' '.join(cmd)}", file=sys.stderr)
        liveness = args.liveness_timeout if args.liveness_timeout > 0 else None
        # _with_retry keeps the asyncio loop alive across dsd-fme stalls.
        # The watchdog still fires after `liveness_timeout` seconds of
        # silence — but instead of crashing the whole service (and making
        # systemd restart everything, which blacks out the dashboard for
        # ~10s), we just respawn the child and the rest of the process
        # carries on. The CLI flag default (60s) stays unchanged.
        source = stream_subprocess_with_retry(
            cmd, stop_event=stop_event, liveness_timeout=liveness,
        )
        consume = runner.consume_lines(source)
    else:
        print(f"# replaying {args.replay} (delay={args.replay_delay}s)", file=sys.stderr)
        source = stream_file(args.replay, delay=args.replay_delay, stop_event=stop_event)
        consume = runner.consume_lines(source)

    try:
        await consume
    finally:
        stop_event.set()
        try:
            atomic_write_text(snapshot_path, state.snapshot().model_dump_json(indent=2))
        except Exception as e:  # noqa: BLE001
            print(f"# final snapshot write failed: {e}", file=sys.stderr)
        snap_task.cancel()
        try:
            await snap_task
        except asyncio.CancelledError:
            pass
        fsync_task.cancel()
        try:
            await fsync_task
        except asyncio.CancelledError:
            pass
        if retention_task is not None:
            retention_task.cancel()
            try:
                await retention_task
            except asyncio.CancelledError:
                pass
        if event_retention_task is not None:
            event_retention_task.cancel()
            try:
                await event_retention_task
            except asyncio.CancelledError:
                pass
        event_log.close()

    _print_summary(state)


def _rebuild_index(args: argparse.Namespace) -> int:
    from .event_index import EventIndex
    from .export import partition_dir_for
    from .models import EVENT_SCHEMA_VERSION

    jsonl_path = Path(args.event_log) if args.event_log else None
    if jsonl_path is None:
        print("# --rebuild-index: no --event-log given", file=sys.stderr)
        return 1
    # Prefer the day-partition dir when it exists; fall back to the
    # legacy single file.
    source = partition_dir_for(jsonl_path)
    if not source.is_dir():
        source = jsonl_path
    if not source.exists():
        print(f"# --rebuild-index: {source} not found", file=sys.stderr)
        return 1
    db_path = Path(args.event_db) if args.event_db else jsonl_path.with_suffix(".db")
    print(f"# rebuilding index at {db_path} from {source}", file=sys.stderr)
    idx = EventIndex(db_path, schema_version=EVENT_SCHEMA_VERSION)
    try:
        rows = idx.rebuild_from_jsonl(source)
    finally:
        idx.close()
    print(f"# event index: {rows} rows written", file=sys.stderr)
    return 0


def _migrate_jsonl(args: argparse.Namespace) -> int:
    from .export import MigrationRefused, migrate_monolith

    jsonl_path = Path(args.event_log) if args.event_log else None
    if jsonl_path is None:
        print("# --migrate-jsonl: no --event-log given", file=sys.stderr)
        return 1
    try:
        metrics = migrate_monolith(jsonl_path)
    except MigrationRefused as exc:
        print(f"# --migrate-jsonl: {exc}", file=sys.stderr)
        return 2
    if metrics is None:
        print(
            f"# --migrate-jsonl: nothing to do — no legacy {jsonl_path} "
            "and no unfinished migration",
            file=sys.stderr,
        )
        return 0
    print(f"# --migrate-jsonl: done ({metrics})", file=sys.stderr)
    return 0


def main(argv: Optional[list[str]] = None) -> None:
    args = _parse_args(argv)
    if args.retention_days > 0 and args.event_retention_hours > 0:
        print(
            "# FATAL: --retention-days and --event-retention-hours are "
            "mutually exclusive — pick one.",
            file=sys.stderr,
        )
        sys.exit(2)
    if getattr(args, "live", False) and args.channel_plan:
        # Multi-frequency mode gets its frequencies from the plan, not
        # --frequency. Validate the plan (and single-RSP feasibility) now
        # so a typo fails fast without touching hardware.
        from .channel_plan import load_channel_plan
        try:
            plan = load_channel_plan(Path(args.channel_plan))
        except Exception as exc:  # noqa: BLE001
            print(f"# FATAL: bad --channel-plan: {exc}", file=sys.stderr)
            sys.exit(2)
        if not plan.channels:
            print("# FATAL: --channel-plan has no channels.", file=sys.stderr)
            sys.exit(2)
        if not plan.fits_in_bandwidth(10_000_000):
            print(
                f"# FATAL: channel plan spans {plan.span_hz()/1e6:.3f} MHz — "
                "wider than one RSP1B (~10 MHz). Narrow it, scan, or add a "
                "receiver.",
                file=sys.stderr,
            )
            sys.exit(2)
    elif getattr(args, "live", False) and args.rf_backend == "soapy":
        if not args.frequency:
            print(
                "# FATAL: --rf-backend soapy requires --frequency "
                "(e.g. --frequency 168.5M).",
                file=sys.stderr,
            )
            sys.exit(2)
        try:
            normalize_frequency(args.frequency)
        except ValueError as exc:
            print(f"# FATAL: bad --frequency: {exc}", file=sys.stderr)
            sys.exit(2)
    if getattr(args, "rebuild_index", False):
        sys.exit(_rebuild_index(args))
    if getattr(args, "migrate_jsonl", False):
        sys.exit(_migrate_jsonl(args))
    try:
        asyncio.run(_run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
