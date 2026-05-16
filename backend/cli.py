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
from .wrapper import LineRunner, stream_file, stream_subprocess


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
            deleted, freed = recordings.prune_older_than(hours)
            if deleted:
                print(
                    f"# wav-retention: deleted {deleted} files "
                    f"({freed/1024/1024:.1f} MiB) older than {hours}h",
                    file=sys.stderr,
                )
        except Exception as e:  # noqa: BLE001
            print(f"# wav-retention: pass failed: {e}", file=sys.stderr)


async def _periodic_snapshot(
    state: StateManager,
    path: Path,
    interval: float,
    stop_event: asyncio.Event,
    serve: bool,
    evaluator=None,
) -> None:
    from . import server as srv  # lazy import — only needed when --serve is active
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
            break
        except asyncio.TimeoutError:
            pass
        now = datetime.now()
        state.tick(now)
        # Time-based alerts (cc_silent, quality_spike) live on this same
        # cadence — keeps us from spinning up yet another background task.
        if evaluator is not None:
            try:
                evaluator.tick(now)
            except Exception as e:  # noqa: BLE001
                print(f"# alerts: tick failed: {e}", file=sys.stderr)
        try:
            # Atomic write — power-yank mid-write must not leave a
            # truncated snapshot. ``atomic_write_text`` also preserves
            # the previous file as ``snapshot.json.bak`` so
            # ``StateManager.load_snapshot`` can fall back to it.
            atomic_write_text(path, state.snapshot().model_dump_json(indent=2))
        except Exception as e:  # noqa: BLE001
            print(f"# snapshot write failed: {e}", file=sys.stderr)
        if serve:
            await srv.push_snapshot()


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
                      help="rebuild the SQLite event index from --event-log and exit")

    p.add_argument("--input", default="pulse:dmr_capture.monitor",
                   help="dsd-fme -i input device (live mode, default: %(default)s)")
    p.add_argument("--dsd-bin", default="dsd-fme",
                   help="path to dsd-fme binary (default: %(default)s)")

    p.add_argument("--snapshot", default="snapshot.json",
                   help="periodic JSON snapshot path (default: %(default)s)")
    p.add_argument("--snapshot-interval", type=float, default=1.0,
                   help="snapshot write interval in seconds (default: %(default)s)")

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
    p.add_argument("--alerts-rules", default="alerts.json",
                   help="path to the Alerts Engine rules file "
                        "(default: %(default)s, empty string disables)")
    return p.parse_args(argv)


async def _run(args: argparse.Namespace) -> None:
    from . import __build_date__, __version__
    print(f"# dmr-monitor v{__version__} (build {__build_date__})", file=sys.stderr)

    stop_event = asyncio.Event()

    calls_dir = Path(args.calls_dir)
    recordings = None
    if args.serve:
        from .recordings import RecordingRegistry
        recordings = RecordingRegistry(calls_dir)

    from .event_log import EventLog
    jsonl_path = Path(args.event_log) if args.event_log else None
    db_path = Path(args.event_db) if args.event_db else None
    event_log = EventLog(
        jsonl_path=jsonl_path,
        capacity=args.event_buffer,
        db_path=db_path,
        enable_index=not args.no_event_db,
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

    state = StateManager()
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
        config = uvicorn.Config(
            srv.app,
            host="0.0.0.0",
            port=args.port,
            log_level="warning",
            loop="none",
        )
        uv_server = uvicorn.Server(config)
        uv_server.install_signal_handlers = lambda: None  # we handle signals
        asyncio.create_task(uv_server.serve())
        print(f"# dashboard → http://0.0.0.0:{args.port}/", file=sys.stderr)

    snap_task = asyncio.create_task(
        _periodic_snapshot(
            state, snapshot_path, args.snapshot_interval, stop_event,
            args.serve, evaluator=evaluator,
        )
    )
    retention_task: Optional[asyncio.Task] = None
    if recordings is not None and args.wav_retention_hours > 0:
        retention_task = asyncio.create_task(
            _periodic_wav_retention(recordings, args.wav_retention_hours, stop_event)
        )

    if args.live:
        # dsd-fme writes per-call WAVs to --calls-dir via `-7 <dir> -P`.
        # `-7` must come BEFORE `-P` per the dsd-fme help.
        calls_dir.mkdir(parents=True, exist_ok=True)
        cmd = [
            args.dsd_bin, "-fs",
            "-i", args.input,
            "-7", str(calls_dir),
            "-P",
        ]
        print(f"# starting: {' '.join(cmd)}", file=sys.stderr)
        liveness = args.liveness_timeout if args.liveness_timeout > 0 else None
        source = stream_subprocess(cmd, stop_event=stop_event, liveness_timeout=liveness)
    else:
        print(f"# replaying {args.replay} (delay={args.replay_delay}s)", file=sys.stderr)
        source = stream_file(args.replay, delay=args.replay_delay, stop_event=stop_event)

    try:
        await runner.consume_lines(source)
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
        if retention_task is not None:
            retention_task.cancel()
            try:
                await retention_task
            except asyncio.CancelledError:
                pass
        event_log.close()

    _print_summary(state)


def _rebuild_index(args: argparse.Namespace) -> int:
    from .event_index import EventIndex
    from .models import EVENT_SCHEMA_VERSION

    jsonl_path = Path(args.event_log) if args.event_log else None
    if jsonl_path is None or not jsonl_path.exists():
        print(f"# --rebuild-index: {jsonl_path} not found", file=sys.stderr)
        return 1
    db_path = Path(args.event_db) if args.event_db else jsonl_path.with_suffix(".db")
    print(f"# rebuilding index at {db_path} from {jsonl_path}", file=sys.stderr)
    idx = EventIndex(db_path, schema_version=EVENT_SCHEMA_VERSION)
    try:
        rows = idx.rebuild_from_jsonl(jsonl_path)
    finally:
        idx.close()
    print(f"# event index: {rows} rows written", file=sys.stderr)
    return 0


def main(argv: Optional[list[str]] = None) -> None:
    args = _parse_args(argv)
    if getattr(args, "rebuild_index", False):
        sys.exit(_rebuild_index(args))
    try:
        asyncio.run(_run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
