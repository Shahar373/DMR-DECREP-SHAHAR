"""DMR Cap+ live monitor — Phase 3 CLI beta.

Two modes:

  python -m backend.cli --live
      Spawn dsd-fme, stream its stderr through parser + state manager, write
      a periodic snapshot to snapshot.json. Audio goes to /tmp/dmr_audio.wav
      via dsd-fme's own -o flag (Phase 4b will pipe it through ffmpeg/Icecast).

  python -m backend.cli --replay tests/captures/dmr_night_sample.log
      Same pipeline, but read lines from a captured log instead. Useful for
      smoke-testing without RF.

Snapshot file is overwritten in place every `--snapshot-interval` seconds and
also at clean shutdown. Phase 4 (FastAPI + WebSocket) will read this file or
subscribe to a live event bus instead.
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
from .state import StateManager
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


async def _periodic_snapshot(
    state: StateManager, path: Path, interval: float, stop_event: asyncio.Event
) -> None:
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
            break  # stop_event fired
        except asyncio.TimeoutError:
            pass
        try:
            path.write_text(state.snapshot().model_dump_json(indent=2))
        except Exception as e:  # noqa: BLE001
            print(f"# snapshot write failed: {e}", file=sys.stderr)


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
    p = argparse.ArgumentParser(
        prog="dmr-monitor",
        description="DMR Cap+ live monitor (Phase 3 CLI beta).",
    )
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--live", action="store_true", help="spawn dsd-fme and stream live")
    mode.add_argument("--replay", metavar="FILE", help="replay a captured dsd-fme stderr log")

    p.add_argument("--input", default="pulse:dmr_capture.monitor",
                   help="dsd-fme -i input device (live mode, default: %(default)s)")
    p.add_argument("--audio-out", default="/tmp/dmr_audio.wav",
                   help="dsd-fme -o WAV output path (live mode, default: %(default)s)")
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
    return p.parse_args(argv)


async def _run(args: argparse.Namespace) -> None:
    state = StateManager()
    printer = _make_event_printer(state, args.verbose)
    runner = LineRunner(state, on_event=printer)

    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    snapshot_path = Path(args.snapshot)
    snap_task = asyncio.create_task(
        _periodic_snapshot(state, snapshot_path, args.snapshot_interval, stop_event)
    )

    if args.live:
        cmd = [args.dsd_bin, "-fs", "-i", args.input, "-o", args.audio_out]
        print(f"# starting: {' '.join(cmd)}", file=sys.stderr)
        source = stream_subprocess(cmd, stop_event=stop_event)
    else:
        print(f"# replaying {args.replay} (delay={args.replay_delay}s)", file=sys.stderr)
        source = stream_file(args.replay, delay=args.replay_delay, stop_event=stop_event)

    try:
        await runner.consume_lines(source)
    finally:
        stop_event.set()
        snapshot_path.write_text(state.snapshot().model_dump_json(indent=2))
        snap_task.cancel()
        try:
            await snap_task
        except asyncio.CancelledError:
            pass

    _print_summary(state)


def main(argv: Optional[list[str]] = None) -> None:
    args = _parse_args(argv)
    try:
        asyncio.run(_run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
