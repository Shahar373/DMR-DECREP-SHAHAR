"""Tests for backend.wrapper.LineRunner + stream_file replay.

Subprocess streaming is not unit-tested here — it's covered end-to-end by the
manual smoke test in the README beta section (replay against a captured log
is sufficient regression for the asyncio pump itself).
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path

from backend.state import StateManager
from backend.wrapper import LineRunner, stream_file


CAPTURE = Path(__file__).parent / "captures" / "dmr_night_sample.log"


def _run(coro):
    return asyncio.run(coro)


async def _gen(lines: list[str]) -> AsyncIterator[str]:
    for line in lines:
        yield line


def test_line_runner_drives_state_from_lines():
    state = StateManager()
    runner = LineRunner(state)
    lines = [
        "21:04:17 Sync: +DMR  [slot1]  slot2  | Color Code=02 | CSBK\n",
        " SLOT 2 TGT=1 SRC=2102 Group Call\n",
        " SLCO Capacity Plus Site: 2 - Rest LSN: 6 - RS: 00\n",
        " Lat: 32.10128 Lon: 34.87151 (32.10128, 34.87151)\n",  # no SRC bound → ignored
        " SRC(24): 00000068; IP: 012.000.000.068; Port: 4001;\n",
        " Lat: 32.10128 Lon: 34.87151 (32.10128, 34.87151)\n",
    ]
    _run(runner.consume_lines(_gen(lines)))

    assert state.system.site == 2
    assert 2102 in state.radios and state.radios[2102].voice_frame_count == 1
    assert state.radios[68].last_position is not None
    assert state.radios[68].last_position.lat == 32.10128


def test_line_runner_fires_on_event_callback_for_each_emitted_event():
    state = StateManager()
    captured = []
    runner = LineRunner(state, on_event=captured.append)
    lines = [
        "21:04:17 Sync: +DMR  [slot1]  slot2  | Color Code=02 | CSBK\n",  # no event
        " SLCO Capacity Plus Site: 2 - Rest LSN: 6 - RS: 00\n",          # site_info
        " SLOT 2 TGT=1 SRC=2102 Group Call\n",                            # voice_call
    ]
    _run(runner.consume_lines(_gen(lines)))
    types = [ev.type.value for ev in captured]
    assert types == ["site_info", "voice_call"]


def test_line_runner_swallows_callback_errors():
    state = StateManager()

    def explode(_ev):
        raise RuntimeError("boom")

    runner = LineRunner(state, on_event=explode)
    lines = [" SLCO Capacity Plus Site: 2 - Rest LSN: 6 - RS: 00\n"]
    _run(runner.consume_lines(_gen(lines)))  # must not raise
    assert state.system.site == 2  # state was still updated


def test_line_runner_stop_halts_consumption():
    state = StateManager()
    runner = LineRunner(state)
    lines = [
        " SLCO Capacity Plus Site: 2 - Rest LSN: 6 - RS: 00\n",
        " SLCO Capacity Plus Site: 9 - Rest LSN: 1 - RS: 00\n",
    ]

    async def go():
        async def gen():
            for line in lines:
                yield line
                runner.stop()  # stop after the very first line

        await runner.consume_lines(gen())

    _run(go())
    # Only the first SiteInfo (site=2) should have been applied
    assert state.system.site == 2


def test_stream_file_replays_capture_into_state():
    """End-to-end: feed the trimmed real capture through the runner and
    confirm the same radio set the parser+state regression already verified."""
    state = StateManager()
    runner = LineRunner(state)
    _run(runner.consume_lines(stream_file(str(CAPTURE))))

    assert state.system.site == 2
    radios_with_position = {rid for rid, r in state.radios.items() if r.last_position}
    assert radios_with_position & {65, 68, 70, 74}


# ── Subprocess liveness watchdog ────────────────────────────────────

def test_stream_subprocess_triggers_liveness_timeout_on_silent_child():
    """A subprocess that stays open but produces no stderr should be
    killed and a RuntimeError raised after the liveness timeout, so that
    systemd's Restart=on-failure brings the service back."""
    import pytest

    from backend.wrapper import stream_subprocess

    async def consume():
        # `sleep 5` exits 0 with no stderr — never emits a line, so the
        # 0.2 s watchdog must fire well before sleep finishes.
        gen = stream_subprocess(["sleep", "5"], liveness_timeout=0.2)
        async for _line in gen:
            pass

    with pytest.raises(RuntimeError, match="liveness timeout"):
        asyncio.run(consume())


def test_stream_subprocess_no_timeout_when_lines_keep_flowing():
    """Liveness watchdog is per-line — a child that emits any line within
    the timeout window must keep running until natural EOF."""
    import sys as _sys

    from backend.wrapper import stream_subprocess

    async def collect():
        # Python one-liner that writes two lines to stderr (what we
        # capture) and exits cleanly. No timeout should fire.
        gen = stream_subprocess(
            [_sys.executable, "-c",
             "import sys; sys.stderr.write('a\\nb\\n'); sys.stderr.flush()"],
            liveness_timeout=1.0,
        )
        out = []
        async for line in gen:
            out.append(line)
        return out

    lines = asyncio.run(collect())
    assert lines == ["a\n", "b\n"]


def test_stream_subprocess_with_retry_respawns_on_liveness_timeout():
    """When the inner child stalls, the wrapper should kill it, sleep
    briefly, spawn a fresh child, and keep yielding lines — without
    bubbling RuntimeError up to the caller. The asyncio process stays
    alive across the restart."""
    import sys as _sys

    from backend.wrapper import stream_subprocess_with_retry

    # First child: prints one line then hangs forever (sleep). Second
    # child: prints a different line then exits cleanly. Watchdog timeout
    # is tight (0.4s) so the first child's stall trips fast.
    state_path = "/tmp/_retry_state_marker"
    try:
        import os
        if os.path.exists(state_path):
            os.unlink(state_path)
    except OSError:
        pass

    script = (
        "import os, sys, time, pathlib;"
        "marker = pathlib.Path('" + state_path + "');"
        "first = not marker.exists();"
        "marker.touch();"
        "sys.stderr.write(('first\\n' if first else 'second\\n'));"
        "sys.stderr.flush();"
        # First run hangs (forces watchdog). Second run exits clean.
        "(time.sleep(10) if first else None)"
    )

    async def collect():
        stop = asyncio.Event()
        gen = stream_subprocess_with_retry(
            [_sys.executable, "-c", script],
            stop_event=stop,
            liveness_timeout=0.4,
            backoff_seconds=0.1,
        )
        out: list[str] = []
        async for line in gen:
            out.append(line)
            if "second" in line:
                stop.set()
                break
        return out

    lines = asyncio.run(collect())
    try:
        import os
        os.unlink(state_path)
    except OSError:
        pass
    assert "first\n" in lines
    assert "second\n" in lines


def test_stream_subprocess_with_retry_honours_stop_event():
    """Setting stop_event should make the wrapper exit cleanly without
    spawning another child, even mid-stream."""
    import sys as _sys

    from backend.wrapper import stream_subprocess_with_retry

    async def go():
        stop = asyncio.Event()
        gen = stream_subprocess_with_retry(
            [_sys.executable, "-c",
             "import sys; sys.stderr.write('x\\n'); sys.stderr.flush()"],
            stop_event=stop,
            liveness_timeout=2.0,
            backoff_seconds=0.05,
        )
        first = None
        async for line in gen:
            first = line
            stop.set()
            break
        # Drain the rest — should exit promptly because stop is set.
        rest = []
        async for line in gen:
            rest.append(line)
        return first, rest

    first, rest = asyncio.run(go())
    assert first == "x\n"
    assert rest == []
