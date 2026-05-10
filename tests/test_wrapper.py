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
