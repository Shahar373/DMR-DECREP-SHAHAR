"""Phase-7 tests: multi-channel orchestration + channel event tagging.

No SDR: channel line-sources are fed from in-memory captured lines, and
we assert each channel's events are tagged with that channel and that two
channels' slot-1 calls don't collide in the StateManager.
"""
from __future__ import annotations

import asyncio
from pathlib import Path

from backend.channel_plan import Channel, ChannelPlan
from backend.event_log import EventLog
from backend.rf.multiplex import build_channel_command, run_multichannel
from backend.state import StateManager

# Two minimal dsd-fme-style stderr snippets, one voice call on slot 1 each.
# Real dsd-fme voice line: " SLOT 1 TGT=9 SRC=111 Group Call ".
LINES_A = [
    "09:00:00 Sync: +DMR\n",
    " SLOT 1 TGT=9 SRC=111 Group Call \n",
    " SLOT 1 TGT=9 SRC=111 Group Call \n",
]
LINES_B = [
    "09:00:00 Sync: +DMR\n",
    " SLOT 1 TGT=7 SRC=222 Group Call \n",
    " SLOT 1 TGT=7 SRC=222 Group Call \n",
]


async def _iter(lines):
    for ln in lines:
        yield ln


def _run(plan, factory, state, event_log=None):
    events = []
    asyncio.run(run_multichannel(
        plan, state, factory, event_log=event_log,
        on_event=events.append,
    ))
    return events


def test_events_tagged_per_channel_and_calls_do_not_collide():
    plan = ChannelPlan(channels=[
        Channel(label="cc", frequency_hz=168_500_000),
        Channel(label="ch2", frequency_hz=168_512_500),
    ])
    lines = {"cc": LINES_A, "ch2": LINES_B}
    factory = lambda ch: _iter(lines[ch.label])
    state = StateManager()
    events = _run(plan, factory, state)

    voice = [e for e in events if e.type.value == "voice_call"]
    assert voice, "no voice events parsed"
    # Every event carries its channel's label + frequency.
    by_label = {}
    for e in voice:
        by_label.setdefault(e.channel_label, set()).add(e.src)
    assert by_label["cc"] == {111}
    assert by_label["ch2"] == {222}
    freqs = {e.channel_label: e.frequency for e in voice}
    assert freqs["cc"] == 168_500_000
    assert freqs["ch2"] == 168_512_500

    # Both slot-1 calls coexist in state under composite keys.
    snap = state.snapshot()
    assert len(snap.active_calls) == 2
    keys = set(snap.active_calls)
    assert "cc:1" in keys and "ch2:1" in keys
    srcs = {c.src for c in snap.active_calls.values()}
    assert srcs == {111, 222}
    # Radios stamped with their last channel.
    assert state.radios[111].last_channel == "cc"
    assert state.radios[222].last_frequency == 168_512_500


def test_run_multichannel_writes_to_shared_event_log(tmp_path):
    plan = ChannelPlan(channels=[
        Channel(label="cc", frequency_hz=168_500_000),
        Channel(label="ch2", frequency_hz=168_512_500),
    ])
    log = EventLog(jsonl_path=tmp_path / "events.jsonl", capacity=100, partition=True)
    try:
        lines = {"cc": LINES_A, "ch2": LINES_B}
        _run(plan, lambda ch: _iter(lines[ch.label]), StateManager(), event_log=log)
        # Both channels' events landed in the one shared log.
        buffered = log.recent(limit=100)
        labels = {e.channel_label for e in buffered if e.type.value == "voice_call"}
        assert labels == {"cc", "ch2"}
    finally:
        log.close()


def test_build_channel_command_uses_tcp_and_per_channel_dir():
    ch = Channel(label="ch2", frequency_hz=168_512_500)
    cmd = build_channel_command(ch, "dsd-fme", Path("/tmp/calls"), "127.0.0.1", 7356)
    assert cmd[:4] == ["dsd-fme", "-fs", "-i", "tcp:127.0.0.1:7356"]
    assert "/tmp/calls/ch2" in cmd  # per-channel WAV subdir
    assert cmd[-1] == "-P" and cmd[-3] == "-7"


def test_single_channel_state_unchanged():
    """Sanity: with no channel tag, active_calls keys stay bare slots."""
    from backend.models import VoiceCallEvent
    from datetime import datetime
    sm = StateManager()
    sm.apply(VoiceCallEvent(timestamp=datetime(2026, 7, 11, 9, 0, 0),
                            raw_line="v", slot=1, src=101, tgt=9))
    snap = sm.snapshot()
    assert set(snap.active_calls) == {"1"}  # JSON-stringified bare slot
    assert snap.active_calls["1"].channel_label is None
