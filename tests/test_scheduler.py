"""Phase-8 tests: traffic-following decoder scheduler (pure policy)."""
from __future__ import annotations

from datetime import datetime, timedelta

from backend.channel_plan import Channel, ChannelPlan
from backend.models import (
    BankCallEvent,
    LSNState,
    LSNStatusEvent,
    VoiceCallEvent,
)
from backend.rf.scheduler import TrafficScheduler

_T0 = datetime(2026, 7, 12, 9, 0, 0)


def _plan():
    return ChannelPlan(channels=[
        Channel(label="cc", frequency_hz=168_500_000, lsn=1, control=True),
        Channel(label="ch2", frequency_hz=168_512_500, lsn=2),
        Channel(label="ch3", frequency_hz=168_525_000, lsn=3),
        Channel(label="ch4", frequency_hz=168_537_500, lsn=4),
    ])


def _voice(sec, src, tgt, channel_label):
    ev = VoiceCallEvent(
        timestamp=_T0 + timedelta(seconds=sec),
        raw_line="v", slot=1, src=src, tgt=tgt,
    )
    ev.channel_label = channel_label
    return ev


def test_control_channels_always_active():
    sch = TrafficScheduler(_plan())
    # No activity at all — only the control channel is decoded.
    assert sch.active_labels(_T0) == {"cc"}


def test_traffic_on_a_channel_keeps_it_active_with_hang():
    sch = TrafficScheduler(_plan(), hang_seconds=4.0)
    sch.on_event(_voice(0, 111, 9, "ch2"))
    # Immediately after, ch2 is active alongside the control channel.
    assert sch.active_labels(_T0) == {"cc", "ch2"}
    # Within hang window — still active.
    assert sch.active_labels(_T0 + timedelta(seconds=3)) == {"cc", "ch2"}
    # Past hang — drops back to control only.
    assert sch.active_labels(_T0 + timedelta(seconds=5)) == {"cc"}


def test_lsn_status_grant_activates_mapped_channel():
    sch = TrafficScheduler(_plan(), hang_seconds=4.0)
    ev = LSNStatusEvent(
        timestamp=_T0, raw_line="lsn",
        states=[
            LSNState(lsn=3, state="Active", tg=42),
            LSNState(lsn=2, state="Idle"),
        ],
    )
    sch.on_event(ev)
    # LSN 3 → ch3 gets a decoder (trunk-following); idle LSN 2 does not.
    assert sch.active_labels(_T0) == {"cc", "ch3"}


def test_bank_call_grants_activate_channels():
    sch = TrafficScheduler(_plan(), hang_seconds=4.0)
    ev = BankCallEvent(
        timestamp=_T0, raw_line="bank", bank="One", flag_byte="0x0",
        description="d", lsn_to_tg={2: 100, 4: 200},
    )
    sch.on_event(ev)
    assert sch.active_labels(_T0) == {"cc", "ch2", "ch4"}


def test_max_active_keeps_control_plus_freshest():
    sch = TrafficScheduler(_plan(), hang_seconds=30.0, max_active=2)
    sch.on_event(_voice(0, 1, 9, "ch2"))
    sch.on_event(_voice(1, 2, 9, "ch3"))
    sch.on_event(_voice(2, 3, 9, "ch4"))
    # Budget 2: control (cc) always kept + the single freshest (ch4).
    active = sch.active_labels(_T0 + timedelta(seconds=2))
    assert "cc" in active
    assert "ch4" in active
    assert len(active) == 2
    assert "ch2" not in active


def test_energy_detection_activates_channel():
    sch = TrafficScheduler(_plan(), hang_seconds=4.0, energy_threshold=1.0)
    sch.update_energy({"ch3": 5.0, "ch2": 0.1}, now=_T0)
    assert sch.active_labels(_T0) == {"cc", "ch3"}


def test_replay_uses_event_clock_not_wall_clock():
    """active_labels() with no `now` uses the latest activity time, so a
    replayed capture from 2026 doesn't look 'expired' against today."""
    sch = TrafficScheduler(_plan(), hang_seconds=4.0)
    sch.on_event(_voice(0, 111, 9, "ch2"))
    # No now= passed → reference is the event's own timestamp.
    assert sch.active_labels() == {"cc", "ch2"}


def test_scheduler_follows_decoded_traffic_through_multiplex():
    """End-to-end policy: real parsed events from two channels drive the
    scheduler to activate exactly the channels carrying traffic."""
    import asyncio

    from backend.rf.multiplex import run_multichannel
    from backend.state import StateManager

    plan = _plan()
    sch = TrafficScheduler(plan, hang_seconds=4.0)

    async def _iter(lines):
        for ln in lines:
            yield ln

    # ch2 carries a voice call; ch3/ch4 stay quiet.
    lines = {
        "cc": [" SLOT 1 TGT=9 SRC=1 Group Call \n"],
        "ch2": [" SLOT 1 TGT=9 SRC=111 Group Call \n",
                " SLOT 1 TGT=9 SRC=111 Group Call \n"],
        "ch3": [],
        "ch4": [],
    }
    asyncio.run(run_multichannel(
        plan, StateManager(), lambda ch: _iter(lines[ch.label]),
        on_event=sch.on_event,
    ))
    active = sch.active_labels()
    assert "cc" in active      # control always
    assert "ch2" in active     # decoded traffic
    assert "ch3" not in active and "ch4" not in active
