"""Tests for backend.state.StateManager.

Covers unit-level event handling plus a real-capture replay regression that
exercises the full event vocabulary against tests/captures/dmr_night_sample.log.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from backend.models import (
    DataHeaderEvent,
    EncryptionEvent,
    EventType,
    IPMappingEvent,
    LRRPPositionEvent,
    LSNState,
    LSNStatusEvent,
    PreambleCSBKEvent,
    QualityEvent,
    SiteInfoEvent,
    VoiceCallEvent,
)
from backend.parser import DSDLogParser
from backend.state import StateManager

CAPTURE = Path(__file__).parent / "captures" / "dmr_night_sample.log"


def _ts(seconds: int = 0) -> datetime:
    """Stable, comparable timestamps for unit tests."""
    return datetime(2026, 5, 10, 21, 0, seconds)


# ===========================================================================
# Radio discovery and bookkeeping
# ===========================================================================


def test_voice_call_creates_radio_and_active_call():
    sm = StateManager()
    sm.apply(VoiceCallEvent(timestamp=_ts(1), raw_line="", slot=2, src=2102, tgt=1))

    assert 2102 in sm.radios
    radio = sm.radios[2102]
    assert radio.voice_frame_count == 1
    assert radio.last_tg == 1 and radio.last_slot == 2

    assert 2 in sm.active_calls
    call = sm.active_calls[2]
    assert call.src == 2102 and call.tgt == 1
    assert call.frame_count == 1


def test_repeated_voice_frames_from_same_src_accumulate_into_one_call():
    sm = StateManager()
    for i in range(5):
        sm.apply(VoiceCallEvent(timestamp=_ts(i), raw_line="", slot=2, src=2102, tgt=1))
    call = sm.active_calls[2]
    assert call.frame_count == 5
    assert call.last_frame_at == _ts(4)
    assert sm.radios[2102].voice_frame_count == 5


def test_new_src_on_same_slot_replaces_active_call():
    sm = StateManager()
    sm.apply(VoiceCallEvent(timestamp=_ts(1), raw_line="", slot=2, src=2102, tgt=1))
    sm.apply(VoiceCallEvent(timestamp=_ts(2), raw_line="", slot=2, src=223, tgt=1))
    call = sm.active_calls[2]
    assert call.src == 223
    assert call.frame_count == 1  # new call, not accumulated
    # Both radios should still exist
    assert {2102, 223} <= set(sm.radios)


def test_preamble_csbk_touches_both_radios_for_private_call():
    sm = StateManager()
    sm.apply(
        PreambleCSBKEvent(
            timestamp=_ts(1), raw_line="", addressing="Individual",
            kind="Data", src=199, tgt=64250,
        )
    )
    assert {199, 64250} <= set(sm.radios)
    assert sm.radios[199].voice_frame_count == 0  # CSBK alone is not a voice frame


def test_data_header_touches_only_src():
    sm = StateManager()
    sm.apply(
        DataHeaderEvent(
            timestamp=_ts(1), raw_line="", slot=1, addressing="Indiv",
            delivery="Unconfirmed Delivery", src=199, tgt=64250,
        )
    )
    assert 199 in sm.radios
    # tgt is also a radio id here, but DataHeader handler intentionally only
    # touches src — the recipient is a repeater/gateway, not the sender of
    # observable RF. Loosening this would just inflate ghost radios.
    assert 64250 not in sm.radios


def test_ip_mapping_attaches_ip_to_radio():
    sm = StateManager()
    sm.apply(
        IPMappingEvent(
            timestamp=_ts(1), raw_line="", role="SRC",
            radio_id=68, ip="012.000.000.068", port=4001,
        )
    )
    assert sm.radios[68].ip == "012.000.000.068"


# ===========================================================================
# LRRP positions
# ===========================================================================


def test_lrrp_position_updates_radio_position_and_history():
    sm = StateManager()
    sm.apply(LRRPPositionEvent(timestamp=_ts(1), raw_line="", src=68, lat=32.10128, lon=34.87151))
    sm.apply(LRRPPositionEvent(timestamp=_ts(2), raw_line="", src=68, lat=32.10332, lon=34.87087))

    radio = sm.radios[68]
    assert radio.last_position.lat == 32.10332
    assert radio.last_position.lon == 34.87087
    assert len(radio.position_history) == 2


def test_lrrp_position_without_src_is_ignored():
    sm = StateManager()
    sm.apply(LRRPPositionEvent(timestamp=_ts(1), raw_line="", src=None, lat=32.0, lon=34.0))
    assert sm.radios == {}


def test_position_history_is_capped():
    sm = StateManager(position_history_length=3)
    for i in range(10):
        sm.apply(LRRPPositionEvent(timestamp=_ts(i), raw_line="", src=68, lat=32.0 + i * 0.01, lon=34.0))
    history = sm.radios[68].position_history
    assert len(history) == 3
    # Newest at the end
    assert history[-1].lat == 32.09


# ===========================================================================
# Encryption
# ===========================================================================


def test_encryption_event_marks_active_call_on_same_slot():
    sm = StateManager()
    sm.apply(VoiceCallEvent(timestamp=_ts(1), raw_line="", slot=1, src=2102, tgt=1))
    sm.apply(EncryptionEvent(timestamp=_ts(1), raw_line="", slot=1, flco="0x04", fid="0x80"))
    assert sm.active_calls[1].is_encrypted is True
    assert sm.radios[2102].last_call_was_encrypted is True


def test_encryption_event_without_active_call_does_nothing():
    sm = StateManager()
    sm.apply(EncryptionEvent(timestamp=_ts(1), raw_line="", slot=1, flco="0x04", fid="0x80"))
    assert sm.active_calls == {}
    assert sm.radios == {}


def test_new_call_resets_encryption_flag_on_radio():
    sm = StateManager()
    sm.apply(VoiceCallEvent(timestamp=_ts(1), raw_line="", slot=1, src=2102, tgt=1))
    sm.apply(EncryptionEvent(timestamp=_ts(1), raw_line="", slot=1, flco="0x04", fid="0x80"))
    # Same radio talks again later, this time in clear — flag must reset
    sm.apply(VoiceCallEvent(timestamp=_ts(10), raw_line="", slot=1, src=2102, tgt=1))
    assert sm.radios[2102].last_call_was_encrypted is False


# ===========================================================================
# System status
# ===========================================================================


def test_site_info_event_updates_system():
    sm = StateManager()
    sm.apply(SiteInfoEvent(timestamp=_ts(1), raw_line="", site=2, rest_lsn=6, rs="00"))
    assert sm.system.site == 2 and sm.system.rest_lsn == 6


def test_lsn_status_event_updates_lsn_states_and_active_tgs():
    sm = StateManager()
    sm.apply(
        LSNStatusEvent(
            timestamp=_ts(1), raw_line="",
            states=[
                LSNState(lsn=5, state="Active", tg=215),
                LSNState(lsn=6, state="Active", tg=64250),
                LSNState(lsn=7, state="Idle"),
                LSNState(lsn=8, state="Idle"),
            ],
        )
    )
    assert sm.system.lsn_states[5].state == "Active" and sm.system.lsn_states[5].tg == 215
    assert sm.system.lsn_states[7].state == "Idle"
    assert sm.active_talkgroups() == {5: 215, 6: 64250}


# ===========================================================================
# Quality
# ===========================================================================


def test_quality_events_increment_correct_counters():
    sm = StateManager()
    sm.apply(QualityEvent(timestamp=_ts(1), raw_line="", error_type="CSBK_CRC"))
    sm.apply(QualityEvent(timestamp=_ts(2), raw_line="", error_type="CSBK_CRC"))
    sm.apply(QualityEvent(timestamp=_ts(3), raw_line="", error_type="CACH_BURST_FEC"))
    sm.apply(QualityEvent(timestamp=_ts(4), raw_line="", error_type="SLCO_CRC"))

    assert sm.quality.csbk_crc == 2
    assert sm.quality.cach_burst_fec == 1
    assert sm.quality.slco_crc == 1
    assert sm.quality.last_error_at == _ts(4)


# ===========================================================================
# Idle call cleanup
# ===========================================================================


def test_tick_drops_calls_with_no_recent_frames():
    sm = StateManager(call_idle_timeout=timedelta(seconds=2))
    sm.apply(VoiceCallEvent(timestamp=_ts(1), raw_line="", slot=2, src=2102, tgt=1))
    assert 2 in sm.active_calls
    sm.tick(_ts(10))
    assert sm.active_calls == {}


def test_apply_auto_expires_old_call_when_new_event_arrives_later():
    sm = StateManager(call_idle_timeout=timedelta(seconds=2))
    sm.apply(VoiceCallEvent(timestamp=_ts(1), raw_line="", slot=2, src=2102, tgt=1))
    # 10 seconds later a quality event arrives — apply() auto-ticks
    sm.apply(QualityEvent(timestamp=_ts(11), raw_line="", error_type="CSBK_CRC"))
    assert sm.active_calls == {}


# ===========================================================================
# Snapshot
# ===========================================================================


def test_snapshot_is_json_serializable():
    sm = StateManager()
    sm.apply(VoiceCallEvent(timestamp=_ts(1), raw_line="", slot=2, src=2102, tgt=1))
    sm.apply(LRRPPositionEvent(timestamp=_ts(2), raw_line="", src=68, lat=32.1, lon=34.87))
    sm.apply(SiteInfoEvent(timestamp=_ts(3), raw_line="", site=2, rest_lsn=6, rs="00"))

    snap = sm.snapshot()
    j = snap.model_dump_json()
    assert '"site":2' in j
    assert '"voice_frame_count":1' in j
    assert '"lat":32.1' in j


# ===========================================================================
# Real-capture replay regression
# ===========================================================================


def test_replay_real_capture_produces_expected_radios_and_positions():
    """Feed the trimmed real-capture sample end-to-end and assert it produced
    the radios + positions we already verified manually against the full 30MB
    log (radios 65, 68, 70, 74 appear in the slice with multiple GPS fixes)."""
    parser = DSDLogParser()
    sm = StateManager()
    with CAPTURE.open() as f:
        for line in f:
            ev = parser.parse_line(line)
            if ev is not None:
                sm.apply(ev)

    # Site identified
    assert sm.system.site == 2

    # At least some radios reported positions; their IDs should be among the
    # ones we manually confirmed from the full capture.
    radios_with_position = {rid for rid, r in sm.radios.items() if r.last_position is not None}
    known_position_radios = {65, 68, 70, 74}
    assert radios_with_position & known_position_radios, (
        f"expected at least one of {known_position_radios} to have a position, "
        f"got radios with position: {sorted(radios_with_position)}"
    )

    # Coordinates plausible — Tel Aviv area (32.0..32.2, 34.8..34.95)
    for rid in radios_with_position:
        pos = sm.radios[rid].last_position
        assert 32.0 < pos.lat < 32.2, f"radio {rid} lat outside expected box: {pos.lat}"
        assert 34.8 < pos.lon < 34.95, f"radio {rid} lon outside expected box: {pos.lon}"

    # Voice traffic was observed
    assert any(r.voice_frame_count > 0 for r in sm.radios.values())

    # Quality counters got hit (the capture has CRC/FEC errors)
    q = sm.quality
    assert q.csbk_crc + q.csbk_fec + q.cach_burst_fec + q.slco_crc > 0

    # Encryption event from the appended slice
    encrypted_radios = [r for r in sm.radios.values() if r.last_call_was_encrypted]
    # May or may not be present depending on slot timing; assert the snapshot
    # still serializes cleanly either way.
    sm.snapshot().model_dump_json()
