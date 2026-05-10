"""Parser tests against real DSD-FME captures from a Cap+ control channel.

Lines come from tests/sample_dsd_log.txt (Site 2, captured 2026-05-10).
"""
from __future__ import annotations

from pathlib import Path

from backend.models import EventType
from backend.parser import DSDLogParser

SAMPLE_LOG = Path(__file__).parent / "sample_dsd_log.txt"


# --- Sync line: timestamp tracking + quality flags ---

def test_sync_clean_returns_none_but_updates_timestamp():
    p = DSDLogParser()
    line = "21:04:17 Sync: +DMR  [slot1]  slot2  | Color Code=02 | CSBK"
    assert p.parse_line(line) is None
    # Subsequent line should inherit 21:04:17 in its timestamp
    ev = p.parse_line(" SLCO Capacity Plus Site: 2 - Rest LSN: 6 - RS: 00")
    assert ev is not None
    assert ev.timestamp.hour == 21
    assert ev.timestamp.minute == 4
    assert ev.timestamp.second == 17


def test_sync_with_csbk_crc_emits_quality():
    p = DSDLogParser()
    line = "20:56:22 Sync: +DMR  [slot1]  slot2  | Color Code=02 | CSBK (CRC ERR)"
    ev = p.parse_line(line)
    assert ev is not None
    assert ev.type == EventType.QUALITY
    assert ev.error_type == "CSBK_CRC"


def test_sync_with_cach_fec_emits_quality():
    p = DSDLogParser()
    line = "20:56:22 Sync: +DMR   slot1  [slot2] | CACH/Burst FEC ERR"
    ev = p.parse_line(line)
    assert ev is not None
    assert ev.type == EventType.QUALITY
    assert ev.error_type == "CACH_BURST_FEC"


# --- Channel Status header ---

def test_channel_status_single_block():
    p = DSDLogParser()
    line = " Capacity Plus Channel Status - FL: 3 TS: 1 RS: 0 - Rest LSN: 6 - Single Block"
    ev = p.parse_line(line)
    assert ev is not None
    assert ev.type == EventType.CHANNEL_STATUS
    assert ev.fl == 3
    assert ev.ts == 1
    assert ev.rs == 0
    assert ev.rest_lsn == 6
    assert ev.block_type == "Single"


def test_channel_status_initial_and_final():
    p = DSDLogParser()
    initial = p.parse_line(" Capacity Plus Channel Status - FL: 2 TS: 0 RS: 0 - Rest LSN: 3 - Initial Block")
    final = p.parse_line(" Capacity Plus Channel Status - FL: 1 TS: 0 RS: 0 - Rest LSN: 3 - Final Block")
    assert initial.block_type == "Initial"
    assert final.block_type == "Final"
    assert initial.rest_lsn == 3 and final.rest_lsn == 3


# --- Bank announcements ---

def test_bank_one_with_two_active_calls():
    p = DSDLogParser()
    line = " Bank One F80 Private or Data Call(s) -  LSN 05: TGT 215; LSN 06: TGT 64250;"
    ev = p.parse_line(line)
    assert ev is not None
    assert ev.type == EventType.BANK_CALL
    assert ev.bank == "One"
    assert ev.flag_byte == "F80"
    assert ev.description == "Private or Data"
    assert ev.lsn_to_tg == {5: 215, 6: 64250}


def test_bank_one_with_single_active_call():
    p = DSDLogParser()
    line = " Bank One F80 Private or Data Call(s) -  LSN 05: TGT 215;"
    ev = p.parse_line(line)
    assert ev is not None
    assert ev.lsn_to_tg == {5: 215}


# --- LSN status snapshots ---

def test_lsn_status_all_idle_and_rest():
    p = DSDLogParser()
    line = "  LSN 01:  Idle;  LSN 02:  Idle;  LSN 03:  Rest;  LSN 04:  Idle;"
    ev = p.parse_line(line)
    assert ev is not None
    assert ev.type == EventType.LSN_STATUS
    assert len(ev.states) == 4
    assert ev.states[0].lsn == 1 and ev.states[0].state == "Idle"
    assert ev.states[2].lsn == 3 and ev.states[2].state == "Rest"


def test_lsn_status_active_with_tg_numbers():
    p = DSDLogParser()
    line = "  LSN 05:   215;  LSN 06: 64250;  LSN 07:  Idle;  LSN 08:  Idle;"
    ev = p.parse_line(line)
    assert ev is not None
    assert ev.type == EventType.LSN_STATUS
    by_lsn = {s.lsn: s for s in ev.states}
    assert by_lsn[5].state == "Active" and by_lsn[5].tg == 215
    assert by_lsn[6].state == "Active" and by_lsn[6].tg == 64250
    assert by_lsn[7].state == "Idle" and by_lsn[7].tg is None


def test_lsn_status_does_not_match_bank_call_line():
    """Bank-call line also contains LSN tokens — must not double-emit."""
    p = DSDLogParser()
    line = " Bank One F80 Private or Data Call(s) -  LSN 05: TGT 215; LSN 06: TGT 64250;"
    ev = p.parse_line(line)
    assert ev.type == EventType.BANK_CALL  # not LSN_STATUS


# --- SLCO site info ---

def test_slco_site_info():
    p = DSDLogParser()
    line = " SLCO Capacity Plus Site: 2 - Rest LSN: 6 - RS: 00"
    ev = p.parse_line(line)
    assert ev is not None
    assert ev.type == EventType.SITE_INFO
    assert ev.site == 2
    assert ev.rest_lsn == 6
    assert ev.rs == "00"


# --- Standalone quality errors ---

def test_standalone_slco_crc_err():
    p = DSDLogParser()
    line = " SLCO CRC ERR"
    ev = p.parse_line(line)
    assert ev is not None
    assert ev.type == EventType.QUALITY
    assert ev.error_type == "SLCO_CRC"


# --- Negative cases ---

def test_blank_and_comment_lines_return_none():
    p = DSDLogParser()
    assert p.parse_line("") is None
    assert p.parse_line("   ") is None
    assert p.parse_line("Build Version: AW 2026-26-ged1d1d6") is None
    assert p.parse_line("Pulse Input Device: dmr_capture.monitor;") is None


# --- Whole-file regression: the real sample log produces nonzero events
#     and only known event types. ---

def test_sample_log_parses_with_known_event_types_only():
    p = DSDLogParser()
    events = []
    with SAMPLE_LOG.open() as f:
        for raw in f:
            if raw.lstrip().startswith("#"):
                continue  # skip our annotation comments in the sample file
            ev = p.parse_line(raw)
            if ev is not None:
                events.append(ev)
    # Should have produced at least one of every Phase 1A event type
    seen_types = {e.type for e in events}
    assert EventType.SITE_INFO in seen_types
    assert EventType.CHANNEL_STATUS in seen_types
    assert EventType.LSN_STATUS in seen_types
    assert EventType.BANK_CALL in seen_types
    assert EventType.QUALITY in seen_types
