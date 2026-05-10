"""Parser tests against real DSD-FME captures.

Lines come from tests/sample_dsd_log.txt (Site 2 control + payload, captured
2026-05-09 / 2026-05-10) plus inline NCurses-polluted fixtures with real
\\x1b escapes that can't live in a text file.
"""
from __future__ import annotations

from pathlib import Path

from backend.models import EventType
from backend.parser import DSDLogParser

SAMPLE_LOG = Path(__file__).parent / "sample_dsd_log.txt"


# ===========================================================================
# Phase 1A — control channel
# ===========================================================================


def test_sync_clean_returns_none_but_updates_timestamp():
    p = DSDLogParser()
    assert p.parse_line("21:04:17 Sync: +DMR  [slot1]  slot2  | Color Code=02 | CSBK") is None
    ev = p.parse_line(" SLCO Capacity Plus Site: 2 - Rest LSN: 6 - RS: 00")
    assert ev is not None
    assert (ev.timestamp.hour, ev.timestamp.minute, ev.timestamp.second) == (21, 4, 17)


def test_sync_with_csbk_crc_emits_quality():
    ev = DSDLogParser().parse_line("20:56:22 Sync: +DMR  [slot1]  slot2  | Color Code=02 | CSBK (CRC ERR)")
    assert ev.type == EventType.QUALITY and ev.error_type == "CSBK_CRC"


def test_sync_with_csbk_fec_emits_quality():
    ev = DSDLogParser().parse_line("21:31:38 Sync: +DMR  [slot1]  slot2  | Color Code=02 | CSBK (FEC ERR)")
    assert ev.type == EventType.QUALITY and ev.error_type == "CSBK_FEC"


def test_sync_with_cach_fec_emits_quality():
    ev = DSDLogParser().parse_line("20:56:22 Sync: +DMR   slot1  [slot2] | CACH/Burst FEC ERR")
    assert ev.type == EventType.QUALITY and ev.error_type == "CACH_BURST_FEC"


def test_channel_status_single_block():
    ev = DSDLogParser().parse_line(
        " Capacity Plus Channel Status - FL: 3 TS: 1 RS: 0 - Rest LSN: 6 - Single Block"
    )
    assert ev.type == EventType.CHANNEL_STATUS
    assert ev.fl == 3 and ev.ts == 1 and ev.rs == 0 and ev.rest_lsn == 6
    assert ev.block_type == "Single"


def test_channel_status_initial_and_final():
    p = DSDLogParser()
    initial = p.parse_line(" Capacity Plus Channel Status - FL: 2 TS: 0 RS: 0 - Rest LSN: 3 - Initial Block")
    final = p.parse_line(" Capacity Plus Channel Status - FL: 1 TS: 0 RS: 0 - Rest LSN: 3 - Final Block")
    assert initial.block_type == "Initial" and final.block_type == "Final"


def test_bank_one_with_two_active_calls():
    ev = DSDLogParser().parse_line(
        " Bank One F80 Private or Data Call(s) -  LSN 05: TGT 215; LSN 06: TGT 64250;"
    )
    assert ev.type == EventType.BANK_CALL
    assert ev.bank == "One" and ev.flag_byte == "F80"
    assert ev.description == "Private or Data"
    assert ev.lsn_to_tg == {5: 215, 6: 64250}


def test_bank_one_with_single_active_call():
    ev = DSDLogParser().parse_line(" Bank One F80 Private or Data Call(s) -  LSN 05: TGT 215;")
    assert ev.lsn_to_tg == {5: 215}


def test_lsn_status_all_idle_and_rest():
    ev = DSDLogParser().parse_line("  LSN 01:  Idle;  LSN 02:  Idle;  LSN 03:  Rest;  LSN 04:  Idle;")
    assert ev.type == EventType.LSN_STATUS
    by_lsn = {s.lsn: s for s in ev.states}
    assert by_lsn[1].state == "Idle"
    assert by_lsn[3].state == "Rest"


def test_lsn_status_active_with_tg_numbers():
    ev = DSDLogParser().parse_line("  LSN 05:   215;  LSN 06: 64250;  LSN 07:  Idle;  LSN 08:  Idle;")
    by_lsn = {s.lsn: s for s in ev.states}
    assert by_lsn[5].state == "Active" and by_lsn[5].tg == 215
    assert by_lsn[6].state == "Active" and by_lsn[6].tg == 64250
    assert by_lsn[7].state == "Idle" and by_lsn[7].tg is None


def test_lsn_status_does_not_match_bank_call_line():
    ev = DSDLogParser().parse_line(
        " Bank One F80 Private or Data Call(s) -  LSN 05: TGT 215; LSN 06: TGT 64250;"
    )
    assert ev.type == EventType.BANK_CALL


def test_slco_site_info():
    ev = DSDLogParser().parse_line(" SLCO Capacity Plus Site: 2 - Rest LSN: 6 - RS: 00")
    assert ev.type == EventType.SITE_INFO
    assert ev.site == 2 and ev.rest_lsn == 6 and ev.rs == "00"


def test_standalone_slco_crc_err():
    ev = DSDLogParser().parse_line(" SLCO CRC ERR")
    assert ev.type == EventType.QUALITY and ev.error_type == "SLCO_CRC"


# ===========================================================================
# Phase 1B — payload channel
# ===========================================================================


def test_voice_call_group():
    ev = DSDLogParser().parse_line(" SLOT 2 TGT=1 SRC=2102 Group Call  ")
    assert ev.type == EventType.VOICE_CALL
    assert ev.slot == 2 and ev.src == 2102 and ev.tgt == 1
    assert ev.is_cap_plus is False and ev.is_txi is False
    assert ev.rest_lsn is None


def test_voice_call_group_txi():
    ev = DSDLogParser().parse_line(" SLOT 2 TGT=1 SRC=223 Group TXI Call  ")
    assert ev.type == EventType.VOICE_CALL
    assert ev.is_txi is True and ev.is_cap_plus is False
    assert ev.src == 223


def test_voice_call_capplus_group_with_rest_lsn():
    ev = DSDLogParser().parse_line(" SLOT 2 TGT=1 SRC=0 Cap+ Group Call  Rest LSN: 5 ")
    assert ev.type == EventType.VOICE_CALL
    assert ev.is_cap_plus is True and ev.is_txi is False
    assert ev.rest_lsn == 5


def test_voice_call_capplus_group_txi():
    ev = DSDLogParser().parse_line(" SLOT 2 TGT=1 SRC=2102 Cap+ Group TXI Call  Rest LSN: 5 ")
    assert ev.type == EventType.VOICE_CALL
    assert ev.is_cap_plus is True and ev.is_txi is True
    assert ev.src == 2102 and ev.rest_lsn == 5


def test_voice_call_private_target_is_radio_id():
    """When TGT is another radio id (not a TG), it still parses; downstream
    distinguishes group vs private by checking the addressing/ID range."""
    ev = DSDLogParser().parse_line(" SLOT 2 TGT=2102 SRC=64250 Group Call  ")
    assert ev.tgt == 2102 and ev.src == 64250


def test_preamble_csbk_data():
    ev = DSDLogParser().parse_line(
        " Preamble CSBK - Individual Data - Source: 199 - Target: 64250 - Rest LSN: 6"
    )
    assert ev.type == EventType.PREAMBLE_CSBK
    assert ev.addressing == "Individual" and ev.kind == "Data"
    assert ev.src == 199 and ev.tgt == 64250 and ev.rest_lsn == 6


def test_preamble_csbk_kind_csbk():
    ev = DSDLogParser().parse_line(
        " Preamble CSBK - Individual CSBK - Source: 64250 - Target: 2102 - Rest LSN: 5"
    )
    assert ev.kind == "CSBK" and ev.src == 64250 and ev.tgt == 2102


def test_preamble_csbk_without_rest_lsn():
    ev = DSDLogParser().parse_line(" Preamble CSBK - Individual Data - Source: 199 - Target: 64250")
    assert ev.rest_lsn is None


def test_data_header_unconfirmed():
    ev = DSDLogParser().parse_line(
        " Slot 1 Data Header - Indiv - Unconfirmed Delivery - Source: 199 Target: 64250 "
    )
    assert ev.type == EventType.DATA_HEADER
    assert ev.slot == 1 and ev.addressing == "Indiv"
    assert ev.delivery == "Unconfirmed Delivery"
    assert ev.response_requested is False
    assert ev.src == 199 and ev.tgt == 64250


def test_data_header_confirmed_response_requested():
    ev = DSDLogParser().parse_line(
        " Slot 1 Data Header - Indiv - Confirmed Delivery - Response Requested - Source: 199 Target: 64250 "
    )
    assert ev.delivery == "Confirmed Delivery"
    assert ev.response_requested is True


def test_data_header_response_packet():
    ev = DSDLogParser().parse_line(
        " Slot 1 Data Header - Indiv - Response Packet - Source: 64250 Target: 199 "
    )
    assert ev.delivery == "Response Packet"
    assert ev.response_requested is False


def test_ip_mapping_src_strips_leading_zeros_in_radio_id():
    ev = DSDLogParser().parse_line(" SRC(24): 00000068; IP: 012.000.000.068; Port: 4001; ")
    assert ev.type == EventType.IP_MAPPING
    assert ev.role == "SRC" and ev.radio_id == 68
    assert ev.ip == "012.000.000.068" and ev.port == 4001


def test_ip_mapping_dst():
    ev = DSDLogParser().parse_line(" DST(24): 00064250; IP: 013.000.250.250; Port: 4001; ")
    assert ev.role == "DST" and ev.radio_id == 64250


def test_lrrp_position_stitches_to_recent_src():
    p = DSDLogParser()
    p.parse_line(" SRC(24): 00000068; IP: 012.000.000.068; Port: 4001; ")
    p.parse_line(" DST(24): 00064250; IP: 013.000.250.250; Port: 4001; ")
    ev = p.parse_line(" Lat: 32.10128 Lon: 34.87151 (32.10128, 34.87151)")
    assert ev.type == EventType.LRRP_POSITION
    assert ev.src == 68
    assert ev.lat == 32.10128 and ev.lon == 34.87151


def test_lrrp_position_consumes_src_binding():
    """A second Lat: without a fresh SRC(24) must NOT reuse the previous src."""
    p = DSDLogParser()
    p.parse_line(" SRC(24): 00000068; IP: 012.000.000.068; Port: 4001; ")
    first = p.parse_line(" Lat: 32.10128 Lon: 34.87151")
    second = p.parse_line(" Lat: 31.50000 Lon: 35.00000")
    assert first.src == 68
    assert second.src is None


def test_lrrp_request():
    ev = DSDLogParser().parse_line(" LRRP SRC: 199; Request from TGT: 64250;")
    assert ev.type == EventType.LRRP_REQUEST
    assert ev.src == 199 and ev.tgt == 64250 and ev.direction == "Request"


def test_lrrp_response():
    ev = DSDLogParser().parse_line(" LRRP SRC: 199; Response to TGT: 64250;")
    assert ev.direction == "Response"


def test_encryption_protected_lc():
    ev = DSDLogParser().parse_line(" SLOT 1 Protected LC  FLCO=0x04 FID=0x80 ")
    assert ev.type == EventType.ENCRYPTION
    assert ev.slot == 1 and ev.flco == "0x04" and ev.fid == "0x80"


# ===========================================================================
# ANSI / NCurses escape handling (lines from `dsd-fme -N` runs)
# ===========================================================================


def test_ansi_polluted_sync_line_parses_and_sets_timestamp():
    p = DSDLogParser()
    # Real escape bytes: cursor positioning + charset designate, then content.
    polluted = "\x1b[16;18H5\x1b[24;80H\x1b(B09:00:25 Sync: +DMR   slot1  [slot2] | Color Code=02 | CSBK"
    assert p.parse_line(polluted) is None  # sync emits no event but updates ts
    follow = p.parse_line(" SLCO Capacity Plus Site: 2 - Rest LSN: 4 - RS: 00")
    assert follow.timestamp.hour == 9 and follow.timestamp.minute == 0


def test_ansi_polluted_voice_call_line_parses():
    polluted = "\x1b[24;1H\x1b[K SLOT 2 TGT=1 SRC=2102 Group Call  \x1b[0m"
    ev = DSDLogParser().parse_line(polluted)
    assert ev.type == EventType.VOICE_CALL
    assert ev.slot == 2 and ev.src == 2102 and ev.tgt == 1


def test_ansi_polluted_lat_lon_line_parses():
    p = DSDLogParser()
    p.parse_line(" SRC(24): 00000074; IP: 012.000.000.074; Port: 4001; ")
    ev = p.parse_line("\x1b[16;18H6\x1b[24;80H\x1b(B Lat: 32.07383 Lon: 34.87025 (32.07383, 34.87025)\x1b[0m")
    assert ev.type == EventType.LRRP_POSITION
    assert ev.src == 74 and ev.lat == 32.07383 and ev.lon == 34.87025


# ===========================================================================
# Negative cases & whole-file regression
# ===========================================================================


def test_blank_and_header_lines_return_none():
    p = DSDLogParser()
    assert p.parse_line("") is None
    assert p.parse_line("   ") is None
    assert p.parse_line("Build Version: AW 2026-26-ged1d1d6") is None
    assert p.parse_line("Pulse Input Device: dmr_capture.monitor;") is None


def test_sample_log_parses_with_known_event_types_only():
    """Every Phase 1A and 1B event type appears at least once in the corpus."""
    p = DSDLogParser()
    events = []
    with SAMPLE_LOG.open() as f:
        for raw in f:
            if raw.lstrip().startswith("#"):
                continue
            ev = p.parse_line(raw)
            if ev is not None:
                events.append(ev)
    seen = {e.type for e in events}
    expected = {
        # Phase 1A
        EventType.SITE_INFO,
        EventType.CHANNEL_STATUS,
        EventType.LSN_STATUS,
        EventType.BANK_CALL,
        EventType.QUALITY,
        # Phase 1B
        EventType.VOICE_CALL,
        EventType.PREAMBLE_CSBK,
        EventType.DATA_HEADER,
        EventType.IP_MAPPING,
        EventType.LRRP_POSITION,
        EventType.LRRP_REQUEST,
        EventType.ENCRYPTION,
    }
    missing = expected - seen
    assert not missing, f"sample log did not produce events of types: {missing}"
