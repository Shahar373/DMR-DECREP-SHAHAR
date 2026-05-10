"""Parser tests against synthetic DSD-FME-style lines.

These will be supplemented (or replaced) with real lines once
tests/sample_dsd_log.txt is populated from a live capture.
"""
from __future__ import annotations

from backend.models import EventType
from backend.parser import DSDLogParser


def test_voice_start_parsed():
    p = DSDLogParser()
    line = "Sync: +DMR Slot 2 Group Voice CC=1 SRC=1234567 TGT=8888"
    ev = p.parse_line(line)
    assert ev is not None
    assert ev.type == EventType.VOICE_START
    assert ev.slot == 2
    assert ev.src_id == 1234567
    assert ev.tgt_id == 8888


def test_voice_end_only_after_voice_start():
    p = DSDLogParser()
    # SLOT IDLE alone, no preceding voice → ignored
    assert p.parse_line("Sync: +DMR Slot 1 [SLOT IDLE]") is None
    # Now start a call on slot 1
    p.parse_line("Sync: +DMR Slot 1 Group Voice CC=1 SRC=42 TGT=7")
    end = p.parse_line("Sync: +DMR Slot 1 [SLOT IDLE]")
    assert end is not None
    assert end.type == EventType.VOICE_END
    assert end.slot == 1
    # And after end, another idle is again ignored
    assert p.parse_line("Sync: +DMR Slot 1 [SLOT IDLE]") is None


def test_voice_end_pairing_per_slot():
    p = DSDLogParser()
    p.parse_line("Slot 1 Group Voice SRC=11 TGT=100")
    p.parse_line("Slot 2 Group Voice SRC=22 TGT=200")
    e1 = p.parse_line("Slot 1 [SLOT IDLE]")
    e2 = p.parse_line("Slot 2 [SLOT IDLE]")
    assert e1 is not None and e1.slot == 1
    assert e2 is not None and e2.slot == 2


def test_lrrp_parsed():
    p = DSDLogParser()
    line = "Slot 2 LRRP ID=1234567 Latitude=32.082145 Longitude=34.789432"
    ev = p.parse_line(line)
    assert ev is not None
    assert ev.type == EventType.LRRP
    assert ev.src_id == 1234567
    assert ev.lat == 32.082145
    assert ev.lon == 34.789432


def test_lrrp_negative_coords():
    p = DSDLogParser()
    ev = p.parse_line("LRRP SRC=99 Latitude=-12.34 Longitude=-56.78")
    assert ev is not None
    assert ev.lat == -12.34
    assert ev.lon == -56.78


def test_ars_registered():
    p = DSDLogParser()
    ev = p.parse_line("ARS Registration: SRC=1234567 status=registered")
    assert ev is not None
    assert ev.type == EventType.ARS
    assert ev.src_id == 1234567
    assert ev.registered is True


def test_ars_deregistered():
    p = DSDLogParser()
    ev = p.parse_line("ARS SRC=42 deregistered")
    assert ev is not None
    assert ev.type == EventType.ARS
    assert ev.registered is False


def test_csbk_with_src():
    p = DSDLogParser()
    ev = p.parse_line("CSBK Preamble SRC=999")
    assert ev is not None
    assert ev.type == EventType.CSBK
    assert ev.src_id == 999


def test_csbk_without_src():
    p = DSDLogParser()
    ev = p.parse_line("CSBK control burst")
    assert ev is not None
    assert ev.type == EventType.CSBK
    assert ev.src_id is None


def test_encryption_pi_header():
    p = DSDLogParser()
    ev = p.parse_line("PI Header detected on Slot 1")
    assert ev is not None
    assert ev.type == EventType.ENCRYPTION


def test_encryption_alg_id_nonzero():
    p = DSDLogParser()
    ev = p.parse_line("ALG ID = 0x21")
    assert ev is not None
    assert ev.type == EventType.ENCRYPTION
    assert ev.alg_id == "0x21"


def test_encryption_alg_id_zero_ignored():
    p = DSDLogParser()
    assert p.parse_line("ALG ID = 0x00") is None


def test_unmatched_line_returns_none():
    p = DSDLogParser()
    assert p.parse_line("some unrelated dsd-fme banner output") is None


def test_blank_line_returns_none():
    p = DSDLogParser()
    assert p.parse_line("") is None
    assert p.parse_line("   \n") is None
