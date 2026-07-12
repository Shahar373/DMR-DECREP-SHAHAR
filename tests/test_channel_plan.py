"""Phase-7 tests: channel plan model + geometry helpers."""
from __future__ import annotations

import json

import pytest

from backend.channel_plan import Channel, ChannelPlan, load_channel_plan


def _plan(*freqs, **kw):
    chans = [Channel(label=f"ch{i}", frequency_hz=f) for i, f in enumerate(freqs)]
    return ChannelPlan(channels=chans, **kw)


def test_channel_validation():
    with pytest.raises(ValueError):
        Channel(label="  ", frequency_hz=168_500_000)
    with pytest.raises(ValueError):
        Channel(label="x", frequency_hz=5)  # far below range


def test_duplicate_labels_rejected():
    with pytest.raises(ValueError):
        ChannelPlan(channels=[
            Channel(label="a", frequency_hz=168_500_000),
            Channel(label="a", frequency_hz=168_512_500),
        ])


def test_span_center_and_fit():
    plan = _plan(168_500_000, 168_512_500, 168_525_000)
    assert plan.span_hz() == 25_000
    assert plan.center_hz() == 168_512_500
    assert plan.fits_in_bandwidth(2_000_000) is True
    # A plan spanning 12 MHz does not fit one RSP.
    wide = _plan(160_000_000, 172_000_000)
    assert wide.span_hz() == 12_000_000
    assert wide.fits_in_bandwidth(10_000_000) is False


def test_single_channel_span_is_zero_and_fits():
    plan = _plan(168_500_000)
    assert plan.span_hz() == 0.0
    assert plan.center_hz() == 168_500_000
    assert plan.fits_in_bandwidth(1) is True


def test_lsn_map_and_control_channels():
    plan = ChannelPlan(channels=[
        Channel(label="cc", frequency_hz=168_500_000, lsn=1, control=True),
        Channel(label="ch2", frequency_hz=168_512_500, lsn=2),
        Channel(label="ch3", frequency_hz=168_525_000),  # no lsn
    ])
    assert plan.lsn_to_frequency() == {1: 168_500_000, 2: 168_512_500}
    assert [c.label for c in plan.control_channels()] == ["cc"]
    assert plan.by_label("ch2").frequency_hz == 168_512_500
    assert plan.by_label("nope") is None


def test_load_from_json(tmp_path):
    p = tmp_path / "plan.json"
    p.write_text(json.dumps({"channels": [
        {"label": "cc", "frequency_hz": 168_500_000, "lsn": 1, "control": True},
        {"label": "ch2", "frequency_hz": 168_512_500, "lsn": 2},
    ]}))
    plan = load_channel_plan(p)
    assert len(plan.channels) == 2
    assert plan.control_channels()[0].label == "cc"
