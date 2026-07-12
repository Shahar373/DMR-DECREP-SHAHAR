"""Phase-8 tests: per-channel energy detection with synthetic IQ."""
from __future__ import annotations

import numpy as np

from backend.channel_plan import Channel, ChannelPlan
from backend.rf.energy import active_by_energy, channel_powers


def _plan():
    return ChannelPlan(channels=[
        Channel(label="a", frequency_hz=168_490_000),   # -10 kHz
        Channel(label="b", frequency_hz=168_500_000),   #   0 kHz (center)
        Channel(label="c", frequency_hz=168_510_000),   # +10 kHz
    ])


def test_channel_powers_localise_a_tone():
    plan = _plan()
    fs = 1_000_000.0
    n = 65536
    t = np.arange(n) / fs
    # A tone exactly at channel 'c' (+10 kHz from the 168.5 MHz center).
    iq = np.exp(2j * np.pi * 10_000 * t).astype(np.complex64)
    powers = channel_powers(iq, fs, plan, channel_bw_hz=6_000)
    assert set(powers) == {"a", "b", "c"}
    # Channel 'c' carries essentially all the energy.
    assert powers["c"] > 20 * powers["a"]
    assert powers["c"] > 20 * powers["b"]


def test_channel_powers_empty_block():
    plan = _plan()
    powers = channel_powers(np.zeros(0, dtype=np.complex64), 1e6, plan)
    assert powers == {"a": 0.0, "b": 0.0, "c": 0.0}


def test_active_by_energy_threshold():
    powers = {"a": 0.001, "b": 0.5, "c": 2.0}
    assert active_by_energy(powers, threshold=0.4) == {"b", "c"}
    assert active_by_energy(powers, threshold=10) == set()


def test_two_tones_two_channels():
    plan = _plan()
    fs = 1_000_000.0
    n = 65536
    t = np.arange(n) / fs
    iq = (np.exp(2j * np.pi * -10_000 * t)     # channel 'a'
          + np.exp(2j * np.pi * 10_000 * t)    # channel 'c'
          ).astype(np.complex64)
    powers = channel_powers(iq, fs, plan, channel_bw_hz=6_000)
    # Both 'a' and 'c' hot, 'b' (center, empty) cold.
    assert powers["a"] > 20 * powers["b"]
    assert powers["c"] > 20 * powers["b"]
