"""Phase-7 tests: DSP channelizer/demod with SYNTHETIC IQ (no hardware).

These prove the numpy DSP does the right thing end-to-end on a signal we
generate ourselves — a tone lands in the right channel, and an
FM-modulated tone demodulates back to its modulating frequency.
"""
from __future__ import annotations

import numpy as np
import pytest

from backend.channel_plan import Channel, ChannelPlan
from backend.rf.channelizer import (
    Channelizer,
    channel_offsets,
    ddc,
    fm_demodulate,
    to_pcm16,
)


def test_channel_offsets():
    plan = ChannelPlan(channels=[
        Channel(label="a", frequency_hz=168_490_000),
        Channel(label="b", frequency_hz=168_510_000),
    ])
    center = plan.center_hz()  # 168_500_000
    offs = channel_offsets(plan, center)
    assert offs["a"] == pytest.approx(-10_000)
    assert offs["b"] == pytest.approx(+10_000)


def test_ddc_isolates_a_tone_at_its_offset():
    fs = 2_000_000.0
    n = 200_000
    t = np.arange(n) / fs
    # Two complex tones: one at +100 kHz, one at -300 kHz.
    iq = (np.exp(2j * np.pi * 100_000 * t)
          + 0.5 * np.exp(2j * np.pi * -300_000 * t)).astype(np.complex64)
    decim = 40  # → 50 kHz output rate
    # DDC centered on +100 kHz should recover a strong ~DC tone...
    on = ddc(iq, fs, 100_000, decim)
    # ...and DDC on an empty part of the band (+700 kHz) should be weak.
    off = ddc(iq, fs, 700_000, decim)
    assert np.mean(np.abs(on)) > 5 * np.mean(np.abs(off))


def test_fm_demodulate_recovers_modulating_tone():
    fs = 48_000.0
    n = 48_000
    t = np.arange(n) / fs
    f_mod = 1_000.0            # 1 kHz modulating tone
    dev = 3_000.0             # frequency deviation
    phase = 2 * np.pi * dev / f_mod * np.sin(2 * np.pi * f_mod * t)
    fm = np.exp(1j * phase).astype(np.complex64)
    demod = fm_demodulate(fm)
    # Dominant spectral component of the demod output should be ~f_mod.
    spec = np.abs(np.fft.rfft(demod - demod.mean()))
    freqs = np.fft.rfftfreq(len(demod), 1 / fs)
    peak = freqs[int(np.argmax(spec))]
    assert peak == pytest.approx(f_mod, rel=0.05)


def test_fm_demodulate_short_input_is_safe():
    assert len(fm_demodulate(np.array([1 + 0j]))) == 0


def test_to_pcm16_shape_and_type():
    audio = np.array([0.0, 0.5, -0.5, 1.0, -1.0], dtype=np.float32)
    pcm = to_pcm16(audio)
    assert isinstance(pcm, bytes)
    assert len(pcm) == 2 * len(audio)  # 16-bit
    vals = np.frombuffer(pcm, dtype="<i2")
    assert vals.max() <= 32767 and vals.min() >= -32768


def test_channelizer_process_returns_per_channel_audio():
    plan = ChannelPlan(channels=[
        Channel(label="a", frequency_hz=168_400_000),
        Channel(label="b", frequency_hz=168_600_000),
    ])
    fs = 2_000_000.0
    ch = Channelizer(plan, fs, audio_rate=50_000)
    n = 100_000
    t = np.arange(n) / fs
    # Tone at 168.405 MHz: 5 kHz above channel 'a' center (168.400 MHz),
    # i.e. -95 kHz from the 168.500 capture center. A pure carrier at the
    # exact channel center FM-demods to ~0 (no deviation), so we offset it
    # 5 kHz to give the demod a real, non-zero instantaneous frequency.
    iq = np.exp(2j * np.pi * -95_000 * t).astype(np.complex64)
    out = ch.process(iq)
    assert set(out) == {"a", "b"}
    assert ch.decim == 40
    # Channel 'a' routes the -95 kHz tone to a steady +5 kHz residual → its
    # FM demod is a near-constant DC value at 5 kHz / (audio_rate/2). (FM
    # demod is amplitude-independent, so channel isolation is asserted at
    # the baseband level in test_ddc_isolates_a_tone_at_its_offset, not
    # here.)
    assert np.mean(out["a"]) == pytest.approx(5_000 / (50_000 / 2), rel=0.15)
    assert np.std(out["a"]) < 0.02  # steady tone, low jitter
