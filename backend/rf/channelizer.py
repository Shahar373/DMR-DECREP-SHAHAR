"""Wideband IQ → per-channel narrowband FM audio (Phase 7).

One RSP1B capture at ``sample_rate`` centered on ``center_hz`` carries
every Cap+ channel at once (as long as they fit in the sample rate, which
the channel plan's ``fits_in_bandwidth`` checks). For each channel we do
a digital down-conversion (DDC): frequency-shift the channel to baseband,
low-pass, decimate to an audio-ish rate, then quadrature FM-demodulate.

A per-channel DDC (rather than a full polyphase filterbank) is used
because a Cap+ site has only a handful of channels — the DDC is simpler,
correct, and independently testable. Everything here is pure numpy and
exercised with synthetic IQ in the tests; nothing touches hardware.

numpy is imported lazily so importing this module never fails when numpy
is absent — only actually channelizing does.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from ..channel_plan import ChannelPlan

if TYPE_CHECKING:  # pragma: no cover
    import numpy as np


def _np():
    import numpy as np  # local import so the package loads without numpy
    return np


def channel_offsets(plan: ChannelPlan, center_hz: float) -> dict[str, float]:
    """Baseband offset (Hz, signed) of each channel from the capture center."""
    return {c.label: c.frequency_hz - center_hz for c in plan.channels}


def _lowpass_taps(np, cutoff_norm: float, num_taps: int = 129):
    """Windowed-sinc low-pass FIR. ``cutoff_norm`` is cutoff / Nyquist."""
    n = np.arange(num_taps) - (num_taps - 1) / 2.0
    # sinc already includes the 1/pi; np.sinc(x) = sin(pi x)/(pi x).
    h = cutoff_norm * np.sinc(cutoff_norm * n)
    h *= np.hamming(num_taps)
    h /= np.sum(h)
    return h


def ddc(iq, sample_rate: float, offset_hz: float, decim: int, num_taps: int = 129):
    """Digital down-conversion: shift ``offset_hz`` to DC, low-pass, decimate.

    Returns the complex baseband of one channel at ``sample_rate / decim``.
    """
    np = _np()
    iq = np.asarray(iq, dtype=np.complex64)
    n = np.arange(len(iq))
    mixer = np.exp(-2j * np.pi * offset_hz / sample_rate * n).astype(np.complex64)
    shifted = iq * mixer
    # Cutoff at the output Nyquist (1/decim of input Nyquist) with margin.
    cutoff = 0.9 / decim
    taps = _lowpass_taps(np, cutoff, num_taps).astype(np.float32)
    filtered = np.convolve(shifted, taps, mode="same")
    return filtered[::decim]


def fm_demodulate(baseband):
    """Quadrature FM demod: instantaneous frequency = d(phase)/dt.

    Output is real, roughly proportional to the modulating signal. Scaled
    so full deviation maps near ±1.
    """
    np = _np()
    bb = np.asarray(baseband, dtype=np.complex64)
    if len(bb) < 2:
        return np.zeros(0, dtype=np.float32)
    # angle of consecutive-sample product = phase difference, wrapped.
    demod = np.angle(bb[1:] * np.conj(bb[:-1]))
    return (demod / np.pi).astype(np.float32)


def to_pcm16(audio, volume: float = 0.9):
    """Real audio in ~[-1, 1] → little-endian int16 PCM bytes (for the
    TCP feed dsd-fme reads with ``-i tcp``)."""
    np = _np()
    a = np.asarray(audio, dtype=np.float32)
    if len(a):
        peak = float(np.max(np.abs(a)))
        if peak > 0:
            a = a / peak
    clipped = np.clip(a * volume, -1.0, 1.0)
    return (clipped * 32767.0).astype("<i2").tobytes()


class Channelizer:
    """Split a wideband IQ stream into per-channel FM audio.

    ``audio_rate`` is the target output rate; ``decim`` is derived from
    ``sample_rate / audio_rate`` (rounded). Stateless per ``process``
    call — callers feed successive IQ blocks.
    """

    def __init__(self, plan: ChannelPlan, sample_rate: float, audio_rate: int = 48_000):
        self.plan = plan
        self.sample_rate = float(sample_rate)
        self.audio_rate = int(audio_rate)
        self.center_hz = plan.center_hz() or 0.0
        self.decim = max(1, round(self.sample_rate / self.audio_rate))
        self.offsets = channel_offsets(plan, self.center_hz)

    def process(self, iq) -> dict[str, "np.ndarray"]:
        """One IQ block → {channel_label: demodulated audio (float32)}."""
        out = {}
        for label, offset in self.offsets.items():
            bb = ddc(iq, self.sample_rate, offset, self.decim)
            out[label] = fm_demodulate(bb)
        return out
