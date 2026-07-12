"""Per-channel energy detection (Phase 8 fallback).

Given a wideband IQ block, estimate the power in each channel's bin so the
scheduler can activate a channel that's carrying signal even before a
control-channel grant is decoded. numpy is imported lazily.

Testable with synthetic IQ: a tone at one channel's offset should show up
as high power in that channel and low power elsewhere.
"""
from __future__ import annotations

from ..channel_plan import ChannelPlan
from .channelizer import channel_offsets


def _np():
    import numpy as np
    return np


def channel_powers(
    iq,
    sample_rate: float,
    plan: ChannelPlan,
    channel_bw_hz: float = 12_500.0,
) -> dict[str, float]:
    """Linear power in each channel's band, from one IQ block.

    A single Welch-style periodogram of the block is integrated over the
    ±channel_bw/2 window around each channel's offset from the capture
    center. Returns {label: power}.
    """
    np = _np()
    x = np.asarray(iq, dtype=np.complex64)
    n = len(x)
    if n == 0:
        return {c.label: 0.0 for c in plan.channels}

    win = np.hanning(n).astype(np.float32)
    spec = np.fft.fftshift(np.fft.fft(x * win))
    psd = (np.abs(spec) ** 2) / (n * n)
    # Bin center frequencies (Hz), matching fftshift ordering.
    freqs = np.fft.fftshift(np.fft.fftfreq(n, 1.0 / sample_rate))

    center = plan.center_hz() or 0.0
    offsets = channel_offsets(plan, center)
    half = channel_bw_hz / 2.0
    out: dict[str, float] = {}
    for label, off in offsets.items():
        mask = (freqs >= off - half) & (freqs <= off + half)
        out[label] = float(np.sum(psd[mask]))
    return out


def active_by_energy(
    powers: dict[str, float],
    threshold: float,
) -> set[str]:
    """Labels whose power is at/above ``threshold``."""
    return {label for label, p in powers.items() if p >= threshold}
