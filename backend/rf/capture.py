"""SoapySDR wideband IQ capture for multi-frequency mode (Phase 7).

Owns the single RSP1B: opens it once via SoapySDR, tunes to the channel
plan's center frequency at a sample rate wide enough to span every
channel, and yields IQ blocks to the channelizer.

SoapySDR is imported lazily and behind a clear error, so the rest of the
package imports and unit-tests without the library or hardware present.
There is no automated test of a live capture here (it needs an RSP); the
channelizer that consumes these blocks is tested with synthetic IQ.
"""
from __future__ import annotations

from typing import Iterator, Optional

from ..channel_plan import ChannelPlan


def soapy_available() -> bool:
    try:
        import SoapySDR  # noqa: F401
        return True
    except Exception:  # noqa: BLE001
        return False


def choose_sample_rate(plan: ChannelPlan, guard_hz: float = 50_000,
                       max_rate: float = 10_000_000) -> float:
    """Pick a capture sample rate that spans the whole plan with guard band.

    RSP1B tops out near 10 MHz of usable bandwidth. Raises if the plan is
    wider than ``max_rate`` — that's the "channels don't fit one radio"
    case the operator must resolve (scan, or a second receiver).
    """
    needed = plan.span_hz() + 2 * guard_hz
    if needed > max_rate:
        raise ValueError(
            f"channel plan spans {plan.span_hz()/1e6:.3f} MHz + guard "
            f"> {max_rate/1e6:.1f} MHz max — one RSP1B can't cover it; "
            "narrow the plan, scan, or add a receiver."
        )
    # A modest floor keeps the per-channel decimation sane.
    return max(needed, 2_000_000.0)


class WidebandCapture:
    """Thin SoapySDR wrapper: open, tune, stream IQ blocks."""

    def __init__(
        self,
        plan: ChannelPlan,
        driver: str = "sdrplay",
        device_args: str = "",
        sample_rate: Optional[float] = None,
        gain: Optional[float] = None,
        block_size: int = 65536,
    ) -> None:
        self.plan = plan
        self.driver = driver
        self.device_args = device_args
        self.sample_rate = sample_rate or choose_sample_rate(plan)
        self.center_hz = plan.center_hz()
        self.gain = gain
        self.block_size = block_size
        self._dev = None
        self._stream = None

    def open(self) -> None:  # pragma: no cover - needs hardware
        try:
            import SoapySDR
            from SoapySDR import SOAPY_SDR_CF32, SOAPY_SDR_RX
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                "SoapySDR not available — install soapysdr + SoapySDRPlay3 "
                "(see scripts/setup-sdrplay.sh)"
            ) from exc
        args = f"driver={self.driver}"
        if self.device_args:
            args += f",{self.device_args}"
        self._dev = SoapySDR.Device(args)
        self._dev.setSampleRate(SOAPY_SDR_RX, 0, self.sample_rate)
        self._dev.setFrequency(SOAPY_SDR_RX, 0, self.center_hz)
        if self.gain is not None:
            self._dev.setGain(SOAPY_SDR_RX, 0, self.gain)
        self._stream = self._dev.setupStream(SOAPY_SDR_RX, SOAPY_SDR_CF32)
        self._dev.activateStream(self._stream)

    def blocks(self) -> Iterator["object"]:  # pragma: no cover - needs hardware
        import numpy as np
        buff = np.empty(self.block_size, dtype=np.complex64)
        while True:
            sr = self._dev.readStream(self._stream, [buff], len(buff))
            n = sr.ret
            if n > 0:
                yield buff[:n].copy()

    def close(self) -> None:  # pragma: no cover - needs hardware
        if self._dev is not None and self._stream is not None:
            try:
                self._dev.deactivateStream(self._stream)
                self._dev.closeStream(self._stream)
            except Exception:  # noqa: BLE001
                pass
        self._dev = None
        self._stream = None
