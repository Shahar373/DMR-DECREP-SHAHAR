"""Live SDR control (Phase 9): persisted tuning + retune signalling.

The operator can change frequency/gain/ppm/bandwidth (and pause/resume
the live decode) from the dashboard while the service keeps running.
``RfRuntimeConfig`` is the on-disk state (survives restarts);
``RfController`` is the in-process object the live loop and the FastAPI
handlers share — a change made via the API mutates the config, persists
it, and signals the live loop through ``retune_event`` /
``live_toggle_event`` so ``stream_subprocess_with_retry`` (see
``wrapper.py``) respawns dsd-fme with the new tuning.

Field names mirror the CLI flags 1:1 (``rf_backend``, ``input``,
``frequency``, ``sdr_driver``, ``sdr_device_args``, ``gain``, ``ppm``,
``bandwidth_khz``) so ``apply_to_args()`` can hand ``build_dsd_command()``
a drop-in ``argparse.Namespace`` snapshot without any translation layer.
"""
from __future__ import annotations

import asyncio
import copy
from pathlib import Path
from typing import Literal, Optional

from pydantic import BaseModel

from ..dsd_command import normalize_frequency
from ..state import atomic_write_text


class RfRuntimeConfig(BaseModel):
    rf_backend: Literal["pulse", "soapy"] = "pulse"
    input: str = "pulse:dmr_capture.monitor"
    frequency: Optional[str] = None  # normalized "NNN.NM" string; None if unset
    sdr_driver: str = "sdrplay"
    sdr_device_args: str = ""
    gain: float = 0.0
    ppm: int = 0
    bandwidth_khz: int = 24
    live_enabled: bool = True


def load_or_seed_rf_config(path: Path, seed: RfRuntimeConfig) -> RfRuntimeConfig:
    """Load a persisted config; on any problem (missing, corrupt), seed
    from ``seed`` (the CLI-flag-derived defaults) and persist that."""
    if path.exists():
        try:
            return RfRuntimeConfig.model_validate_json(
                path.read_text(encoding="utf-8")
            )
        except Exception:  # noqa: BLE001 — corrupt config must not block startup
            pass
    atomic_write_text(path, seed.model_dump_json(indent=2), keep_backup=False)
    return seed


class RfController:
    """Shared between the live-capture loop and the FastAPI handlers.

    All mutation happens on the asyncio event loop (server handlers and
    the capture loop share one loop), so no lock is needed.
    """

    def __init__(self, config: RfRuntimeConfig, config_path: Path) -> None:
        self.config = config
        self.config_path = config_path
        # Fired (by set_tuning) to ask the current dsd-fme child to exit
        # so the retry loop respawns it with the new tuning. Consumed by
        # wrapper.stream_subprocess_with_retry as `interrupt_event`.
        self.retune_event = asyncio.Event()
        # Fired (by set_live_enabled) whenever live_enabled flips either
        # way. Single-consumer pattern: the live-loop supervisor waits on
        # it and clears it after observing the new value.
        self.live_toggle_event = asyncio.Event()
        self.last_error: Optional[str] = None

    # --- mutation (called from FastAPI handlers) ---

    def set_tuning(
        self,
        *,
        rf_backend: Optional[str] = None,
        input: Optional[str] = None,
        frequency: Optional[str] = None,
        sdr_driver: Optional[str] = None,
        sdr_device_args: Optional[str] = None,
        gain: Optional[float] = None,
        ppm: Optional[int] = None,
        bandwidth_khz: Optional[int] = None,
    ) -> dict:
        """Apply any provided fields, validate, persist, and signal a
        retune. Raises ValueError on bad input (e.g. an unparsable
        frequency) — the caller (the API handler) turns that into a 422.
        """
        new = self.config.model_copy()
        if rf_backend is not None:
            if rf_backend not in ("pulse", "soapy"):
                raise ValueError(f"rf_backend must be 'pulse' or 'soapy', got {rf_backend!r}")
            new.rf_backend = rf_backend
        if input is not None:
            new.input = input
        if frequency is not None:
            new.frequency = normalize_frequency(frequency)  # raises ValueError on garbage
        if sdr_driver is not None:
            new.sdr_driver = sdr_driver
        if sdr_device_args is not None:
            new.sdr_device_args = sdr_device_args
        if gain is not None:
            new.gain = gain
        if ppm is not None:
            new.ppm = ppm
        if bandwidth_khz is not None:
            if bandwidth_khz <= 0:
                raise ValueError("bandwidth_khz must be positive")
            new.bandwidth_khz = bandwidth_khz
        if new.rf_backend == "soapy" and not new.frequency:
            raise ValueError("rf_backend=soapy requires a frequency (set one first)")

        self.config = new
        self._persist()
        self.retune_event.set()
        return self.status()

    def set_live_enabled(self, enabled: bool) -> dict:
        if self.config.live_enabled != enabled:
            self.config.live_enabled = enabled
            self._persist()
            self.live_toggle_event.set()
        return self.status()

    def _persist(self) -> None:
        try:
            atomic_write_text(
                self.config_path, self.config.model_dump_json(indent=2),
                keep_backup=False,
            )
        except Exception as exc:  # noqa: BLE001 — persistence failure must not crash a retune
            self.last_error = f"config persist failed: {exc}"

    # --- reads ---

    def status(self) -> dict:
        d = self.config.model_dump()
        d["last_error"] = self.last_error
        return d

    def apply_to_args(self, base_args) -> object:
        """A shallow copy of ``base_args`` with RF-tuning fields
        overridden from the current config — feed straight to
        ``build_dsd_command()``. Re-called on every dsd-fme spawn so a
        retune mid-stream is picked up by the next spawn."""
        a = copy.copy(base_args)
        a.rf_backend = self.config.rf_backend
        a.input = self.config.input
        a.frequency = self.config.frequency
        a.sdr_driver = self.config.sdr_driver
        a.sdr_device_args = self.config.sdr_device_args
        a.gain = self.config.gain
        a.ppm = self.config.ppm
        a.bandwidth_khz = self.config.bandwidth_khz
        return a
