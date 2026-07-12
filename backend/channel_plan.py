"""Channel plan for multi-frequency capture (Phase 7).

A Capacity Plus site spreads its logical slots (LSNs) across several
physical frequencies. With one wideband RSP1B capture (≤10 MHz) we can
channelize and decode all of them at once — this module is the operator's
declaration of *which* physical channels exist and how they map to LSNs.

The plan is a small JSON file, e.g.::

    {
      "channels": [
        {"label": "cc",  "frequency_hz": 168500000, "lsn": 1, "control": true},
        {"label": "ch2", "frequency_hz": 168512500, "lsn": 2},
        {"label": "ch3", "frequency_hz": 168525000, "lsn": 3}
      ]
    }

Pure and hardware-independent: loading, validation, and the geometry
helpers (span, center frequency, does-it-fit-in-N-MHz) are all testable
without an SDR.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class Channel(BaseModel):
    label: str                          # short unique id, used in events/UI
    frequency_hz: float                 # tuned center of this DMR channel
    lsn: Optional[int] = None           # Cap+ logical slot number, if known
    control: bool = False               # is this (usually) the control channel?

    @field_validator("label")
    @classmethod
    def _label_nonempty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("channel label must be non-empty")
        return v

    @field_validator("frequency_hz")
    @classmethod
    def _freq_sane(cls, v: float) -> float:
        # 1 kHz – 2 GHz is the RSP1B's range; reject obvious unit mistakes.
        if not (1_000 <= v <= 2_000_000_000):
            raise ValueError(f"frequency_hz out of range: {v}")
        return v


class ChannelPlan(BaseModel):
    channels: list[Channel] = Field(default_factory=list)

    @model_validator(mode="after")
    def _unique_labels(self) -> "ChannelPlan":
        labels = [c.label for c in self.channels]
        if len(labels) != len(set(labels)):
            dupes = sorted({l for l in labels if labels.count(l) > 1})
            raise ValueError(f"duplicate channel labels: {dupes}")
        return self

    # --- geometry (single-RSP feasibility) ---

    def span_hz(self) -> float:
        """Width between the lowest and highest channel center. 0 if <2."""
        if len(self.channels) < 2:
            return 0.0
        freqs = [c.frequency_hz for c in self.channels]
        return max(freqs) - min(freqs)

    def center_hz(self) -> Optional[float]:
        """Midpoint to tune the wideband capture at. None if empty."""
        if not self.channels:
            return None
        freqs = [c.frequency_hz for c in self.channels]
        return (max(freqs) + min(freqs)) / 2.0

    def fits_in_bandwidth(self, bandwidth_hz: float, guard_hz: float = 25_000) -> bool:
        """True if every channel (plus a guard band each side) fits inside a
        single ``bandwidth_hz`` capture — i.e. one RSP can cover them all."""
        if len(self.channels) < 2:
            return True
        return self.span_hz() + 2 * guard_hz <= bandwidth_hz

    # --- lookups ---

    def by_label(self, label: str) -> Optional[Channel]:
        for c in self.channels:
            if c.label == label:
                return c
        return None

    def lsn_to_frequency(self) -> dict[int, float]:
        """{lsn: frequency_hz} for channels that declared an LSN — the map
        the trunk-following scheduler (Phase 8) needs to tune to a grant."""
        return {c.lsn: c.frequency_hz for c in self.channels if c.lsn is not None}

    def control_channels(self) -> list[Channel]:
        return [c for c in self.channels if c.control]


def load_channel_plan(path: Path) -> ChannelPlan:
    """Load and validate a channel-plan JSON file."""
    p = Path(path)
    data = json.loads(p.read_text(encoding="utf-8"))
    return ChannelPlan.model_validate(data)
