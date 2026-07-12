"""dsd-fme spawn-command construction — pure, hardware-independent.

Split out of ``cli.py`` (0.26.0) so ``backend/rf/control.py`` (the live
SDR controller) can build commands without importing ``cli.py`` — cli.py
is the process entry point and lazily imports RF submodules inside
``_run()``, so a module-level import the other way would be circular.

Anything with ``rf_backend`` / ``input`` / ``frequency`` / ``sdr_driver`` /
``sdr_device_args`` / ``gain`` / ``ppm`` / ``bandwidth_khz`` /
``dsd_bin`` / ``calls_dir`` attributes works as input — an
``argparse.Namespace`` (CLI) or an ``RfRuntimeConfig``-derived snapshot
(live control) alike.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any


def normalize_frequency(value: str) -> str:
    """Normalise a user frequency into dsd-fme's 'NNN.NNNM' MHz form.

    Accepts plain Hz (``168500000``), MHz with an M suffix (``168.5M``),
    or a bare small number treated as MHz (``168.5``). Raises ValueError
    on garbage so callers can fail fast with a clear message.
    """
    s = str(value).strip()
    if not s:
        raise ValueError("empty frequency")
    if s[-1] in ("M", "m"):
        mhz = float(s[:-1])
    else:
        n = float(s)
        # Heuristic: anything ≥ 10 000 is Hz, below that is MHz.
        mhz = n / 1e6 if n >= 10_000 else n
    if not (0.001 <= mhz <= 3000):
        raise ValueError(f"frequency out of range: {value!r} → {mhz} MHz")
    return f"{mhz:g}M"


def build_soapy_input(args: Any) -> str:
    """Build dsd-fme's SoapySDR input string.

    Verified against dsd-neo's documented form
    ``soapy[:args]:freq[:gain[:ppm[:bw]]]`` — e.g.
    ``soapy:driver=sdrplay:168.5M:22:-2:24``. NOTE: the exact accepted
    keys can differ between dsd-fme forks/builds; check ``dsd-fme -h``
    on the target machine if the spawn fails.
    """
    device = f"driver={args.sdr_driver}"
    if args.sdr_device_args:
        device += f",{args.sdr_device_args}"
    freq = normalize_frequency(args.frequency)
    gain = f"{args.gain:g}"
    return (
        f"soapy:{device}:{freq}:{gain}:{args.ppm}:{args.bandwidth_khz}"
    )


def build_dsd_command(args: Any) -> list[str]:
    """Assemble the dsd-fme spawn command. Pure — unit-testable without
    hardware; the only inputs are the RF-tuning attributes on ``args``.

    ``-fs`` = DMR/Cap+ decode; ``-7 <dir>`` must come BEFORE ``-P``
    (per-call WAV recording) per the dsd-fme help.
    """
    if args.rf_backend == "soapy":
        input_str = build_soapy_input(args)
    else:
        input_str = args.input
    return [
        args.dsd_bin, "-fs",
        "-i", input_str,
        "-7", str(Path(args.calls_dir)),
        "-P",
    ]
