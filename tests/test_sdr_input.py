"""Phase-6 tests: SDRplay/SoapySDR input construction (v0.23.0).

Pure command-building — no RF hardware required. The exact dsd-fme soapy
arg syntax can differ between forks; these tests pin the shape the CLI
emits so a regression is caught, and the real binary syntax is verified
against `dsd-fme -h` at deploy time.
"""
from __future__ import annotations

import argparse

import pytest

from backend.cli import (
    build_dsd_command,
    build_soapy_input,
    normalize_frequency,
)


def _args(**kw):
    base = dict(
        rf_backend="pulse",
        input="pulse:dmr_capture.monitor",
        dsd_bin="dsd-fme",
        calls_dir="/tmp/dmr_calls",
        frequency=None,
        sdr_driver="sdrplay",
        sdr_device_args="",
        gain=0,
        ppm=0,
        bandwidth_khz=24,
    )
    base.update(kw)
    return argparse.Namespace(**base)


# ── normalize_frequency ──────────────────────────────────────────────


@pytest.mark.parametrize("value,expected", [
    ("168.5M", "168.5M"),
    ("168500000", "168.5M"),
    ("168.5", "168.5M"),
    ("435M", "435M"),
    ("435000000", "435M"),
    ("1200000000", "1200M"),  # 1.2 GHz in Hz
])
def test_normalize_frequency_accepts_hz_and_mhz(value, expected):
    assert normalize_frequency(value) == expected


@pytest.mark.parametrize("bad", ["", "abc", "0", "999999M", "-5"])
def test_normalize_frequency_rejects_garbage(bad):
    with pytest.raises(ValueError):
        normalize_frequency(bad)


# ── build_soapy_input ────────────────────────────────────────────────


def test_soapy_input_default_shape():
    s = build_soapy_input(_args(frequency="168.5M"))
    assert s == "soapy:driver=sdrplay:168.5M:0:0:24"


def test_soapy_input_with_gain_ppm_bw_and_device_args():
    s = build_soapy_input(_args(
        frequency="435000000", sdr_device_args="serial=123456",
        gain=22, ppm=-2, bandwidth_khz=48,
    ))
    assert s == "soapy:driver=sdrplay,serial=123456:435M:22:-2:48"


def test_soapy_input_alternate_driver():
    s = build_soapy_input(_args(frequency="168.5M", sdr_driver="rtlsdr"))
    assert s.startswith("soapy:driver=rtlsdr:")


# ── build_dsd_command ────────────────────────────────────────────────


def test_dsd_command_pulse_backend_uses_input_verbatim():
    cmd = build_dsd_command(_args(rf_backend="pulse"))
    assert cmd == [
        "dsd-fme", "-fs",
        "-i", "pulse:dmr_capture.monitor",
        "-7", "/tmp/dmr_calls", "-P",
    ]


def test_dsd_command_soapy_backend_builds_soapy_string():
    cmd = build_dsd_command(_args(
        rf_backend="soapy", frequency="168.5M", gain=20,
    ))
    assert cmd[:2] == ["dsd-fme", "-fs"]
    assert cmd[2] == "-i"
    assert cmd[3] == "soapy:driver=sdrplay:168.5M:20:0:24"
    # -7 <dir> must precede -P (dsd-fme help requirement).
    assert cmd[4:] == ["-7", "/tmp/dmr_calls", "-P"]


def test_dsd_command_honours_custom_binary_and_calls_dir():
    cmd = build_dsd_command(_args(
        rf_backend="soapy", frequency="435M",
        dsd_bin="/opt/dsd-fme/dsd-fme", calls_dir="/var/lib/dmr/calls",
    ))
    assert cmd[0] == "/opt/dsd-fme/dsd-fme"
    assert "/var/lib/dmr/calls" in cmd
