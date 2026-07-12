"""Phase-9 tests: RfRuntimeConfig persistence + RfController (pure, no hardware)."""
from __future__ import annotations

import argparse
import json

import pytest

from backend.dsd_command import build_dsd_command
from backend.rf.control import RfController, RfRuntimeConfig, load_or_seed_rf_config


def _seed(**kw):
    base = dict(rf_backend="pulse", input="pulse:dmr_capture.monitor",
               frequency=None, sdr_driver="sdrplay", sdr_device_args="",
               gain=0.0, ppm=0, bandwidth_khz=24, live_enabled=True)
    base.update(kw)
    return RfRuntimeConfig(**base)


# ── load_or_seed_rf_config ───────────────────────────────────────────


def test_seeds_and_persists_when_file_missing(tmp_path):
    p = tmp_path / "sdr_runtime.json"
    seed = _seed(rf_backend="soapy", frequency="168.5M")
    cfg = load_or_seed_rf_config(p, seed)
    assert cfg == seed
    assert p.exists()
    assert json.loads(p.read_text())["frequency"] == "168.5M"


def test_loads_existing_config_over_seed(tmp_path):
    p = tmp_path / "sdr_runtime.json"
    saved = _seed(rf_backend="soapy", frequency="435M", gain=15)
    p.write_text(saved.model_dump_json())
    cfg = load_or_seed_rf_config(p, _seed())  # different seed — must be ignored
    assert cfg.frequency == "435M"
    assert cfg.gain == 15


def test_corrupt_config_falls_back_to_seed(tmp_path):
    p = tmp_path / "sdr_runtime.json"
    p.write_text("{not valid json")
    seed = _seed(frequency="168.5M")
    cfg = load_or_seed_rf_config(p, seed)
    assert cfg == seed
    # Corrupt file was overwritten with the seed.
    assert json.loads(p.read_text())["frequency"] == "168.5M"


# ── RfController.set_tuning ──────────────────────────────────────────


def test_set_tuning_updates_persists_and_signals_retune(tmp_path):
    p = tmp_path / "sdr_runtime.json"
    ctrl = RfController(_seed(rf_backend="soapy", frequency="168.5M"), p)
    assert not ctrl.retune_event.is_set()

    status = ctrl.set_tuning(frequency="435M", gain=20, ppm=-3)
    assert status["frequency"] == "435M"
    assert status["gain"] == 20
    assert status["ppm"] == -3
    assert ctrl.retune_event.is_set()
    # Persisted to disk.
    assert json.loads(p.read_text())["frequency"] == "435M"


def test_set_tuning_accepts_hz_and_bare_mhz(tmp_path):
    ctrl = RfController(_seed(rf_backend="soapy", frequency="168.5M"), tmp_path / "c.json")
    ctrl.set_tuning(frequency="435000000")
    assert ctrl.config.frequency == "435M"
    ctrl.set_tuning(frequency="450.5")
    assert ctrl.config.frequency == "450.5M"


def test_set_tuning_rejects_bad_frequency(tmp_path):
    ctrl = RfController(_seed(rf_backend="soapy", frequency="168.5M"), tmp_path / "c.json")
    with pytest.raises(ValueError):
        ctrl.set_tuning(frequency="not-a-frequency")
    # Config untouched on failure.
    assert ctrl.config.frequency == "168.5M"
    assert not ctrl.retune_event.is_set()


def test_set_tuning_rejects_bad_backend(tmp_path):
    ctrl = RfController(_seed(), tmp_path / "c.json")
    with pytest.raises(ValueError):
        ctrl.set_tuning(rf_backend="bogus")


def test_switching_to_soapy_without_frequency_is_rejected(tmp_path):
    ctrl = RfController(_seed(rf_backend="pulse", frequency=None), tmp_path / "c.json")
    with pytest.raises(ValueError, match="requires a frequency"):
        ctrl.set_tuning(rf_backend="soapy")
    # Switching to soapy WITH a frequency in the same call is fine.
    status = ctrl.set_tuning(rf_backend="soapy", frequency="168.5M")
    assert status["rf_backend"] == "soapy"


def test_set_tuning_rejects_nonpositive_bandwidth(tmp_path):
    ctrl = RfController(_seed(rf_backend="soapy", frequency="168.5M"), tmp_path / "c.json")
    with pytest.raises(ValueError):
        ctrl.set_tuning(bandwidth_khz=0)


def test_switch_to_pulse_ignores_frequency_requirement(tmp_path):
    ctrl = RfController(_seed(rf_backend="soapy", frequency="168.5M"), tmp_path / "c.json")
    status = ctrl.set_tuning(rf_backend="pulse", input="pulse:other.monitor")
    assert status["rf_backend"] == "pulse"
    assert status["input"] == "pulse:other.monitor"


# ── RfController.set_live_enabled ────────────────────────────────────


def test_set_live_enabled_toggles_and_signals(tmp_path):
    ctrl = RfController(_seed(live_enabled=True), tmp_path / "c.json")
    assert not ctrl.live_toggle_event.is_set()
    status = ctrl.set_live_enabled(False)
    assert status["live_enabled"] is False
    assert ctrl.live_toggle_event.is_set()


def test_set_live_enabled_noop_when_unchanged_does_not_signal(tmp_path):
    ctrl = RfController(_seed(live_enabled=True), tmp_path / "c.json")
    ctrl.set_live_enabled(True)  # already True
    assert not ctrl.live_toggle_event.is_set()


# ── apply_to_args / build_dsd_command integration ────────────────────


def test_apply_to_args_feeds_build_dsd_command(tmp_path):
    base = argparse.Namespace(
        dsd_bin="dsd-fme", calls_dir="/tmp/calls",
        rf_backend="pulse", input="pulse:dmr_capture.monitor",
        frequency=None, sdr_driver="sdrplay", sdr_device_args="",
        gain=0, ppm=0, bandwidth_khz=24,
    )
    ctrl = RfController(_seed(rf_backend="soapy", frequency="168.5M", gain=22), tmp_path / "c.json")
    tuned_args = ctrl.apply_to_args(base)
    cmd = build_dsd_command(tuned_args)
    assert cmd == [
        "dsd-fme", "-fs", "-i", "soapy:driver=sdrplay:168.5M:22:0:24",
        "-7", "/tmp/calls", "-P",
    ]
    # base_args itself is untouched (apply_to_args copies, doesn't mutate).
    assert base.rf_backend == "pulse"


def test_apply_to_args_reflects_a_retune_on_next_call(tmp_path):
    base = argparse.Namespace(
        dsd_bin="dsd-fme", calls_dir="/tmp/calls",
        rf_backend="soapy", input="pulse:x", frequency="168.5M",
        sdr_driver="sdrplay", sdr_device_args="", gain=0, ppm=0, bandwidth_khz=24,
    )
    ctrl = RfController(_seed(rf_backend="soapy", frequency="168.5M"), tmp_path / "c.json")
    cmd1 = build_dsd_command(ctrl.apply_to_args(base))
    ctrl.set_tuning(frequency="435M")
    cmd2 = build_dsd_command(ctrl.apply_to_args(base))
    assert "168.5M" in cmd1[3]
    assert "435M" in cmd2[3]
