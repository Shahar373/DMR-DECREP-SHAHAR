#!/usr/bin/env bash
# One-shot system-dependency installer for the DMR Cap+ Monitor on a fresh
# 64-bit Raspberry Pi OS (Bookworm). Installs every apt package the project
# needs, including the *build* dependencies for dsd-fme.
#
# dsd-fme itself is NOT installed here: it is a source build whose recipe
# lives upstream (https://github.com/lwvmobile/dsd-fme) and changes between
# releases. This script makes sure every dependency that build needs is in
# place, then points you at it. Run scripts/check_env.sh afterwards to see
# what (if anything) is still missing.
#
# Usage:  bash scripts/install-deps.sh
# Needs sudo for apt. Idempotent — safe to re-run.
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

if [ "$(id -u)" -eq 0 ]; then
    SUDO=""
else
    SUDO="sudo"
fi

echo "DMR Cap+ Monitor — installing system dependencies"
echo "=================================================="
echo "# architecture: $(uname -m)"

# Core Python toolchain for the virtualenv + tests.
PY_PKGS=(python3 python3-venv python3-pip)

# dsd-fme build dependencies (kept in sync with scripts/check_env.sh).
DSD_BUILD_PKGS=(build-essential cmake git pkg-config libtool autoconf
                libitpp-dev libsndfile1-dev libpulse-dev libusb-1.0-0-dev)

# Audio utilities. `pactl` (pulseaudio-utils) talks to whatever
# PulseAudio-compatible server is running — classic PulseAudio OR the
# PipeWire-pulse shim that ships by default on Bookworm. We deliberately do
# NOT install the full `pulseaudio` daemon so we don't fight PipeWire.
AUDIO_PKGS=(pulseaudio-utils)

# SoapySDR runtime for --rf-backend soapy (direct SDRplay control, no
# SDRconnect / virtual cable). The proprietary SDRplay API service itself
# must come from sdrplay.com — see scripts/setup-sdrplay.sh.
SDR_PKGS=(soapysdr-tools libsoapysdr-dev)

# Optional Phase 4b (audio streaming → Icecast). Not used by the current
# code; installed best-effort so a later --serve audio feature works.
OPTIONAL_PKGS=(ffmpeg icecast2)

echo "# apt-get update"
$SUDO apt-get update -y

echo "# installing core + dsd-fme build deps + audio utils"
$SUDO apt-get install -y "${PY_PKGS[@]}" "${DSD_BUILD_PKGS[@]}" "${AUDIO_PKGS[@]}"

echo "# installing SoapySDR runtime (for --rf-backend soapy)"
if ! $SUDO apt-get install -y "${SDR_PKGS[@]}"; then
    echo "  (SoapySDR packages failed — only needed for --rf-backend soapy;"
    echo "   see scripts/setup-sdrplay.sh)"
fi

echo "# installing optional Phase 4b packages (ffmpeg, icecast2)"
if ! $SUDO apt-get install -y "${OPTIONAL_PKGS[@]}"; then
    echo "  (optional packages failed to install — safe to ignore for now)"
fi

echo ""
echo "System dependencies installed. Next steps:"
echo "  bash scripts/check_env.sh         # verify (will still flag dsd-fme as missing)"
echo "  bash scripts/setup-sdrplay.sh     # direct SDR control (--rf-backend soapy)"
echo "  bash scripts/setup-pulseaudio.sh  # OR the legacy dmr_capture sink (pulse)"
echo "  python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"
echo "  .venv/bin/pytest tests/           # 177/177 should pass"
echo ""
echo "dsd-fme is NOT available via apt — build it from source (deps are ready):"
echo "  https://github.com/lwvmobile/dsd-fme"
