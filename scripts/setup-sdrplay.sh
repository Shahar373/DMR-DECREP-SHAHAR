#!/usr/bin/env bash
# Set up the direct-SDR chain for `--rf-backend soapy`:
#
#   RSP1B ── SDRplay API service (proprietary daemon) ── SoapySDRPlay3 ── dsd-fme -i soapy:...
#
# This replaces the legacy SDRconnect-GUI + virtual-audio-cable chain
# (scripts/setup-pulseaudio.sh stays available for --rf-backend pulse).
#
# The SDRplay API itself is proprietary and must be downloaded from
# sdrplay.com — this script installs everything that CAN come from apt,
# verifies each link of the chain, and prints exact instructions for the
# pieces it can't install itself.
set -uo pipefail

PASS="✓"; FAIL="✗"; WARN="!"
fails=0

step() { echo; echo "── $1"; }

step "1. SoapySDR runtime + tools (apt)"
if command -v SoapySDRUtil >/dev/null 2>&1; then
    echo "  $PASS SoapySDRUtil already installed"
else
    echo "  installing soapysdr-tools..."
    sudo apt-get install -y soapysdr-tools libsoapysdr-dev >/dev/null 2>&1 \
        && echo "  $PASS installed" \
        || { echo "  $FAIL apt install failed — run: sudo apt install soapysdr-tools libsoapysdr-dev"; fails=$((fails+1)); }
fi

step "1b. SoapySDR python bindings (apt)"
if python3 -c "import SoapySDR" >/dev/null 2>&1; then
    echo "  $PASS python3 already sees SoapySDR"
else
    echo "  installing python3-soapysdr..."
    sudo apt-get install -y python3-soapysdr >/dev/null 2>&1 \
        && echo "  $PASS installed" \
        || { echo "  $FAIL apt install failed — run: sudo apt install python3-soapysdr"; fails=$((fails+1)); }
    echo "  note: apt installs bindings for system python3 only. A venv"
    echo "        created WITHOUT --system-site-packages (e.g. via the"
    echo "        README's 'python3 -m venv .venv') won't see them — either"
    echo "        recreate with --system-site-packages, or flip"
    echo "        'include-system-site-packages = false' to 'true' in"
    echo "        .venv/pyvenv.cfg (no need to recreate the venv)."
fi

step "2. SDRplay API service (proprietary — manual download)"
if pgrep -x sdrplay_apiService >/dev/null 2>&1; then
    echo "  $PASS sdrplay_apiService is running"
elif [ -x /usr/local/bin/sdrplay_apiService ] || [ -x /opt/sdrplay_api/sdrplay_apiService ]; then
    echo "  $WARN installed but not running — start it:"
    echo "        sudo systemctl start sdrplay.service   (or reboot)"
    fails=$((fails+1))
else
    echo "  $FAIL not found. Download 'SDRplay API' (ARM64/Linux) from"
    echo "        https://www.sdrplay.com/api/  and run the installer,"
    echo "        then reboot so the service starts."
    fails=$((fails+1))
fi

step "3. SoapySDRPlay3 driver module"
if SoapySDRUtil --info 2>/dev/null | grep -qi sdrplay; then
    echo "  $PASS SoapySDRPlay3 module registered"
else
    if sudo apt-get install -y soapysdr-module-sdrplay3 >/dev/null 2>&1; then
        echo "  $PASS installed from apt (soapysdr-module-sdrplay3)"
    else
        echo "  $FAIL not registered and not in apt. Build from source:"
        echo "        git clone https://github.com/pothosware/SoapySDRPlay3"
        echo "        cd SoapySDRPlay3 && mkdir build && cd build"
        echo "        cmake .. && make -j4 && sudo make install && sudo ldconfig"
        fails=$((fails+1))
    fi
fi

step "4. Device discovery"
if command -v SoapySDRUtil >/dev/null 2>&1; then
    if SoapySDRUtil --find 2>/dev/null | grep -qi "driver.*=.*sdrplay"; then
        echo "  $PASS RSP found:"
        SoapySDRUtil --find 2>/dev/null | grep -i -A2 sdrplay | sed 's/^/        /'
    else
        echo "  $WARN no SDRplay device found — check USB cable/power and"
        echo "        that the API service is running, then re-run this script."
        fails=$((fails+1))
    fi
fi

echo
if [ "$fails" -eq 0 ]; then
    echo "All good. Run the monitor with direct SDR control, e.g.:"
    echo "  python -m backend.cli --live --rf-backend soapy --frequency 168.5M --serve"
else
    echo "$fails item(s) need attention (see above). The legacy pulse chain"
    echo "(scripts/setup-pulseaudio.sh + SDRconnect) keeps working meanwhile."
    exit 1
fi
