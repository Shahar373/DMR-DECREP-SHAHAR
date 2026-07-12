#!/usr/bin/env bash
# Phase 0: environment check for DMR Cap+ Monitor on Raspberry Pi 5.
# Run on the Pi after a fresh install. Each check runs independently.
# Exit code is the number of FAILed checks (warnings do not count).

set -u

PASS="\033[32mPASS\033[0m"
FAIL="\033[31mFAIL\033[0m"
WARN="\033[33mWARN\033[0m"
INFO="\033[36mINFO\033[0m"

fails=0

print_result() {
    # $1=status, $2=label, $3=detail
    printf "[%b] %-22s %s\n" "$1" "$2" "${3:-}"
}

# 0. CPU architecture (informational — Raspberry Pi 5 is aarch64)
check_arch() {
    local osname=""
    if [ -r /etc/os-release ]; then
        osname=$(. /etc/os-release 2>/dev/null && echo "${PRETTY_NAME:-}")
    fi
    print_result "$INFO" "architecture" "$(uname -m)${osname:+ — $osname}"
}

# 1. Python 3.11+
check_python() {
    if ! command -v python3 >/dev/null 2>&1; then
        print_result "$FAIL" "python3" "not found"
        echo "       Install: sudo apt install -y python3 python3-venv python3-pip"
        fails=$((fails + 1))
        return
    fi
    local ver
    ver=$(python3 -c 'import sys; print("%d.%d" % sys.version_info[:2])')
    local major minor
    major=${ver%.*}
    minor=${ver#*.}
    if [ "$major" -lt 3 ] || { [ "$major" -eq 3 ] && [ "$minor" -lt 11 ]; }; then
        print_result "$FAIL" "python3" "found $ver, need >= 3.11"
        echo "       Install: sudo apt install -y python3.11 python3.11-venv"
        fails=$((fails + 1))
    else
        print_result "$PASS" "python3" "$ver"
    fi
}

# 2. DSD-FME
check_dsd_fme() {
    if command -v dsd-fme >/dev/null 2>&1; then
        local bin binarch
        bin=$(command -v dsd-fme)
        print_result "$PASS" "dsd-fme" "$bin"
        # A dsd-fme copied from an old 32-bit Pi is "found" here but fails at
        # runtime on a 64-bit OS with "Exec format error". Warn on mismatch.
        if [ "$(uname -m)" = "aarch64" ] && command -v file >/dev/null 2>&1; then
            binarch=$(file -b "$bin" 2>/dev/null)
            if ! printf '%s' "$binarch" | grep -qi "aarch64"; then
                print_result "$WARN" "dsd-fme arch" \
                    "not aarch64 — may fail with Exec format error ($binarch)"
            fi
        fi
    else
        print_result "$FAIL" "dsd-fme" "not found"
        echo "       Build from source: https://github.com/lwvmobile/dsd-fme"
        echo "       Deps: sudo apt install -y build-essential cmake libitpp-dev libsndfile1-dev \\"
        echo "                                  libpulse-dev libusb-1.0-0-dev libtool autoconf"
        fails=$((fails + 1))
    fi
}

# 3. ffmpeg
check_ffmpeg() {
    if command -v ffmpeg >/dev/null 2>&1; then
        local ver
        ver=$(ffmpeg -version 2>/dev/null | head -n1 | awk '{print $3}')
        print_result "$PASS" "ffmpeg" "$ver"
    else
        print_result "$WARN" "ffmpeg" "not found (optional — Phase 4b audio streaming, not used yet)"
        echo "       Install: sudo apt install -y ffmpeg"
    fi
}

# 4. Icecast2
check_icecast() {
    if command -v icecast2 >/dev/null 2>&1; then
        print_result "$PASS" "icecast2" "$(command -v icecast2)"
    elif dpkg -l icecast2 >/dev/null 2>&1; then
        print_result "$PASS" "icecast2" "installed (dpkg)"
    else
        print_result "$WARN" "icecast2" "not found (optional — Phase 4b audio streaming, not used yet)"
        echo "       Install: sudo apt install -y icecast2"
    fi
}

# 5. PulseAudio
check_pulseaudio() {
    if ! command -v pactl >/dev/null 2>&1; then
        print_result "$FAIL" "pactl" "not found"
        echo "       Install: sudo apt install -y pulseaudio pulseaudio-utils"
        fails=$((fails + 1))
        return
    fi
    if pactl info >/dev/null 2>&1; then
        print_result "$PASS" "pulseaudio" "session active"
    else
        print_result "$WARN" "pulseaudio" "pactl present but no session (start user pulse first)"
    fi
}

# 6. SDR chain for --rf-backend soapy (warn only — the pulse chain with
#    SDRconnect still works without any of this)
check_sdr_chain() {
    if pgrep -x sdrplay_apiService >/dev/null 2>&1; then
        print_result "$PASS" "sdrplay API service" "running"
    else
        print_result "$WARN" "sdrplay API service" "not running (needed for --rf-backend soapy; see scripts/setup-sdrplay.sh)"
    fi
    if ! command -v SoapySDRUtil >/dev/null 2>&1; then
        print_result "$WARN" "SoapySDRUtil" "not found (sudo apt install soapysdr-tools)"
        return
    fi
    if SoapySDRUtil --find 2>/dev/null | grep -qi "driver.*=.*sdrplay"; then
        print_result "$PASS" "SoapySDR + RSP" "device discovered"
    else
        print_result "$WARN" "SoapySDR + RSP" "no sdrplay device found (bash scripts/setup-sdrplay.sh)"
    fi
    # Legacy chain still detected for operators on --rf-backend pulse.
    if command -v sdrconnect >/dev/null 2>&1 \
        || [ -d "/opt/SDRconnect" ] || [ -d "$HOME/SDRconnect" ]; then
        print_result "$PASS" "sdrconnect (legacy)" "present"
    fi
}

# 7. dmr_capture null sink (informational)
check_null_sink() {
    if ! command -v pactl >/dev/null 2>&1; then
        return
    fi
    if pactl list short sinks 2>/dev/null | grep -qw "dmr_capture"; then
        print_result "$PASS" "dmr_capture sink" "loaded"
    else
        print_result "$WARN" "dmr_capture sink" "not loaded (required for --live)"
        echo "       Create it: bash scripts/setup-pulseaudio.sh"
    fi
}

echo "DMR Cap+ Monitor — environment check"
echo "============================================="
check_arch
check_python
check_dsd_fme
check_ffmpeg
check_icecast
check_pulseaudio
check_sdr_chain
check_null_sink
echo "============================================="
if [ "$fails" -eq 0 ]; then
    echo "All required checks passed."
else
    echo "$fails required check(s) failed. See install hints above."
fi
exit "$fails"
