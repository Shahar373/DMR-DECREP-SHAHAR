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
        print_result "$PASS" "dsd-fme" "$(command -v dsd-fme)"
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
        print_result "$FAIL" "ffmpeg" "not found"
        echo "       Install: sudo apt install -y ffmpeg"
        fails=$((fails + 1))
    fi
}

# 4. Icecast2
check_icecast() {
    if command -v icecast2 >/dev/null 2>&1; then
        print_result "$PASS" "icecast2" "$(command -v icecast2)"
    elif dpkg -l icecast2 >/dev/null 2>&1; then
        print_result "$PASS" "icecast2" "installed (dpkg)"
    else
        print_result "$FAIL" "icecast2" "not found"
        echo "       Install: sudo apt install -y icecast2"
        fails=$((fails + 1))
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

# 6. SDRconnect (warn only — manual install)
check_sdrconnect() {
    if command -v sdrconnect >/dev/null 2>&1; then
        print_result "$PASS" "sdrconnect" "$(command -v sdrconnect)"
    elif [ -d "/opt/SDRconnect" ] || [ -d "$HOME/SDRconnect" ]; then
        print_result "$PASS" "sdrconnect" "found in install dir"
    else
        print_result "$WARN" "sdrconnect" "not found (install manually from SDRplay)"
    fi
}

# 7. dmr_capture null sink (informational)
check_null_sink() {
    if ! command -v pactl >/dev/null 2>&1; then
        return
    fi
    if pactl list short modules 2>/dev/null | grep -q "dmr_capture"; then
        print_result "$PASS" "dmr_capture sink" "loaded"
    else
        print_result "$INFO" "dmr_capture sink" "not loaded yet (Phase 2 will create it)"
    fi
}

echo "DMR Cap+ Monitor — Phase 0 environment check"
echo "============================================="
check_python
check_dsd_fme
check_ffmpeg
check_icecast
check_pulseaudio
check_sdrconnect
check_null_sink
echo "============================================="
if [ "$fails" -eq 0 ]; then
    echo "All required checks passed."
else
    echo "$fails required check(s) failed. See install hints above."
fi
exit "$fails"
