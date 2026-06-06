#!/usr/bin/env bash
# Create the `dmr_capture` null sink that dsd-fme reads from via the default
# input device `pulse:dmr_capture.monitor`. Without this sink the live
# pipeline can't open its input — the classic "worked on the old Pi because
# someone set up the sink years ago" failure when migrating to a new Pi.
#
# Works with both classic PulseAudio and the PipeWire-pulse server that ships
# by default on Raspberry Pi OS Bookworm — `pactl` talks to either one.
#
# Run as the user that will run dmr-monitor (NOT root, NOT via sudo) so the
# sink lands in that user's audio session.
set -euo pipefail

SINK_NAME="dmr_capture"

if [ "$(id -u)" -eq 0 ]; then
    echo "Refusing to run as root — run as the user that runs dmr-monitor." >&2
    exit 1
fi

if ! command -v pactl >/dev/null 2>&1; then
    echo "pactl not found. Install it first: bash scripts/install-deps.sh" >&2
    exit 1
fi

if ! pactl info >/dev/null 2>&1; then
    echo "No PulseAudio/PipeWire session reachable by pactl." >&2
    echo "Log into a desktop/user session so the audio server is running, then retry." >&2
    exit 1
fi

# 1. Load the sink now (idempotent — skip if it already exists).
if pactl list short sinks 2>/dev/null | grep -qw "$SINK_NAME"; then
    echo "Sink '$SINK_NAME' already loaded."
else
    echo "Loading null sink '$SINK_NAME'..."
    pactl load-module module-null-sink \
        sink_name="$SINK_NAME" \
        sink_properties=device.description="$SINK_NAME" >/dev/null
fi

# 2. Verify the monitor source dsd-fme will read from.
if pactl list short sources 2>/dev/null | grep -qw "${SINK_NAME}.monitor"; then
    echo "OK: capture source '${SINK_NAME}.monitor' is available."
else
    echo "ERROR: '${SINK_NAME}.monitor' not found after load." >&2
    exit 1
fi

# 3. Persist across reboots with a systemd --user oneshot that recreates the
#    sink once the audio server is up. Because pactl reaches PulseAudio or
#    PipeWire-pulse interchangeably, the same unit works on either stack.
UNIT_DIR="$HOME/.config/systemd/user"
UNIT="$UNIT_DIR/dmr-capture-sink.service"
mkdir -p "$UNIT_DIR"
cat > "$UNIT" <<'EOF'
[Unit]
Description=DMR capture null sink (dmr_capture)
# Ordering hints only — these units may not exist on every audio stack.
After=pipewire-pulse.service pulseaudio.service

[Service]
Type=oneshot
RemainAfterExit=yes
# Wait for the audio server to be reachable, then create the sink if it
# isn't already present (no-op on every boot after the first).
ExecStart=/usr/bin/env bash -c 'for i in $(seq 1 15); do pactl info >/dev/null 2>&1 && break; sleep 1; done; pactl list short sinks | grep -qw dmr_capture || pactl load-module module-null-sink sink_name=dmr_capture sink_properties=device.description=dmr_capture'

[Install]
WantedBy=default.target
EOF

systemctl --user daemon-reload
systemctl --user enable dmr-capture-sink.service >/dev/null 2>&1 || true
# Let the user service run without an active login session (survives reboot).
loginctl enable-linger "$(whoami)" >/dev/null 2>&1 || true

echo ""
echo "Done. The '$SINK_NAME' sink is loaded now and will be recreated on boot."
echo "dsd-fme input device:  pulse:${SINK_NAME}.monitor"
