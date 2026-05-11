#!/usr/bin/env bash
# Install dmr-monitor as a systemd user service that starts on boot.
# Run once from the project root: bash scripts/install-service.sh
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
USER="$(whoami)"
SERVICE_DIR="$HOME/.config/systemd/user"
SERVICE_DST="$SERVICE_DIR/dmr-monitor.service"

mkdir -p "$SERVICE_DIR"

# Substitute the actual repo path into the unit file
sed "s|/home/shahar/DMR-DECREP-SHAHAR|$REPO|g" \
    "$REPO/scripts/dmr-monitor.service" > "$SERVICE_DST"

systemctl --user daemon-reload
systemctl --user enable dmr-monitor
systemctl --user start  dmr-monitor

# Allow the service to run even when no user session is active (survives logout/reboot)
loginctl enable-linger "$USER"

echo ""
echo "Installed. Commands:"
echo "  systemctl --user status  dmr-monitor"
echo "  systemctl --user stop    dmr-monitor"
echo "  systemctl --user restart dmr-monitor"
echo "  journalctl --user -u dmr-monitor -f   # live logs"
