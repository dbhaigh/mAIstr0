#!/bin/bash
set -euo pipefail

agent="$HOME/Library/LaunchAgents/com.maistr0.agent.plist"
binary="$HOME/.local/bin/maistr0"
label="com.maistr0.agent"
domain="gui/$(id -u)"

if launchctl print "$domain/$label" >/dev/null 2>&1; then
	launchctl bootout "$domain" "$agent"
fi
rm -f "$agent" "$binary"
rm -rf "$HOME/.local/darwin-amd64" "$HOME/.local/darwin-arm64"
echo "Removed the mAIstr0 LaunchAgent and executable. Configuration and data were kept."
