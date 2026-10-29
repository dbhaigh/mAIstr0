#!/bin/bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
package_root="$(cd "$script_dir/../.." && pwd)"
source_binary="$package_root/maistr0"
install_dir="$HOME/.local/bin"
binary="$install_dir/maistr0"
agent_dir="$HOME/Library/LaunchAgents"
agent="$agent_dir/com.maistr0.agent.plist"
label="com.maistr0.agent"
domain="gui/$(id -u)"
trap 'rm -f "$agent.tmp"' EXIT

if [[ ! -f "$source_binary" ]]; then
	echo "Extract the macOS release archive and run packaging/launchd/install.sh from its directory." >&2
	exit 1
fi

mkdir -p "$install_dir" "$agent_dir"
install -m 0755 "$source_binary" "$binary"

xml_path="$(printf '%s' "$binary" | sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g')"
xml_path="$(printf '%s' "$xml_path" | sed -e 's/\\/\\\\/g' -e 's/&/\\&/g' -e 's/|/\\|/g')"
sed "s|/usr/local/bin/maistr0|$xml_path|g" "$script_dir/com.maistr0.agent.plist" > "$agent.tmp"
plutil -lint "$agent.tmp"
if launchctl print "$domain/$label" >/dev/null 2>&1; then
	launchctl bootout "$domain" "$agent"
fi
mv "$agent.tmp" "$agent"
for target in darwin-amd64 darwin-arm64; do
	if [[ -d "$package_root/$target" ]]; then
		mkdir -p "$HOME/.local/$target"
		cp -R "$package_root/$target/." "$HOME/.local/$target/"
	fi
done
launchctl bootstrap "$domain" "$agent"
trap - EXIT
echo "Installed and started the mAIstr0 per-user LaunchAgent."
