#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
	echo "Usage: $0 <version> <windows-zip> <output-exe>" >&2
	exit 2
fi

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
version="$1"
archive="$2"
output="$3"

if [[ ! "$version" =~ ^[0-9A-Za-z][0-9A-Za-z._+-]*$ ]]; then
	echo "Invalid release version: $version" >&2
	exit 2
fi
if [[ ! -f "$archive" ]]; then
	echo "Windows release archive not found: $archive" >&2
	exit 1
fi
if ! command -v makensis >/dev/null 2>&1; then
	echo "makensis is required to create the Windows self-extracting archive." >&2
	exit 1
fi
if ! command -v unzip >/dev/null 2>&1; then
	echo "unzip is required to stage the Windows release archive." >&2
	exit 1
fi

archive="$(cd "$(dirname "$archive")" && pwd)/$(basename "$archive")"
mkdir -p "$(dirname "$output")"
output="$(cd "$(dirname "$output")" && pwd)/$(basename "$output")"
stage="$(mktemp -d)"
trap 'rm -rf "$stage"' EXIT

unzip -q "$archive" -d "$stage"
if [[ ! -s "$stage/maistr0.exe" ]]; then
	echo "Windows release archive does not contain maistr0.exe: $archive" >&2
	exit 1
fi

makensis -V2 \
	"-DVERSION=$version" \
	"-DSOURCE_DIR=$stage" \
	"-DOUTPUT_FILE=$output" \
	"$root/build/windows-sfx.nsi"

if [[ ! -s "$output" ]]; then
	echo "NSIS did not create the expected self-extracting archive: $output" >&2
	exit 1
fi
