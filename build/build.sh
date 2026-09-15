#!/usr/bin/env bash
# Cross-compiles maistr0 for Windows and Linux from any host with Go installed.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

version_file="dist/version.txt"
if [[ -f "$version_file" ]]; then
	version=$(awk -v v="$(cat "$version_file")" 'BEGIN { printf "%.4f", v + 0.0001 }')
else
	version="0.0001"
fi
mkdir -p dist
printf '%s' "$version" > "$version_file"
ldflags="-s -w -X github.com/maistr0/maistr0/internal/version.Value=${version}"

mkdir -p dist/windows-amd64 dist/linux-amd64 dist/linux-ubuntu-amd64

echo "Building windows/amd64..."
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "$ldflags" -o dist/windows-amd64/maistr0.exe ./cmd/maistr0

echo "Building linux/amd64..."
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "$ldflags" -o dist/linux-amd64/maistr0 ./cmd/maistr0

echo "Building ubuntu linux/amd64..."
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 GOFLAGS='-tags=ubuntu' go build -ldflags "$ldflags" -o dist/linux-ubuntu-amd64/maistr0 ./cmd/maistr0

echo "Done. Binaries in dist/windows-amd64, dist/linux-amd64, and dist/linux-ubuntu-amd64"
