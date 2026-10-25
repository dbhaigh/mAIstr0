#!/usr/bin/env sh
# Runs the Go race detector, which requires cgo and a C compiler.
# Usage: ./build/test-race.sh [packages] [go test flags]
# ./... is added when no package pattern (./x, ../x, host.tld/x, x/...) is given.
set -eu
cd "$(dirname "$0")/.."
has_package=false
for arg in "$@"; do
	case "$arg" in
	. | .. | ./* | ../* | *...) has_package=true ;;
	-*) ;;
	*.*/*) has_package=true ;;
	esac
done
if [ "$has_package" = false ]; then
	set -- "$@" ./...
fi
CGO_ENABLED=1 exec go test -race "$@"
