#!/usr/bin/env bash
set -euo pipefail

if [[ "$(uname -s)" != "Darwin" ]]; then
	echo "Build macOS release packages on a macOS host with Xcode command line tools." >&2
	exit 1
fi

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

version=$(sed -n 's/^[[:space:]]*var Value = "\([^"]*\)".*/\1/p' internal/version/version.go)
if [[ -z "$version" ]]; then
	echo "Could not read release version from internal/version/version.go" >&2
	exit 1
fi
plutil -lint packaging/macos/Info.plist packaging/launchd/com.maistr0.agent.plist

output="$root/dist/macos/v${version}"
mkdir -p "$output"
ldflags="-s -w -X github.com/maistr0/maistr0/internal/version.Value=${version}"

for arch in arm64 amd64; do
	clang_arch="$arch"
	if [[ "$arch" == amd64 ]]; then
		clang_arch=x86_64
	fi
	echo "Building darwin/$arch with cgo..."
	mkdir -p "$root/dist/darwin-$arch"
	CGO_ENABLED=1 GOOS=darwin GOARCH="$arch" CGO_CFLAGS="-arch $clang_arch" CGO_LDFLAGS="-arch $clang_arch" \
		go build -trimpath -ldflags "$ldflags" -o "$root/dist/darwin-$arch/maistr0" ./cmd/maistr0
done
go run ./cmd/updatebundle -root "$root/dist" -version "$version" -darwin-only

for arch in arm64 amd64; do
	stage="$(mktemp -d)"
	archive_stage="$(mktemp -d)"
	trap 'rm -rf "$stage" "$archive_stage"' EXIT
	cp "$root/dist/darwin-$arch/maistr0" "$stage/"
	mkdir -p "$stage/docs" "$stage/packaging/launchd"
	cp LICENSE README.md changelog.md SECURITY.md "$stage/"
	cp docs/*.md docs/*.json docs/*.jsonl "$stage/docs/"
	cp packaging/launchd/com.maistr0.agent.plist packaging/launchd/install.sh \
		packaging/launchd/uninstall.sh "$stage/packaging/launchd/"
	cp -R "$root/dist/darwin-arm64" "$root/dist/darwin-amd64" "$stage/"

	app="$stage/mAIstr0.app"
	mkdir -p "$app/Contents/MacOS"
	cp "$stage/maistr0" "$app/Contents/MacOS/maistr0"
	cp packaging/macos/Info.plist "$app/Contents/Info.plist"
	cp -R "$root/dist/darwin-arm64" "$root/dist/darwin-amd64" "$app/Contents/"
	ditto -c -k --sequesterRsrc --keepParent "$app" "$output/mAIstr0_${version}_darwin_${arch}.zip"
	cp "$stage/maistr0" "$archive_stage/"
	cp -R "$stage/docs" "$stage/packaging" "$stage/darwin-arm64" "$stage/darwin-amd64" "$archive_stage/"
	tar -czf "$output/maistr0_${version}_darwin_${arch}.tar.gz" -C "$archive_stage" .
	rm -rf "$stage" "$archive_stage"
	trap - EXIT
done

(cd "$output" && shasum -a 256 *.zip *.tar.gz > checksums-darwin.txt)
echo "Created unsigned macOS arm64 and amd64 packages in $output"
