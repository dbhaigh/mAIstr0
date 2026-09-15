# Cross-compiles maistr0 for Windows and Linux from any host with Go installed.
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$versionFile = "dist/version.txt"
if (Test-Path $versionFile) {
	$previous = [decimal](Get-Content $versionFile -Raw).Trim()
	$version = ($previous + [decimal]"0.0001").ToString("0.0000", [Globalization.CultureInfo]::InvariantCulture)
} else {
	$version = "0.0001"
}
New-Item -ItemType Directory -Force -Path "dist" | Out-Null
Set-Content -Path $versionFile -Value $version -NoNewline
$ldflags = "-s -w -X github.com/maistr0/maistr0/internal/version.Value=$version"

New-Item -ItemType Directory -Force -Path "dist/windows-amd64" | Out-Null
New-Item -ItemType Directory -Force -Path "dist/linux-amd64" | Out-Null
New-Item -ItemType Directory -Force -Path "dist/linux-ubuntu-amd64" | Out-Null

Write-Host "Building windows/amd64..."
$env:GOOS = "windows"; $env:GOARCH = "amd64"; $env:CGO_ENABLED = "0"
go build -ldflags $ldflags -o "dist/windows-amd64/maistr0.exe" ./cmd/maistr0
if ($LASTEXITCODE -ne 0) { throw "windows build failed" }

Write-Host "Building linux/amd64..."
$env:GOOS = "linux"; $env:GOARCH = "amd64"; $env:CGO_ENABLED = "0"
go build -ldflags $ldflags -o "dist/linux-amd64/maistr0" ./cmd/maistr0
if ($LASTEXITCODE -ne 0) { throw "linux build failed" }

Write-Host "Building ubuntu linux/amd64..."
$env:GOOS = "linux"; $env:GOARCH = "amd64"; $env:CGO_ENABLED = "0"; $env:GOFLAGS = "-tags=ubuntu"
go build -ldflags $ldflags -o "dist/linux-ubuntu-amd64/maistr0" ./cmd/maistr0
if ($LASTEXITCODE -ne 0) { throw "ubuntu linux build failed" }

Remove-Item Env:GOOS, Env:GOARCH, Env:CGO_ENABLED, Env:GOFLAGS
Write-Host "Done. Binaries in dist/windows-amd64, dist/linux-amd64, and dist/linux-ubuntu-amd64"
