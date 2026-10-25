# Cross-compiles maistr0 for Windows and Linux from any host with Go installed.
param(
	[ValidateSet("maistr0.exe", "maistr0-update.exe")]
	[string]$WindowsBinary = "maistr0.exe"
)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$versionSource = Get-Content "internal/version/version.go" -Raw
if ($versionSource -notmatch 'var Value = "([^"]+)"') {
	throw "Could not read release version from internal/version/version.go"
}
$version = $Matches[1]
$versionFile = "dist/version.txt"
New-Item -ItemType Directory -Force -Path "dist" | Out-Null
Set-Content -Path $versionFile -Value $version -NoNewline
$ldflags = "-s -w -X github.com/maistr0/maistr0/internal/version.Value=$version"

New-Item -ItemType Directory -Force -Path "dist/windows-amd64" | Out-Null
New-Item -ItemType Directory -Force -Path "dist/linux-amd64" | Out-Null
New-Item -ItemType Directory -Force -Path "dist/linux-ubuntu-amd64" | Out-Null

Write-Host "Building windows/amd64..."
$savedEnvironment = @{}
foreach ($name in @("GOOS", "GOARCH", "CGO_ENABLED", "GOFLAGS")) {
	$savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
}
try {
	$env:GOOS = "windows"; $env:GOARCH = "amd64"; $env:CGO_ENABLED = "0"
	go build -trimpath -ldflags $ldflags -o "dist\windows-amd64\$WindowsBinary" .\cmd\maistr0
	if ($LASTEXITCODE -ne 0) { throw "windows build failed" }

	Write-Host "Building linux/amd64..."
	$env:GOOS = "linux"; $env:GOARCH = "amd64"; $env:CGO_ENABLED = "0"
	go build -trimpath -ldflags $ldflags -o "dist\linux-amd64\maistr0" .\cmd\maistr0
	if ($LASTEXITCODE -ne 0) { throw "linux build failed" }

	Write-Host "Building ubuntu linux/amd64..."
	$env:GOOS = "linux"; $env:GOARCH = "amd64"; $env:CGO_ENABLED = "0"; $env:GOFLAGS = "-tags=ubuntu"
	go build -trimpath -ldflags $ldflags -o "dist\linux-ubuntu-amd64\maistr0" .\cmd\maistr0
	if ($LASTEXITCODE -ne 0) { throw "ubuntu linux build failed" }
} finally {
	foreach ($name in $savedEnvironment.Keys) {
		[Environment]::SetEnvironmentVariable($name, $savedEnvironment[$name], "Process")
	}
}
go run .\cmd\updatebundle -root .\dist -version $version -windows-binary $WindowsBinary
if ($LASTEXITCODE -ne 0) { throw "peer bundle metadata generation failed" }
Write-Host "Done. Binaries in dist/windows-amd64, dist/linux-amd64, and dist/linux-ubuntu-amd64"
