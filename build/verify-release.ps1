#Requires -Version 7.0
[CmdletBinding()]
param([Parameter(Mandatory = $true)][string]$OutputDirectory)

$ErrorActionPreference = "Stop"
$root = Split-Path $PSScriptRoot -Parent
$output = [IO.Path]::GetFullPath($OutputDirectory)
if (Test-Path $output) { throw "Choose a new release verification directory" }
New-Item -ItemType Directory -Path $output | Out-Null
$source = Get-Content ([IO.Path]::Combine($root, "internal", "version", "version.go")) -Raw
if ($source -notmatch 'var Value = "([^"]+)"') { throw "Cannot read source version" }
$version = $Matches[1]
$suffix = if ($IsWindows) { ".exe" } else { "" }
$binaryName = "maistr0$suffix"
$saved = @{}
foreach ($name in @("GOOS", "GOARCH", "CGO_ENABLED", "GOFLAGS", "MAISTR0_RELEASE_BINARY", "MAISTR0_UPGRADE_BINARY", "MAISTR0_RELEASE_ARCHIVE")) {
    $saved[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
}
Push-Location $root
try {
    $env:GOOS = ""; $env:GOARCH = ""; $env:CGO_ENABLED = "0"; $env:GOFLAGS = ""
    $hashes = @()
    $archives = @()
    foreach ($trial in @(1, 2)) {
        $folder = Join-Path $output "trial-$trial"
        New-Item -ItemType Directory -Path $folder | Out-Null
        $binary = Join-Path $folder $binaryName
        & go build -trimpath -ldflags "-s -w -X github.com/maistr0/maistr0/internal/version.Value=$version" -o $binary github.com/maistr0/maistr0/cmd/maistr0
        if ($LASTEXITCODE -ne 0) { throw "Release build $trial failed" }
        $hashes += (Get-FileHash $binary -Algorithm SHA256).Hash
        foreach ($name in @("LICENSE", "README.md", "changelog.md", "SECURITY.md")) {
            Copy-Item (Join-Path $root $name) $folder
        }
        $docs = Join-Path $folder "docs"
        New-Item -ItemType Directory -Path $docs | Out-Null
        Get-ChildItem (Join-Path $root "docs") -File | Where-Object { $_.Extension -in @(".md", ".json", ".jsonl") } |
            Sort-Object Name | Copy-Item -Destination $docs
        $examples = Join-Path $folder "examples"
        New-Item -ItemType Directory -Path $examples | Out-Null
        Copy-Item ([IO.Path]::Combine($root, "examples", "benchmark.json")) $examples
        $packaging = Join-Path $folder "packaging\systemd"
        New-Item -ItemType Directory -Path $packaging -Force | Out-Null
        Copy-Item ([IO.Path]::Combine($root, "packaging", "systemd", "maistr0.service")) $packaging
        $epoch = [DateTime]::new(2000, 1, 1, 0, 0, 0, [DateTimeKind]::Utc)
        Get-ChildItem $folder -Recurse | ForEach-Object { $_.LastWriteTimeUtc = $epoch }
        if ($IsWindows) {
            $archive = Join-Path $output "trial-$trial.zip"
            Compress-Archive -Path (Join-Path $folder "*") -DestinationPath $archive
        } else {
            $archive = Join-Path $output "trial-$trial.tar.gz"
            & tar --sort=name --mtime=@946684800 --owner=0 --group=0 --numeric-owner -czf $archive -C $folder .
            if ($LASTEXITCODE -ne 0) { throw "Archive creation failed" }
        }
        $archives += $archive
    }
    if ($hashes[0] -ne $hashes[1]) { throw "Release binaries are not byte-reproducible" }
    $archiveHashes = @($archives | ForEach-Object { (Get-FileHash $_ -Algorithm SHA256).Hash })
    if ($archiveHashes[0] -ne $archiveHashes[1]) { throw "Native verification archives are not byte-reproducible" }
    $env:MAISTR0_RELEASE_BINARY = [IO.Path]::Combine($output, "trial-1", $binaryName)
    $env:MAISTR0_UPGRADE_BINARY = [IO.Path]::Combine($output, "trial-2", $binaryName)
    $env:MAISTR0_RELEASE_ARCHIVE = $archives[0]
    & go test ./internal/integration -run '^TestReleaseCandidateInstallUpgrade$' -count=1 -timeout=120s
    if ($LASTEXITCODE -ne 0) { throw "Packaged install/upgrade acceptance failed" }
    @{
        schema_version = 1; version = $version; go = (& go version) -join ""
        platform = [Environment]::OSVersion.Platform.ToString()
        binary_sha256 = $hashes; archive_sha256 = $archiveHashes
        byte_reproducible_on_this_host = $true; packaged_install_and_restart = "passed"
        scope = "same-source native candidate; not a historical-release migration, publisher signature, GUI, or hardware qualification"
    } | ConvertTo-Json -Depth 4 | Set-Content -Encoding UTF8 (Join-Path $output "acceptance.json")
} finally {
    foreach ($name in $saved.Keys) { [Environment]::SetEnvironmentVariable($name, $saved[$name], "Process") }
    Pop-Location
}
