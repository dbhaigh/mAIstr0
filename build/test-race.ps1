# Runs the Go race detector, preparing the cgo C toolchain on Windows.
#
# The race detector requires cgo. On Windows, GCC's internal programs (cc1.exe,
# as.exe, ld.exe) live outside the compiler's bin directory, so Windows resolves
# their DLLs from PATH. An earlier PATH entry that ships older copies of the same
# MinGW runtime DLLs (for example libgcc_s_seh-1.dll or libzstd.dll) makes cc1.exe
# fail with STATUS_ENTRYPOINT_NOT_FOUND, which surfaces as a silent
# "runtime/cgo: cgo.exe: exit status 2". This script puts the compiler's bin
# directory first for this process only and verifies the toolchain before testing.
#
# Usage:
#   .\build\test-race.ps1                          # go test -race ./...
#   .\build\test-race.ps1 -count=1                 # flags only: ./... is added
#   .\build\test-race.ps1 ./internal/hub -count=1  # packages and go test flags
#   .\build\test-race.ps1 -CheckOnly               # verify the toolchain only
param(
	[switch]$CheckOnly,
	[Parameter(ValueFromRemainingArguments = $true)]
	[string[]]$GoTestArgs
)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$onWindows = [Environment]::OSVersion.Platform -eq [PlatformID]::Win32NT

function Test-CCompiler([string]$Compiler) {
	$work = Join-Path ([IO.Path]::GetTempPath()) ("maistr0-cgo-probe-" + [Guid]::NewGuid().ToString("N"))
	New-Item -ItemType Directory -Path $work | Out-Null
	try {
		$source = Join-Path $work "probe.c"
		$binary = Join-Path $work "probe.exe"
		Set-Content -Path $source -Value "int main(void) { return 0; }" -Encoding ascii
		$output = & $Compiler -O2 -o $binary $source 2>&1 | Out-String
		$code = $LASTEXITCODE
		if ($code -eq 0) {
			& $binary
			$code = $LASTEXITCODE
		}
		return [pscustomobject]@{ ExitCode = $code; Output = $output.Trim() }
	} finally {
		Remove-Item -Recurse -Force -Path $work -ErrorAction SilentlyContinue
	}
}

# Lists PATH directories, ahead of the compiler, that contain DLLs which also ship
# with the compiler and would therefore be loaded by its internal programs.
function Get-ShadowingDirectories([string]$CompilerDir, [string[]]$PathEntries) {
	$compilerDlls = Get-ChildItem -Path $CompilerDir -Filter "*.dll" -File | ForEach-Object { $_.Name }
	$results = @()
	foreach ($entry in $PathEntries) {
		if (-not $entry) { continue }
		if ($entry.TrimEnd('\') -ieq $CompilerDir.TrimEnd('\')) { break }
		if (-not (Test-Path -LiteralPath $entry -PathType Container)) { continue }
		$shared = @($compilerDlls | Where-Object { Test-Path -LiteralPath (Join-Path $entry $_) })
		if ($shared.Count -gt 0) {
			$results += [pscustomobject]@{ Directory = $entry; Dlls = ($shared -join ", ") }
		}
	}
	return $results
}

if ($onWindows) {
	$ccName = (go env CC).Trim()
	if (-not $ccName) { $ccName = "gcc" }
	$command = Get-Command $ccName -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
	if (-not $command) {
		foreach ($candidate in @("C:\msys64\ucrt64\bin\gcc.exe", "C:\msys64\mingw64\bin\gcc.exe")) {
			if (Test-Path $candidate) {
				$command = Get-Command $candidate
				break
			}
		}
	}
	if (-not $command) {
		throw "No C compiler '$ccName' was found. The race detector requires cgo; install a MinGW-w64 GCC (for example MSYS2's mingw-w64-ucrt-x86_64-gcc) and add its bin directory to PATH, or set CC."
	}
	$compiler = $command.Source
	$compilerDir = Split-Path -Parent $compiler
	$originalEntries = $env:PATH -split ";"

	$initial = Test-CCompiler $compiler
	$env:PATH = (@($compilerDir) + ($originalEntries | Where-Object { $_ -and $_.TrimEnd('\') -ine $compilerDir.TrimEnd('\') })) -join ";"
	$prepared = Test-CCompiler $compiler

	if ($prepared.ExitCode -ne 0) {
		$hex = "0x{0:X8}" -f $prepared.ExitCode
		throw "C compiler '$compiler' cannot build a trivial program (exit $($prepared.ExitCode), $hex). $($prepared.Output) Reinstall or update the toolchain, for example with 'pacman -Syu' in MSYS2."
	}
	if ($initial.ExitCode -ne 0) {
		$hex = "0x{0:X8}" -f $initial.ExitCode
		Write-Warning "C compiler failed with the inherited PATH (exit $hex) and works with '$compilerDir' first; using that order for this run."
		foreach ($shadow in Get-ShadowingDirectories $compilerDir $originalEntries) {
			Write-Warning "  '$($shadow.Directory)' shadows: $($shadow.Dlls)"
		}
		Write-Warning "To fix plain 'go test -race', move '$compilerDir' ahead of those directories in PATH."
	}
	Write-Host "cgo toolchain: $compiler"
}

$env:CGO_ENABLED = "1"
if ($CheckOnly) {
	return
}
$packagePattern = '^(\.{1,2}([/\\]|$)|[\w.-]+\.\w+/)|\.\.\.$'
if (-not ($GoTestArgs | Where-Object { $_ -match $packagePattern })) {
	$GoTestArgs = @($GoTestArgs | Where-Object { $_ }) + "./..."
}
go test -race @GoTestArgs
exit $LASTEXITCODE
