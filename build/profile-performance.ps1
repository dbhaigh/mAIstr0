#Requires -Version 7.0
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$OutputDirectory,
    [ValidateSet("Baseline", "Profile", "Both", "Analyze")][string]$Mode = "Both",
    [string]$GoExecutable = "go",
    [string]$WslDistribution = "",
    [string[]]$Workloads = @(),
    [int]$Repetitions = 5,
    [string]$BenchTime = "1s",
    [string]$ProfileTime = "3s"
)

$ErrorActionPreference = "Stop"
if ($Repetitions -lt 1) { throw "Repetitions must be positive" }
$repository = Split-Path $PSScriptRoot -Parent
$output = [IO.Path]::GetFullPath($OutputDirectory)
if ($Mode -ne "Analyze" -and (Test-Path (Join-Path $output "manifest.json"))) {
    throw "Choose a new output directory; existing measurements will not be overwritten"
}
New-Item -ItemType Directory -Path $output -Force | Out-Null
$artifactRoot = $output
if ($WslDistribution) {
    $converted = & wsl.exe -d $WslDistribution --exec wslpath -a $output
    if ($LASTEXITCODE -ne 0 -or -not $converted) { throw "Could not resolve WSL output path" }
    $artifactRoot = $converted.Trim()
}

function Invoke-Go([string[]]$Arguments) {
    Push-Location $repository
    try {
        if ($WslDistribution) {
            & wsl.exe -d $WslDistribution --cd $repository --exec $GoExecutable @Arguments
        } else {
            & $GoExecutable @Arguments
        }
        if ($LASTEXITCODE -ne 0) { throw "Go command failed: $($Arguments -join ' ')" }
    } finally {
        Pop-Location
    }
}

function Start-GoReader([string[]]$Arguments) {
    $info = [Diagnostics.ProcessStartInfo]::new()
    $info.FileName = $GoExecutable
    if ($WslDistribution) {
        $info.FileName = "wsl.exe"
        $Arguments = @("-d", $WslDistribution, "--cd", $repository, "--exec", $GoExecutable) + $Arguments
    }
    foreach ($argument in $Arguments) { $info.ArgumentList.Add($argument) }
    $info.WorkingDirectory = $repository
    $info.UseShellExecute = $false
    $info.RedirectStandardOutput = $true
    return [Diagnostics.Process]::Start($info)
}

function Write-TraceSummaries([string]$Name, [string]$Prefix) {
    foreach ($kind in @("sched", "sync")) {
        $destination = Join-Path $output "$Name-trace-$kind.pprof"
        $process = Start-GoReader @("tool", "trace", "-pprof=$kind", "$Prefix-trace.out")
        try {
            $stream = [IO.File]::Create($destination)
            try { $process.StandardOutput.BaseStream.CopyTo($stream) } finally { $stream.Dispose() }
            $process.WaitForExit()
            if ($process.ExitCode -ne 0) { throw "Trace conversion failed for $Name ($kind)" }
        } finally {
            $process.Dispose()
        }
        Invoke-Go @("tool", "pprof", "-top", "-nodecount=15", "$Prefix-trace-$kind.pprof") |
            Set-Content -Encoding UTF8 (Join-Path $output "$Name-trace-$kind.txt")
    }
    $peak = [long]0
    $samples = 0
    $process = Start-GoReader @("tool", "trace", "-d=parsed", "$Prefix-trace.out")
    try {
        while ($null -ne ($line = $process.StandardOutput.ReadLine())) {
            if ($line -match 'Name="/memory/classes/heap/objects:bytes".*Uint64\((\d+)\)') {
                $peak = [Math]::Max($peak, [long]$Matches[1])
                $samples++
            }
        }
        $process.WaitForExit()
        if ($process.ExitCode -ne 0 -or $samples -eq 0) {
            throw "Could not read heap metrics from $Name execution trace"
        }
    } finally {
        $process.Dispose()
    }
    @{
        HeapObjectsPeakBytes = $peak
        HeapMetricSamples = $samples
        Scope = "Whole instrumented process, including setup/calibration; not retained live heap"
    } | ConvertTo-Json | Set-Content -Encoding UTF8 (Join-Path $output "$Name-trace-metrics.json")
}

$matrix = @(
    @{ Name = "direct"; Package = "orchestrator"; Pattern = '^BenchmarkProfileRouting$/^Direct$' },
    @{ Name = "transparent"; Package = "orchestrator"; Pattern = '^BenchmarkProfileRouting$/^Transparent$' },
    @{ Name = "slow-stream"; Package = "orchestrator"; Pattern = '^BenchmarkProfileRouting$/^SlowStream$' },
    @{ Name = "heterogeneous"; Package = "orchestrator"; Pattern = '^BenchmarkProfileHeterogeneousRouting$' },
    @{ Name = "workflow"; Package = "orchestrator"; Pattern = '^BenchmarkProfileWorkflow$'; Fixed = "256x" },
    @{ Name = "snapshot"; Package = "orchestrator"; Pattern = '^BenchmarkBuildSnapshot$' },
    @{ Name = "memory-http"; Package = "orchestrator"; Pattern = '^BenchmarkProfileMemorySyncHTTP$' },
    @{ Name = "shard-lifecycle"; Package = "orchestrator"; Pattern = '^BenchmarkProfileShardLifecycle$' },
    @{ Name = "inventory"; Package = "nodeagent"; Pattern = '^BenchmarkProfileInventory$' },
    @{ Name = "telemetry"; Package = "nodeagent"; Pattern = '^BenchmarkProfileTelemetry$' },
    @{ Name = "publication"; Package = "hub"; Pattern = '^BenchmarkProfilePublication$' },
    @{ Name = "memory-operations"; Package = "memory"; Pattern = '^BenchmarkProfileMemoryOperations$'; Fixed = "256x" },
    @{ Name = "sync-page"; Package = "memory"; Pattern = '^BenchmarkProfileSyncPage$' },
    @{ Name = "sync-transfer"; Package = "memory"; Pattern = '^BenchmarkMemoryHistoryPageTransfer$/^100K$' }
)
if ($Workloads.Count -gt 0) {
    foreach ($name in $Workloads) {
        if ($matrix.Name -notcontains $name) { throw "Unknown workload: $name" }
    }
    $matrix = @($matrix | Where-Object { $Workloads -contains $_.Name })
}
if ($Mode -eq "Analyze") {
    if (-not (Test-Path (Join-Path $output "manifest.json"))) { throw "Analyze requires an existing profile manifest" }
    foreach ($workload in $matrix) {
        $prefix = Join-Path $artifactRoot $workload.Name
        if ($WslDistribution) { $prefix = $artifactRoot + "/" + $workload.Name }
        Write-TraceSummaries $workload.Name $prefix
    }
    return
}
$manifest = @{
    GeneratedUTC = [DateTime]::UtcNow.ToString("o")
    GoVersion = (Invoke-Go @("version")) -join "`n"
    Mode = $Mode
    WslDistribution = $WslDistribution
    CPU = 2
    Repetitions = $Repetitions
    BenchTime = $BenchTime
    ProfileTime = $ProfileTime
    LatencyWindow = 8192
    Workloads = $matrix
    Commands = @()
}
$manifestPath = Join-Path $output "manifest.json"
$manifest | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 $manifestPath

foreach ($workload in $matrix) {
    $package = "github.com/maistr0/maistr0/internal/" + $workload.Package
    $common = @("test", $package, "-p=1", "-run=^$", "-bench=$($workload.Pattern)",
        "-benchmem", "-cpu=2", "-timeout=180s")
    if ($Mode -ne "Profile") {
        $duration = $BenchTime
        if ($workload.Fixed) { $duration = $workload.Fixed }
        $arguments = $common + @("-count=$Repetitions", "-benchtime=$duration", "-json")
        $manifest.Commands += ,$arguments
        $manifest | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 $manifestPath
        $log = Join-Path $output ($workload.Name + "-bench.jsonl")
        Invoke-Go $arguments | Tee-Object -FilePath $log
        if (-not (Select-String -Path $log -Pattern "ns/op" -Quiet)) {
            throw "No benchmark results for $($workload.Name)"
        }
    }
    if ($Mode -ne "Baseline") {
        $duration = $ProfileTime
        if ($workload.Fixed) { $duration = $workload.Fixed }
        $prefix = Join-Path $artifactRoot $workload.Name
        if ($WslDistribution) { $prefix = $artifactRoot + "/" + $workload.Name }
        $binary = $prefix + ".test"
        if (-not $WslDistribution) { $binary += ".exe" }
        $arguments = $common + @("-count=1", "-benchtime=$duration",
            "-cpuprofile=$prefix-cpu.pprof", "-memprofile=$prefix-memory.pprof", "-memprofilerate=65536",
            "-trace=$prefix-trace.out", "-mutexprofile=$prefix-mutex.pprof", "-mutexprofilefraction=1",
            "-blockprofile=$prefix-block.pprof", "-blockprofilerate=1000", "-o=$binary")
        $manifest.Commands += ,$arguments
        $manifest | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 $manifestPath
        $log = Join-Path $output ($workload.Name + "-profile.txt")
        Invoke-Go $arguments | Tee-Object -FilePath $log
        if (-not (Select-String -Path $log -Pattern "ns/op" -Quiet)) {
            throw "No profiled benchmark results for $($workload.Name)"
        }
        foreach ($profile in @(
            @{ File = "cpu"; Sample = "cpu" },
            @{ File = "memory"; Sample = "alloc_space" },
            @{ File = "memory"; Sample = "inuse_space" },
            @{ File = "mutex"; Sample = "delay" },
            @{ File = "block"; Sample = "delay" }
        )) {
            Invoke-Go @("tool", "pprof", "-top", "-nodecount=15", "-sample_index=$($profile.Sample)",
                $binary, "$prefix-$($profile.File).pprof") |
                Set-Content -Encoding UTF8 (Join-Path $output ("$($workload.Name)-$($profile.Sample)-$($profile.File).txt"))
        }
        Write-TraceSummaries $workload.Name $prefix
    }
    $manifest | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 $manifestPath
}
