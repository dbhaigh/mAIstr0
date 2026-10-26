[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$BeforeDirectory,
    [Parameter(Mandatory = $true)][string]$AfterDirectory,
    [Parameter(Mandatory = $true)][string]$OutputFile
)

$ErrorActionPreference = "Stop"
$beforeManifest = Get-Content (Join-Path $BeforeDirectory "manifest.json") -Raw | ConvertFrom-Json
$afterManifest = Get-Content (Join-Path $AfterDirectory "manifest.json") -Raw | ConvertFrom-Json
foreach ($property in @("GoVersion", "CPU", "Repetitions", "BenchTime", "ProfileTime", "LatencyWindow", "WslDistribution")) {
    if ($beforeManifest.$property -ne $afterManifest.$property) {
        throw "Measurement configuration differs: $property"
    }
}
$beforeMatrix = $beforeManifest.Workloads | Sort-Object Name | Select-Object Name, Package, Pattern, Fixed | ConvertTo-Json -Compress
$afterMatrix = $afterManifest.Workloads | Sort-Object Name | Select-Object Name, Package, Pattern, Fixed | ConvertTo-Json -Compress
if ($beforeMatrix -ne $afterMatrix) { throw "Workload matrices differ" }

function Read-Samples([string]$Directory) {
    $samples = [Collections.Generic.List[object]]::new()
    foreach ($file in Get-ChildItem -LiteralPath $Directory -Filter "*-bench.jsonl" | Sort-Object Name) {
        $output = [Text.StringBuilder]::new()
        foreach ($line in Get-Content -LiteralPath $file.FullName) {
            $event = $line | ConvertFrom-Json
            if ($event.Action -eq "output") { [void]$output.Append($event.Output) }
        }
        foreach ($line in ($output.ToString() -split "`n")) {
            if ($line -notmatch '^(?<name>Benchmark\S+)\s+(?<iterations>\d+)\s+(?<metrics>.*ns/op.*)$') {
                continue
            }
            $name, $iterations, $text = $Matches.name, [long]$Matches.iterations, $Matches.metrics
            $metrics = [ordered]@{}
            foreach ($match in [regex]::Matches($text, '(?<value>[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s+(?<unit>\S+)')) {
                $metrics[$match.Groups["unit"].Value] = [double]::Parse($match.Groups["value"].Value,
                    [Globalization.CultureInfo]::InvariantCulture)
            }
            if (-not $metrics.Contains("ns/op") -or $metrics["ns/op"] -le 0) {
                throw "Invalid benchmark result: $name"
            }
            $samples.Add([pscustomobject]@{
                Name = $name; Iterations = $iterations; Metrics = $metrics; Source = $file.Name
            })
        }
    }
    if ($samples.Count -eq 0) { throw "No benchmark samples in $Directory" }
    return $samples.ToArray()
}

function Summarize([object[]]$Samples, [string]$Unit) {
    $values = @($Samples | ForEach-Object { $_.Metrics[$Unit] } | Sort-Object)
    $middle = [int][Math]::Floor($values.Count / 2)
    $median = $values[$middle]
    if ($values.Count % 2 -eq 0) { $median = ($values[$middle - 1] + $median) / 2 }
    return @{ Min = $values[0]; Median = $median; Max = $values[-1] }
}

$before, $after = @(Read-Samples $BeforeDirectory), @(Read-Samples $AfterDirectory)
$names = @($before.Name | Sort-Object -Unique)
if (($names -join "`n") -ne (($after.Name | Sort-Object -Unique) -join "`n")) {
    throw "Benchmark result sets differ"
}
$results = foreach ($name in $names) {
    $a, $b = @($before | Where-Object Name -eq $name), @($after | Where-Object Name -eq $name)
    if ($a.Count -ne $beforeManifest.Repetitions -or $b.Count -ne $afterManifest.Repetitions) {
        throw "Incorrect repetition count for $name"
    }
    $metrics = [ordered]@{}
    foreach ($unit in $a[0].Metrics.Keys) {
        if (-not $b[0].Metrics.Contains($unit)) { throw "Missing metric $unit for $name" }
        $baseline, $changed = (Summarize $a $unit), (Summarize $b $unit)
        $percent = $null
        if ($baseline.Median -ne 0) { $percent = 100 * ($changed.Median / $baseline.Median - 1) }
        $metrics[$unit] = @{ Before = $baseline; After = $changed; ChangePercent = $percent }
    }
    [pscustomobject]@{ Name = $name; Metrics = $metrics }
}
$report = [ordered]@{
    SchemaVersion = 1
    FixtureVersion = "synthetic-profile-v1"
    GeneratedUTC = [DateTime]::UtcNow.ToString("o")
    BeforeUTC = $beforeManifest.GeneratedUTC
    AfterUTC = $afterManifest.GeneratedUTC
    Environment = @{
        Go = $beforeManifest.GoVersion; BenchmarkCPU = $beforeManifest.CPU
        WslDistribution = $beforeManifest.WslDistribution
        HardwareInference = $false
    }
    Method = @{
        Repetitions = $beforeManifest.Repetitions
        BenchTime = $beforeManifest.BenchTime
        ProfileTime = $beforeManifest.ProfileTime
        LatencyWindow = $beforeManifest.LatencyWindow
        Quantiles = "Nearest rank within each trial; median/min/max across trials, not pooled quantiles"
        ChangePercent = "100 * (after median / before median - 1); null when baseline is zero"
        ProfileTimings = "Excluded from benchmark comparisons"
    }
    Workloads = $beforeManifest.Workloads
    Results = @($results)
    BeforeSamples = $before
    AfterSamples = $after
}
$report | ConvertTo-Json -Depth 12 | Set-Content -Encoding UTF8 $OutputFile
foreach ($result in $results) {
    $metric = $result.Metrics["ns/op"]
    "{0}: {1:N0} -> {2:N0} ns/op ({3:N1}%)" -f $result.Name,
        $metric.Before.Median, $metric.After.Median, $metric.ChangePercent
}
