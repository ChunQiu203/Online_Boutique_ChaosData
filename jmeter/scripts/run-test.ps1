[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$Config,

    [Parameter(Mandatory = $true)]
    [string]$RunId,

    [string]$Protocol = "http",
    [Alias("Host")]
    [string]$TargetHost = "127.0.0.1",
    [int]$Port = 8080,
    [string]$BasePath = "",
    [string[]]$JMeterProperty = @(),
    [switch]$SkipEnvironmentCheck
)

$ErrorActionPreference = "Stop"
$originalLocation = Get-Location
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$baseDir = Resolve-Path (Join-Path $scriptDir "..")

function Resolve-InputPath {
    param([string]$PathValue)
    if ([System.IO.Path]::IsPathRooted($PathValue)) {
        return (Resolve-Path $PathValue).Path
    }
    $candidateFromOriginal = Join-Path $originalLocation $PathValue
    if (Test-Path $candidateFromOriginal) {
        return (Resolve-Path $candidateFromOriginal).Path
    }
    $candidateFromBase = Join-Path $baseDir $PathValue
    if (Test-Path $candidateFromBase) {
        return (Resolve-Path $candidateFromBase).Path
    }
    throw "Path not found: $PathValue"
}

function Read-PropertiesFile {
    param([string]$PathValue)
    $map = @{}
    Get-Content $PathValue | ForEach-Object {
        $line = $_.Trim()
        if ($line -eq "" -or $line.StartsWith("#") -or $line.StartsWith("!")) {
            return
        }
        $idx = $line.IndexOf("=")
        if ($idx -lt 0) {
            return
        }
        $key = $line.Substring(0, $idx).Trim()
        $value = $line.Substring($idx + 1).Trim()
        $map[$key] = $value
    }
    return $map
}

function Try-CommandText {
    param([scriptblock]$Action)
    try {
        $value = & $Action 2>$null
        if ($LASTEXITCODE -ne $null -and $LASTEXITCODE -ne 0) {
            return ""
        }
        return (($value | Out-String).Trim())
    } catch {
        return ""
    }
}

function Get-Prop {
    param(
        [hashtable]$Props,
        [string]$Name,
        [string]$Default = ""
    )
    if ($Props.ContainsKey($Name)) {
        return [string]$Props[$Name]
    }
    return $Default
}

if ($RunId -notmatch "^[A-Za-z0-9._-]+$") {
    throw "RunId may contain only letters, numbers, dot, underscore and hyphen."
}

$jmeterCmd = Get-Command jmeter -ErrorAction SilentlyContinue
if (-not $jmeterCmd) {
    throw "Apache JMeter is not available in PATH. Install JMeter 5.6.3+ and retry."
}

$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if (-not $pythonCmd) {
    throw "Python is not available in PATH. Python 3.10+ is required."
}

$configPath = Resolve-InputPath $Config
$jmxPath = Join-Path $baseDir "online-boutique.jmx"
$summarizerPath = Join-Path $baseDir "tools/summarize_results.py"

if (-not (Test-Path $jmxPath)) {
    throw "JMX not found: $jmxPath"
}
if (-not (Test-Path $summarizerPath)) {
    throw "Summarizer not found: $summarizerPath"
}

$experimentDir = Join-Path $baseDir ("experiments/" + $RunId)
if (Test-Path $experimentDir) {
    throw "Experiment directory already exists; refusing to overwrite Run ID '$RunId': $experimentDir"
}

$jmeterDir = Join-Path $experimentDir "jmeter"
$monitoringPromDir = Join-Path $experimentDir "monitoring/prometheus"
$monitoringGrafanaDir = Join-Path $experimentDir "monitoring/grafana"
$chaosDir = Join-Path $experimentDir "chaos"
New-Item -ItemType Directory -Force $jmeterDir, $monitoringPromDir, $monitoringGrafanaDir, $chaosDir | Out-Null

Set-Location $baseDir

if (-not $SkipEnvironmentCheck) {
    & (Join-Path $baseDir "scripts/check-environment.ps1") -Protocol $Protocol -Host $TargetHost -Port $Port -BasePath $BasePath
    if ($LASTEXITCODE -ne 0) {
        throw "Environment check failed; JMeter run was not started."
    }
}

$props = Read-PropertiesFile $configPath
$startUtc = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
$jtlPath = Join-Path $jmeterDir "result.jtl"
$jmeterLogPath = Join-Path $jmeterDir "jmeter.log"
$reportDir = Join-Path $jmeterDir "report"
$manifestPath = Join-Path $experimentDir "manifest.csv"
$eventsPath = Join-Path $experimentDir "events.csv"
$exitCodePath = Join-Path $jmeterDir "exit-code.txt"

& (Join-Path $baseDir "scripts/mark-event.ps1") -RunId $RunId -Event TEST_START -Details "JMeter run started"

$repoUrl = Try-CommandText { git remote get-url origin }
$repoCommit = Try-CommandText { git rev-parse HEAD }
$kubeContext = Try-CommandText { kubectl config current-context }
$minikubeProfile = Try-CommandText { minikube profile }
$frontendImage = Try-CommandText { kubectl get deployment frontend -o "jsonpath={.spec.template.spec.containers[?(@.name=='server')].image}" }
$reviewImage = Try-CommandText { kubectl get deployment reviewservice -o "jsonpath={.spec.template.spec.containers[?(@.name=='server')].image}" }
$jmeterVersion = Try-CommandText { jmeter --version }
$javaVersion = Try-CommandText { java -version }

$manifest = [PSCustomObject]@{
    run_id = $RunId
    system_repository = $repoUrl
    system_commit = $repoCommit
    test_package_commit = $repoCommit
    kubernetes_context = $kubeContext
    minikube_profile = $minikubeProfile
    deployment_manifest = "deploy-all.yaml"
    frontend_image = $frontendImage
    reviewservice_image = $reviewImage
    scenario = Get-Prop $props "scenario" "shopping"
    start_utc = $startUtc
    end_utc = ""
    users = Get-Prop $props "users" "1"
    rampup_s = Get-Prop $props "rampup" "1"
    duration_s = Get-Prop $props "duration" "60"
    host = $TargetHost
    port = $Port
    checkout_percent = Get-Prop $props "checkout_percent" "30"
    currency_percent = Get-Prop $props "currency_percent" "20"
    review_write_percent = Get-Prop $props "review_write_percent" "10"
    target_service = ""
    fault_type = ""
    fault_parameters = ""
    operator = $env:USERNAME
    notes = ""
    jmeter_version = ($jmeterVersion -replace "`r?`n", " | ")
    java_version = ($javaVersion -replace "`r?`n", " | ")
}
$manifest | Export-Csv -NoTypeInformation -Encoding UTF8 $manifestPath

$jmeterArgs = @(
    "-n",
    "-t", $jmxPath,
    "-q", $configPath,
    "-Jjmeter_base_dir=$baseDir",
    "-Jrun_id=$RunId",
    "-Jprotocol=$Protocol",
    "-Jhost=$TargetHost",
    "-Jport=$Port",
    "-Jbase_path=$BasePath",
    "-l", $jtlPath,
    "-j", $jmeterLogPath,
    "-e",
    "-o", $reportDir
)

foreach ($prop in $JMeterProperty) {
    if ($prop -notmatch "^[A-Za-z0-9_.-]+=") {
        throw "Invalid JMeter property override '$prop'. Expected key=value."
    }
    $jmeterArgs += "-J$prop"
}

$jmeterExit = 0
$summaryExit = 0

try {
    & $jmeterCmd.Source @jmeterArgs
    $jmeterExit = $LASTEXITCODE
} finally {
    $endUtc = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
    & (Join-Path $baseDir "scripts/mark-event.ps1") -RunId $RunId -Event TEST_END -Details "JMeter exit code $jmeterExit"
    if (Test-Path $manifestPath) {
        $updated = Import-Csv $manifestPath
        $updated.end_utc = $endUtc
        $updated | Export-Csv -NoTypeInformation -Encoding UTF8 $manifestPath
    }
    "jmeter_exit_code=$jmeterExit" | Set-Content -Encoding UTF8 $exitCodePath
}

if (Test-Path $jtlPath) {
    & $pythonCmd.Source $summarizerPath --jtl $jtlPath --events $eventsPath --output-dir $jmeterDir
    $summaryExit = $LASTEXITCODE
    Add-Content -Encoding UTF8 $exitCodePath "summary_exit_code=$summaryExit"
} else {
    Write-Warning "JTL was not created; summary was skipped."
    $summaryExit = 1
    Add-Content -Encoding UTF8 $exitCodePath "summary_exit_code=1"
}

Set-Location $originalLocation

if ($jmeterExit -ne 0) {
    exit $jmeterExit
}
exit $summaryExit
