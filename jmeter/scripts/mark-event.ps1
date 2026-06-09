[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$RunId,

    [Parameter(Mandatory = $true)]
    [ValidateSet("TEST_START", "WARMUP_END", "FAULT_START", "FAULT_END", "TEST_END", "NOTE")]
    [string]$Event,

    [string]$Details = ""
)

$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$baseDir = Resolve-Path (Join-Path $scriptDir "..")
$experimentDir = Join-Path $baseDir ("experiments/" + $RunId)
$eventsPath = Join-Path $experimentDir "events.csv"

New-Item -ItemType Directory -Force $experimentDir | Out-Null

if (-not (Test-Path $eventsPath)) {
    "run_id,event,timestamp_utc,details" | Set-Content -Encoding UTF8 $eventsPath
}

function ConvertTo-CsvField {
    param([string]$Value)
    if ($null -eq $Value) {
        $Value = ""
    }
    '"' + ($Value -replace '"', '""') + '"'
}

$timestamp = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
$row = @(
    ConvertTo-CsvField $RunId
    ConvertTo-CsvField $Event
    ConvertTo-CsvField $timestamp
    ConvertTo-CsvField $Details
) -join ","

Add-Content -Encoding UTF8 $eventsPath $row
Write-Host "Recorded event $Event for $RunId at $timestamp"
