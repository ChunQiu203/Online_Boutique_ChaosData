[CmdletBinding()]
param(
    [string]$Protocol = "http",
    [Alias("Host")]
    [string]$TargetHost = "127.0.0.1",
    [int]$Port = 8080,
    [string]$BasePath = "",
    [string]$ProductId = "OLJCESPC7Z",
    [switch]$ScaleLoadGenerator
)

$ErrorActionPreference = "Continue"
$failures = 0

function Fail-Check {
    param([string]$Message)
    Write-Error $Message
    $script:failures++
}

function Run-Step {
    param(
        [string]$Name,
        [scriptblock]$Action,
        [switch]$WarnOnly
    )
    Write-Host "== $Name =="
    try {
        & $Action
        if ($LASTEXITCODE -ne $null -and $LASTEXITCODE -ne 0) {
            throw "exit code $LASTEXITCODE"
        }
    } catch {
        if ($WarnOnly) {
            Write-Warning "$Name failed: $($_.Exception.Message)"
        } else {
            Fail-Check "$Name failed: $($_.Exception.Message)"
        }
    }
}

function Get-KubectlValue {
    param([string[]]$Arguments)
    try {
        $value = & kubectl @Arguments 2>$null
        if ($LASTEXITCODE -ne 0) {
            return ""
        }
        return (($value | Out-String).Trim())
    } catch {
        return ""
    }
}

if (-not (Get-Command kubectl -ErrorAction SilentlyContinue)) {
    Fail-Check "kubectl is not available in PATH."
    exit 1
}

Run-Step "kubectl config current-context" { kubectl config current-context }
Run-Step "kubectl get nodes" { kubectl get nodes }
Run-Step "kubectl get deployment frontend reviewservice" { kubectl get deployment frontend reviewservice }
Run-Step "kubectl get service frontend frontend-external reviewservice" { kubectl get service frontend frontend-external reviewservice }
Run-Step "kubectl get pods" { kubectl get pods }

$frontendReady = Get-KubectlValue @("get", "deployment", "frontend", "-o", "jsonpath={.status.readyReplicas}/{.status.replicas}")
if ($frontendReady -notmatch "^[1-9][0-9]*/[1-9][0-9]*$") {
    Fail-Check "frontend Deployment is not Ready. Current ready/desired: '$frontendReady'"
} else {
    Write-Host "frontend Ready: $frontendReady"
}

$reviewReady = Get-KubectlValue @("get", "deployment", "reviewservice", "-o", "jsonpath={.status.readyReplicas}/{.status.replicas}")
if ($reviewReady -notmatch "^[1-9][0-9]*/[1-9][0-9]*$") {
    Fail-Check "reviewservice Deployment is not Ready. Current ready/desired: '$reviewReady'"
} else {
    Write-Host "reviewservice Ready: $reviewReady"
}

$frontendImage = Get-KubectlValue @("get", "deployment", "frontend", "-o", "jsonpath={.spec.template.spec.containers[?(@.name=='server')].image}")
$reviewImage = Get-KubectlValue @("get", "deployment", "reviewservice", "-o", "jsonpath={.spec.template.spec.containers[?(@.name=='server')].image}")

Write-Host "frontend image: $frontendImage"
Write-Host "reviewservice image: $reviewImage"

if ($frontendImage -notmatch "frontend:local") {
    Fail-Check "frontend image is not frontend:local. Current image: '$frontendImage'"
}
if ($reviewImage -notmatch "reviewservice:local") {
    Fail-Check "reviewservice image is not reviewservice:local. Current image: '$reviewImage'"
}

$reviewAddr = Get-KubectlValue @("get", "deployment", "frontend", "-o", "jsonpath={.spec.template.spec.containers[?(@.name=='server')].env[?(@.name=='REVIEW_SERVICE_ADDR')].value}")
Write-Host "frontend REVIEW_SERVICE_ADDR: $reviewAddr"
if ($reviewAddr -ne "reviewservice:8080") {
    Fail-Check "frontend REVIEW_SERVICE_ADDR is not reviewservice:8080. Current value: '$reviewAddr'"
}

$loadgenReplicas = Get-KubectlValue @("get", "deployment", "loadgenerator", "-o", "jsonpath={.spec.replicas}")
if ($loadgenReplicas -ne "") {
    Write-Host "loadgenerator replicas: $loadgenReplicas"
    if ([int]$loadgenReplicas -ne 0) {
        Write-Warning "正式 JMeter 压测前请执行: kubectl scale deployment/loadgenerator --replicas=0"
        if ($ScaleLoadGenerator) {
            Run-Step "Scale loadgenerator to 0" { kubectl scale deployment/loadgenerator --replicas=0 }
        }
    }
} else {
    Write-Warning "loadgenerator Deployment not found or not readable."
}

$baseUrl = "${Protocol}://${TargetHost}:${Port}${BasePath}"
Write-Host "Base URL: $baseUrl"

try {
    $home = Invoke-WebRequest -Uri "$baseUrl/" -UseBasicParsing -TimeoutSec 10
    if ($home.StatusCode -ne 200) {
        Fail-Check "GET / returned HTTP $($home.StatusCode)"
    } elseif ($home.Content -notmatch "Hot Products") {
        Fail-Check "GET / did not contain stable text 'Hot Products'."
    } else {
        Write-Host "GET / OK"
    }
} catch {
    Fail-Check "GET / failed at $baseUrl/: $($_.Exception.Message)"
}

try {
    $health = Invoke-WebRequest -Uri "$baseUrl/_healthz" -UseBasicParsing -TimeoutSec 10
    if ($health.StatusCode -ne 200 -or $health.Content.Trim() -ne "ok") {
        Fail-Check "GET /_healthz did not return 200 ok."
    } else {
        Write-Host "GET /_healthz OK"
    }
} catch {
    Fail-Check "GET /_healthz failed at $baseUrl/_healthz: $($_.Exception.Message)"
}

try {
    $product = Invoke-WebRequest -Uri "$baseUrl/product/$ProductId" -UseBasicParsing -TimeoutSec 15
    if ($product.StatusCode -ne 200) {
        Fail-Check "GET /product/$ProductId returned HTTP $($product.StatusCode)"
    }
    if ($product.Content -notmatch "Customer Reviews") {
        Fail-Check "Product detail page did not contain 'Customer Reviews'."
    }
    if ($product.Content -notmatch "Write a Review") {
        Fail-Check "Product detail page did not contain 'Write a Review'."
    }
    if ($product.Content -notmatch "Add To Cart") {
        Fail-Check "Product detail page did not contain 'Add To Cart'."
    }
    if ($product.Content -match "Something has failed|Uh, oh!") {
        Fail-Check "Product detail page appears to be the error template."
    }
    if ($failures -eq 0) {
        Write-Host "GET /product/$ProductId review area OK"
    }
} catch {
    Fail-Check "GET /product/$ProductId failed at $baseUrl/product/${ProductId}: $($_.Exception.Message)"
}

if ($failures -gt 0) {
    Write-Error "$failures environment check(s) failed."
    exit 1
}

Write-Host "Environment check passed."
exit 0
