# Send storno request: .\run\storno.ps1 <house_id> [-Partition 1|2|3]
param(
    [Parameter(Mandatory=$true)]
    [int]$Id,
    [ValidateSet(0,1,2,3)]
    [int]$Partition = 0
)

Write-Host "=== PROCESS: Storno (one-shot) ===" -ForegroundColor Cyan
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

if (Test-Path "venv\Scripts\Activate.ps1") {
    & "venv\Scripts\Activate.ps1"
}

$args = @($Id)
if ($Partition -gt 0) {
    $args += "--partition", $Partition
}
python python/storno_producer.py @args
