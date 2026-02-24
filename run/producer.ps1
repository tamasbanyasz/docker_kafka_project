# Start the 3-partition Kafka producer (Python)
Write-Host "=== PROCESS: Producer (Python) ===" -ForegroundColor Cyan
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

if (Test-Path "venv\Scripts\Activate.ps1") {
    & "venv\Scripts\Activate.ps1"
}
python python/producer_partitioned.py
