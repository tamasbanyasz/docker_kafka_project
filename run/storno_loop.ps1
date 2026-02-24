# Periodically storno random houses - test producer + consumer + storno together
Write-Host "=== PROCESS: Storno loop (periodic) ===" -ForegroundColor Cyan
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

if (Test-Path "venv\Scripts\Activate.ps1") {
    & "venv\Scripts\Activate.ps1"
}

python python/storno_loop.py @args
