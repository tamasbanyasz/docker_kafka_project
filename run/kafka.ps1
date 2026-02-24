# Start Kafka via Docker Compose
Write-Host "=== PROCESS: Kafka (Docker) ===" -ForegroundColor Cyan
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot
docker compose up -d
Write-Host "Kafka started. Bootstrap: 127.0.0.1:9092"
