# Consumer Group mód: először törli a group-ot (friss partition assignment), majd indítja a consumert
# Használat: .\run\consumer_group.ps1
# Előfeltétel: config.yaml -> consumer_group: true
Write-Host "=== PROCESS: Consumer (Go) - Consumer Group mode with reset ===" -ForegroundColor Cyan
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

# Group ID a config-ból (egyszerű parse)
$GroupId = "my-service"
if (Test-Path "config.yaml") {
    $line = Get-Content "config.yaml" | Select-String "group_id:"
    if ($line -match "group_id:\s*[\`"']?([\w\-]+)") {
        $GroupId = $Matches[1].Trim()
    }
}

# Kafka container keresése
$Container = docker ps --format "{{.Names}}" 2>$null | Where-Object { $_ -match "kafka" } | Select-Object -First 1
if ($Container) {
    Write-Host "Resetting consumer group: $GroupId (clears stale members for full partition assignment)" -ForegroundColor Yellow
    docker exec $Container /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group $GroupId --delete 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Group deleted. Waiting 3s for coordinator..." -ForegroundColor Green
        Start-Sleep -Seconds 3
    }
} else {
    Write-Host "Kafka container not found - skipping group reset" -ForegroundColor Yellow
}

Set-Location (Join-Path $ProjectRoot "go")
go run .
