# Start the Kafka consumer (Go) - joins triplets from all 3 partitions
Write-Host "=== PROCESS: Consumer (Go) ===" -ForegroundColor Cyan
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

# Consumer Group mód: group törlése indulás előtt (friss partition assignment, nincs stale member)
$cfgPath = Join-Path $ProjectRoot "config.yaml"
if (Test-Path $cfgPath) {
    $content = Get-Content $cfgPath -Raw
    if ($content -match "consumer_group:\s*true") {
        $GroupId = "my-service"
        if ($content -match "group_id:\s*[\`"']?([\w\-]+)") { $GroupId = $Matches[1].Trim() }
        $Container = docker ps --format "{{.Names}}" 2>$null | Where-Object { $_ -match "kafka" } | Select-Object -First 1
        if ($Container) {
            Write-Host "Consumer Group reset: deleting group '$GroupId' (ensures all 3 partitions)" -ForegroundColor Yellow
            docker exec $Container /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group $GroupId --delete 2>$null
            Start-Sleep -Seconds 3
        }
    }
}

Set-Location (Join-Path $ProjectRoot "go")
go run .
