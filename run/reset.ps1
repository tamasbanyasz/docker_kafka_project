# Reset: clear output folder and Kafka topics. Run from project root.
param([switch]$KeepKafka)

Write-Host "=== PROCESS: Reset ===" -ForegroundColor Cyan
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

$OutputDir = "output"
if (Test-Path $OutputDir) {
    $removed = 0
    $failed = 0
    Get-ChildItem $OutputDir -File | ForEach-Object {
        try {
            Remove-Item $_.FullName -Force -ErrorAction Stop
            $removed++
        } catch {
            Write-Host "In use (stop consumer/producer first): $($_.Name)"
            $failed++
        }
    }
    if ($removed -gt 0) { Write-Host "Output: $removed file(s) removed." }
    if ($failed -gt 0) { Write-Host "Output: $failed file(s) could not be removed." }
} else {
    New-Item -ItemType Directory -Path $OutputDir | Out-Null
    Write-Host "Output folder created."
}

if (-not $KeepKafka) {
    $Topics = @("teszt-partitioned", "house-storno")
    $ContainerName = (docker ps --format "{{.Names}}" 2>$null | Where-Object { $_ -match "kafka" }) | Select-Object -First 1
    if ($ContainerName) {
        foreach ($Topic in $Topics) {
            docker exec $ContainerName /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic $Topic 2>$null
            if ($LASTEXITCODE -eq 0) {
                Write-Host "Kafka topic deleted: $Topic"
            } else {
                Write-Host "Topic $Topic may not exist (ok)"
            }
        }
    } else {
        Write-Host "Kafka container not running. Start with: .\run\kafka.ps1"
    }
}

Write-Host "Reset complete. Start producer first, then consumer."
