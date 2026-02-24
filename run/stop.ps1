# Stop producer and consumer processes. Run from project root.
Write-Host "=== PROCESS: Stop ===" -ForegroundColor Cyan
$procs = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
    $cmd = if ($_.CommandLine) { $_.CommandLine } else { "" }
    ($_.Name -match "consumer|kafka-consumer" -and $cmd -match "docker_kafka|kafka") -or
    ($_.Name -match "python" -and $cmd -match "producer_partitioned")
}
foreach ($p in $procs) {
    Write-Host "Stopping: $($p.Name) PID $($p.ProcessId)"
    Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
}
if ($procs) { Write-Host "Done." } else { Write-Host "No producer/consumer processes found." }
Write-Host "Then run: .\run\reset.ps1"
