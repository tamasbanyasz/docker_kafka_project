# Start the Kafka consumer (Go) - joins triplets from all 3 partitions
# Run from project root
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location (Join-Path $ProjectRoot "go")
go run .
