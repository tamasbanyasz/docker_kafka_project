# Houses.jsonl -> houses_formatted.json (struktúrált, megnyitható editorban)
# Használat: .\run\show_houses.ps1 [-First N] [-Last N]
# Példa: .\run\show_houses.ps1 -First 10   (első 10 -> output/houses_formatted.json)
#        .\run\show_houses.ps1 -First 0    (összes)
param(
    [int]$First = 0,    # 0 = mind; -First 10 = első 10
    [int]$Last = 0      # 0 = nincs; -Last 5 = utolsó 5
)

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$HousesFile = Join-Path $ProjectRoot "output\houses.jsonl"
$OutFile = Join-Path $ProjectRoot "output\houses_formatted.json"

if (-not (Test-Path $HousesFile)) {
    Write-Host "File not found: $HousesFile" -ForegroundColor Red
    exit 1
}

$lines = Get-Content $HousesFile -Encoding UTF8
$total = $lines.Count
if ($Last -gt 0) {
    $lines = $lines[-$Last..-1]
} elseif ($First -gt 0) {
    $lines = $lines | Select-Object -First $First
}

$arr = [System.Collections.ArrayList]::new()
foreach ($line in $lines) {
    if ($line.Trim() -eq "") { continue }
    try {
        $obj = $line | ConvertFrom-Json
        [void]$arr.Add($obj)
    } catch { }
}

$arr | ConvertTo-Json -Depth 10 | Set-Content $OutFile -Encoding UTF8
Write-Host "Written to $OutFile ($($arr.Count) records)" -ForegroundColor Green
