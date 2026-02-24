# Wrapper: run storno from project root (works when cwd is go/)
$ProjectRoot = Split-Path -Parent $PSScriptRoot
& "$ProjectRoot\run\storno.ps1" @args
