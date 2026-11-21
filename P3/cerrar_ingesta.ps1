$ErrorActionPreference = 'Stop'
$projectDir = Split-Path -Parent $PSScriptRoot
Push-Location -LiteralPath $projectDir
try {
    python -X utf8 P3/cerrar_ingesta.py --resume
    if ($LASTEXITCODE -ne 0) { throw 'No se completo la evidencia de ingesta. Se conservan las fuentes historicas.' }
} finally { Pop-Location }
