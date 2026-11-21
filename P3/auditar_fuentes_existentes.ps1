$ErrorActionPreference = 'Stop'
$projectDir = Split-Path -Parent $PSScriptRoot
Push-Location -LiteralPath $projectDir
try {
    python -X utf8 P3/auditar_fuentes_existentes.py
    if ($LASTEXITCODE -ne 0) { throw 'No se completo el inventario. No se modifica ningun recurso.' }
} finally { Pop-Location }
