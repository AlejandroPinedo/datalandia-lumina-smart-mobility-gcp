$ErrorActionPreference = 'Stop'
$projectDir = Split-Path -Parent $PSScriptRoot
Push-Location -LiteralPath $projectDir
try {
    python -X utf8 P3/recuperar_web_pasajero.py
    if ($LASTEXITCODE -ne 0) { throw 'No se pudo consultar Gold. No se generan datos de respaldo.' }
} finally { Pop-Location }
