$ErrorActionPreference = 'Stop'
$projectDir = Split-Path -Parent $PSScriptRoot
Push-Location -LiteralPath $projectDir
try {
    python -X utf8 P3/completar_validacion_fondo.py --resume
    if ($LASTEXITCODE -ne 0) { throw 'Validacion incompleta. Las fuentes historicas se conservan.' }
} finally { Pop-Location }
