# Consulta fija de solo lectura al Gold ya existente. No genera ni carga datos.
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$env:CLOUDSDK_CONFIG = Join-Path $repoRoot '.runtime/gcloud'
$env:GOOGLE_APPLICATION_CREDENTIALS = Join-Path $env:CLOUDSDK_CONFIG 'application_default_credentials.json'
$env:GCP_PROJECT_ID = 'bigdata-herles-pinedo'
if (-not (Test-Path -LiteralPath $env:GOOGLE_APPLICATION_CREDENTIALS)) {
    throw 'No se encontro la sesion local de Google Cloud. No se generan datos alternativos.'
}
$taskPackages = Join-Path $repoRoot '.runtime/python-gcp'
$env:PYTHONPATH = "$taskPackages;$repoRoot/Avance1_P1_Lumina_Ingesta_Storage"
python -c 'from google.cloud import bigquery'
if ($LASTEXITCODE -ne 0) {
    New-Item -ItemType Directory -Force $taskPackages | Out-Null
    python -m pip install --target $taskPackages 'google-cloud-bigquery>=3.25.0' --disable-pip-version-check
    if ($LASTEXITCODE -ne 0) { throw 'No se pudo preparar el cliente BigQuery.' }
}
Push-Location $repoRoot
try {
    python -X utf8 P3/usar_datos_existentes.py
    if ($LASTEXITCODE -ne 0) { throw 'La consulta no termino. Codex puede revisar .runtime/consulta_existente_error.json.' }
    Write-Host 'LISTO: se consultaron las tablas existentes; no se generaron ni cargaron viajes.'
} finally { Pop-Location }
