# Ejecutar personalmente desde PowerShell con acceso de red y cuenta autorizada.
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$env:CLOUDSDK_CONFIG = Join-Path $repoRoot '.runtime/gcloud'
$env:GOOGLE_APPLICATION_CREDENTIALS = Join-Path $env:CLOUDSDK_CONFIG 'application_default_credentials.json'
$env:CLOUDSDK_CORE_DISABLE_FILE_LOGGING = 'true'
if (-not (Test-Path -LiteralPath $env:GOOGLE_APPLICATION_CREDENTIALS)) {
    throw 'Primero ejecuta P3/iniciar_sesion_gcp.ps1 con una cuenta con acceso al proyecto.'
}
$taskPackages = Join-Path $repoRoot '.runtime/python-gcp'
New-Item -ItemType Directory -Force $taskPackages | Out-Null
$env:PYTHONPATH = "$taskPackages;$repoRoot/Avance1_P1_Lumina_Ingesta_Storage"
python -m pip install --target $taskPackages --upgrade google-cloud-bigquery
if ($LASTEXITCODE -ne 0) { throw 'No se pudieron instalar las dependencias locales de GCP' }
$env:GCP_PROJECT_ID = 'bigdata-herles-pinedo'
Push-Location $repoRoot
try {
    python -X utf8 P3/demo_e2e.py --backend bigquery --serve
    if ($LASTEXITCODE -ne 0) { throw 'La demo no completó sus verificaciones. Conserva el mensaje de error, sin credenciales.' }
} finally { Pop-Location }
