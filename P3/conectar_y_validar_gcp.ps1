# Flujo fijo de P3: un inicio de sesion y validacion automatica.
# Ejecutar personalmente en PowerShell con red. No compartir credenciales.
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$runtimeDir = Join-Path $repoRoot '.runtime'
$env:CLOUDSDK_CONFIG = Join-Path $runtimeDir 'gcloud'
$env:CLOUDSDK_CORE_DISABLE_FILE_LOGGING = 'true'
$env:GCP_PROJECT_ID = 'bigdata-herles-pinedo'
$env:GOOGLE_APPLICATION_CREDENTIALS = Join-Path $env:CLOUDSDK_CONFIG 'application_default_credentials.json'
New-Item -ItemType Directory -Force $env:CLOUDSDK_CONFIG | Out-Null
$sdkCandidates = @(
    'C:/Program Files (x86)/Google/Cloud SDK/google-cloud-sdk/bin/gcloud.cmd',
    "$env:LOCALAPPDATA/Google/Cloud SDK/google-cloud-sdk/bin/gcloud.cmd"
)
$gcloudBin = $sdkCandidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
if (-not $gcloudBin) { throw 'Google Cloud SDK no esta instalado en las rutas conocidas.' }

Write-Host 'Selecciona en el navegador la cuenta con acceso a bigdata-herles-pinedo.'
Write-Host 'Se prepararan gcloud y los clientes Python con el mismo inicio de sesion.'
& $gcloudBin auth login --update-adc
if ($LASTEXITCODE -ne 0) { throw 'No se completo el inicio de sesion. No se ejecuto la demo.' }
& $gcloudBin config set project bigdata-herles-pinedo --quiet
if ($LASTEXITCODE -ne 0) { throw 'No se pudo seleccionar el proyecto.' }
if (-not (Test-Path -LiteralPath $env:GOOGLE_APPLICATION_CREDENTIALS)) {
    throw 'No se generaron las credenciales locales para Python.'
}

$taskPackages = Join-Path $runtimeDir 'python-gcp'
New-Item -ItemType Directory -Force $taskPackages | Out-Null
$env:PYTHONPATH = "$taskPackages;$repoRoot/Avance1_P1_Lumina_Ingesta_Storage"
Write-Host 'Instalando el cliente BigQuery dentro de .runtime (excluido de Git)...'
python -m pip install --target $taskPackages --upgrade 'google-cloud-bigquery>=3.25.0' --disable-pip-version-check
if ($LASTEXITCODE -ne 0) { throw 'No se pudo instalar el cliente BigQuery.' }

Push-Location $repoRoot
try {
    Write-Host 'Ejecutando CSV/IoT -> Bronze -> Silver -> Gold -> API. No se modifican las tablas historicas.'
    python -X utf8 P3/demo_e2e.py --backend bigquery
    if ($LASTEXITCODE -ne 0) {
        throw 'La validacion no termino. Informa el error; nunca compartas tokens ni archivos de credenciales.'
    }
    Write-Host ''
    Write-Host 'LISTO: demo BigQuery validada. Avisa a Codex para revisar P3/evidencias/bigquery_latest.json.'
} finally { Pop-Location }
