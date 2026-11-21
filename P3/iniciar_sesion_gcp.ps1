# Ejecutar personalmente en PowerShell. No compartir las credenciales generadas.
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$env:CLOUDSDK_CONFIG = Join-Path $repoRoot '.runtime/gcloud'
$env:CLOUDSDK_CORE_DISABLE_FILE_LOGGING = 'true'
New-Item -ItemType Directory -Force $env:CLOUDSDK_CONFIG | Out-Null
$sdkCandidates = @(
    'C:/Program Files (x86)/Google/Cloud SDK/google-cloud-sdk/bin/gcloud.cmd',
    "$env:LOCALAPPDATA/Google/Cloud SDK/google-cloud-sdk/bin/gcloud.cmd"
)
$gcloudBin = $sdkCandidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
if (-not $gcloudBin) { throw 'No se encontró Google Cloud SDK' }
& $gcloudBin auth login
if ($LASTEXITCODE -ne 0) { throw 'No se completó el acceso de gcloud' }
& $gcloudBin config set project bigdata-herles-pinedo
if ($LASTEXITCODE -ne 0) { throw 'No se pudo seleccionar el proyecto' }
& $gcloudBin auth application-default login
if ($LASTEXITCODE -ne 0) { throw 'No se completó el acceso de los clientes Python' }
& $gcloudBin auth list --filter=status:ACTIVE --format='value(account)'
Write-Host 'Acceso local preparado. Avisa a Codex para verificar permisos del proyecto.'
