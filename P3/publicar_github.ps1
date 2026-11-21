# Ejecutar personalmente si Codex no puede acceder a la red de GitHub.
# Autentica una cuenta con permisos de escritura; no comparte tokens ni usa force-push.
$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
$env:GH_CONFIG_DIR = Join-Path $repoRoot '.runtime/gh'
New-Item -ItemType Directory -Force $env:GH_CONFIG_DIR | Out-Null
gh auth login --hostname github.com --git-protocol https --web
if ($LASTEXITCODE -ne 0) { throw 'No se completó el acceso de GitHub' }
$originUrl = git -C $repoRoot remote get-url origin
if ($originUrl -ne 'https://github.com/AlejandroPinedo/datalandia-lumina-smart-mobility-gcp.git') {
    throw 'El remoto no coincide con el repositorio del proyecto; revisar antes de publicar.'
}
$pendingChanges = git -C $repoRoot status --porcelain
if ($pendingChanges) { throw 'Hay cambios sin commit. Pide a Codex que prepare la entrega final antes del push.' }
git -C $repoRoot -c credential.helper= -c 'credential.helper=!gh auth git-credential' push origin HEAD:main
if ($LASTEXITCODE -ne 0) { throw 'Push rechazado o sin acceso. No se fuerza la publicación.' }
Write-Host 'Publicación completada. Comparando el commit local con GitHub...'
$localCommit = git -C $repoRoot rev-parse HEAD
$remoteLine = git -C $repoRoot -c credential.helper= -c 'credential.helper=!gh auth git-credential' ls-remote origin refs/heads/main
if ($LASTEXITCODE -ne 0 -or -not $remoteLine.StartsWith($localCommit)) {
    throw 'No se pudo verificar la coincidencia del commit remoto.'
}
Write-Host "Commit confirmado en GitHub: $localCommit"
