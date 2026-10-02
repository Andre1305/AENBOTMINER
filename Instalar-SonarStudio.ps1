param([string]$Version = '')
$ErrorActionPreference = 'Stop'
$projectRoot = $PSScriptRoot
$manifest = Get-Content -LiteralPath (Join-Path $projectRoot 'sonarstudio-versions.json') -Raw | ConvertFrom-Json
if (-not $Version) {
    $available = @($manifest.versions | Where-Object { Test-Path -LiteralPath (Join-Path $projectRoot $_.folder) })
    if (-not $available.Count) { throw 'Nenhuma pasta de aplicativo encontrada.' }
    $Version = $available[-1].version
}
$selected = $manifest.versions | Where-Object { $_.version -eq $Version }
if (-not $selected) { throw "Versão desconhecida: $Version" }
if (-not (Get-Command pixi -ErrorAction SilentlyContinue)) { throw 'Instale Pixi pelo site oficial https://pixi.sh ou winget install prefix-dev.pixi e execute novamente.' }
$runtimeProject = Join-Path $projectRoot 'work\PINGMapper-main'
New-Item -ItemType Directory -Path $runtimeProject -Force | Out-Null
Copy-Item -LiteralPath (Join-Path $projectRoot 'sonarstudio-runtime.toml') -Destination (Join-Path $runtimeProject 'pixi.toml') -Force
pixi install --manifest-path (Join-Path $runtimeProject 'pixi.toml')
if ($LASTEXITCODE -ne 0) { throw 'Falha ao instalar dependências; consulte o erro do Pixi acima.' }
$python = Join-Path $runtimeProject '.pixi\envs\default\python.exe'
& $python (Join-Path $projectRoot 'tools\prepare_assets.py') --folder (Join-Path $projectRoot $selected.folder)
if ($LASTEXITCODE -ne 0) { throw 'Falha ao preparar os modelos/fontes.' }
$appFolder = Join-Path $projectRoot $selected.folder
$csc = Join-Path $env:WINDIR 'Microsoft.NET\Framework64\v4.0.30319\csc.exe'
& $csc /nologo /target:winexe /reference:System.Windows.Forms.dll ("/out:"+(Join-Path $appFolder 'SonarStudio.exe')) (Join-Path $appFolder 'Launcher.cs')
if ($LASTEXITCODE -ne 0) { throw 'Falha ao compilar o launcher.' }
New-Item -ItemType Directory -Path (Join-Path $projectRoot 'work\tmp') -Force | Out-Null
Write-Output "Instalado. Abra $appFolder\SonarStudio.exe"
