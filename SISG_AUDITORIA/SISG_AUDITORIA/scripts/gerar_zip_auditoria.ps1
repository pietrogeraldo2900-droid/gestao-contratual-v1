[CmdletBinding()]
param(
    [Parameter(Mandatory = $false)]
    [string]$Origem,

    [Parameter(Mandatory = $false)]
    [string]$Saida,

    [Parameter(Mandatory = $false)]
    [string]$PastaAuditoria
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-FullPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PathValue
    )

    if ([System.IO.Path]::IsPathRooted($PathValue)) {
        return [System.IO.Path]::GetFullPath($PathValue)
    }

    return [System.IO.Path]::GetFullPath((Join-Path -Path (Get-Location).Path -ChildPath $PathValue))
}

function Get-RelativePathSafe {
    param(
        [Parameter(Mandatory = $true)]
        [string]$BasePath,

        [Parameter(Mandatory = $true)]
        [string]$TargetPath
    )

    $baseFull = [System.IO.Path]::GetFullPath($BasePath)
    $targetFull = [System.IO.Path]::GetFullPath($TargetPath)

    if (-not $baseFull.EndsWith([System.IO.Path]::DirectorySeparatorChar)) {
        $baseFull = $baseFull + [System.IO.Path]::DirectorySeparatorChar
    }

    $baseUri = New-Object System.Uri($baseFull)
    $targetUri = New-Object System.Uri($targetFull)
    $relativeUri = $baseUri.MakeRelativeUri($targetUri)
    $relativePath = [System.Uri]::UnescapeDataString($relativeUri.ToString())
    return ($relativePath -replace '/', '\')
}

function Test-IsExcludedPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$RelativePath,

        [Parameter(Mandatory = $true)]
        [string[]]$ExcludedDirNames
    )

    $normalized = $RelativePath -replace '/', '\'
    $segments = $normalized.Split('\') | Where-Object { $_ -ne "" }

    foreach ($segment in $segments) {
        if ($ExcludedDirNames -contains $segment) {
            return $true
        }
    }

    if ($normalized -eq "data\runtime" -or $normalized.StartsWith("data\runtime\")) {
        return $true
    }

    return $false
}

$scriptDir = Split-Path -Path $MyInvocation.MyCommand.Path -Parent
$defaultSource = [System.IO.Path]::GetFullPath((Join-Path -Path $scriptDir -ChildPath ".."))

$sourcePathInput = if ([string]::IsNullOrWhiteSpace($Origem)) { $defaultSource } else { $Origem }
$sourcePath = Resolve-FullPath -PathValue $sourcePathInput

if (-not (Test-Path -LiteralPath $sourcePath -PathType Container)) {
    throw "Origem invalida: '$sourcePath'"
}

$outputPathInput = if ([string]::IsNullOrWhiteSpace($Saida)) {
    Join-Path -Path $sourcePath -ChildPath "SISG_AUDITORIA.zip"
} else {
    $Saida
}
$outputPath = Resolve-FullPath -PathValue $outputPathInput

$auditFolderInput = if ([string]::IsNullOrWhiteSpace($PastaAuditoria)) {
    Join-Path -Path $sourcePath -ChildPath "SISG_AUDITORIA"
} else {
    $PastaAuditoria
}
$auditFolderPath = Resolve-FullPath -PathValue $auditFolderInput

$outputDir = Split-Path -Path $outputPath -Parent
if (-not (Test-Path -LiteralPath $outputDir -PathType Container)) {
    New-Item -ItemType Directory -Path $outputDir -Force | Out-Null
}

if (Test-Path -LiteralPath $auditFolderPath) {
    Remove-Item -LiteralPath $auditFolderPath -Recurse -Force
}
New-Item -ItemType Directory -Path $auditFolderPath -Force | Out-Null

if (Test-Path -LiteralPath $outputPath) {
    Remove-Item -LiteralPath $outputPath -Force
}

$excludedDirNames = @(
    ".git",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".venv",
    "venv",
    "node_modules",
    "BASE_MESTRA",
    "saidas"
)

$excludedExtensions = @(".pyc", ".pyo", ".log", ".sqlite3")

$tempRoot = Join-Path -Path ([System.IO.Path]::GetTempPath()) -ChildPath ("sisg_auditoria_" + [System.Guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Path $tempRoot -Force | Out-Null

$copiedFiles = 0
$skippedFiles = 0

try {
    $files = Get-ChildItem -LiteralPath $sourcePath -Recurse -Force -File

    foreach ($file in $files) {
        $relativePath = Get-RelativePathSafe -BasePath $sourcePath -TargetPath $file.FullName
        $normalizedRelativePath = $relativePath -replace '/', '\'

        if (Test-IsExcludedPath -RelativePath $relativePath -ExcludedDirNames $excludedDirNames) {
            $skippedFiles++
            continue
        }

        # Exclui explicitamente data/drafts/* e data/drafts/web/*, mantendo apenas .gitkeep.
        if (($normalizedRelativePath -match '(?i)^data\\drafts(\\|$)') -and ($file.Name -ine ".gitkeep")) {
            $skippedFiles++
            continue
        }

        $extension = [System.IO.Path]::GetExtension($file.Name)
        if ($excludedExtensions -contains $extension.ToLowerInvariant()) {
            $skippedFiles++
            continue
        }

        # Evita incluir o ZIP de saida caso ele esteja dentro da origem.
        $fileFull = [System.IO.Path]::GetFullPath($file.FullName)
        if ($fileFull -eq $outputPath) {
            $skippedFiles++
            continue
        }

        $destinationFile = Join-Path -Path $tempRoot -ChildPath $relativePath
        $destinationDir = Split-Path -Path $destinationFile -Parent
        if (-not (Test-Path -LiteralPath $destinationDir -PathType Container)) {
            New-Item -ItemType Directory -Path $destinationDir -Force | Out-Null
        }

        Copy-Item -LiteralPath $file.FullName -Destination $destinationFile -Force
        $copiedFiles++
    }

    Copy-Item -Path (Join-Path -Path $tempRoot -ChildPath "*") -Destination $auditFolderPath -Recurse -Force
    Compress-Archive -Path (Join-Path -Path $tempRoot -ChildPath "*") -DestinationPath $outputPath -Force
}
finally {
    if (Test-Path -LiteralPath $tempRoot) {
        Remove-Item -LiteralPath $tempRoot -Recurse -Force
    }
}

Write-Host "ZIP de auditoria gerado com sucesso."
Write-Host "Origem: $sourcePath"
Write-Host "Pasta : $auditFolderPath"
Write-Host "Saida : $outputPath"
Write-Host "Arquivos copiados: $copiedFiles"
Write-Host "Arquivos ignorados: $skippedFiles"
