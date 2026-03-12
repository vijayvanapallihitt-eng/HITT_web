param(
    [switch]$IncludeOptional
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$PythonExe = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$Requirements = Join-Path $RepoRoot "requirements.txt"
$OptionalRequirements = Join-Path $RepoRoot "requirements-optional.txt"

if (-not (Test-Path $PythonExe)) {
    py -3.12 -m venv (Join-Path $RepoRoot ".venv")
}

& $PythonExe -m pip install --upgrade pip
& $PythonExe -m pip install -r $Requirements

if ($IncludeOptional) {
    & $PythonExe -m pip install -r $OptionalRequirements
}

& $PythonExe -m pip check
Write-Host "Virtualenv is ready: $PythonExe"
