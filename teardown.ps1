param([switch]$Apply)
$ErrorActionPreference = 'Stop'
Push-Location $PSScriptRoot
try {
    $pythonCommand = if (Test-Path -LiteralPath '.venv\Scripts\python.exe') {
        Join-Path $PSScriptRoot '.venv\Scripts\python.exe'
    } else { 'python' }
    $arguments = @('-m', 'payments', 'teardown')
    if ($Apply) { $arguments += '--apply' }
    & $pythonCommand @arguments
    if ($LASTEXITCODE -ne 0) { throw "Teardown falhou (codigo $LASTEXITCODE)" }
} finally { Pop-Location }
