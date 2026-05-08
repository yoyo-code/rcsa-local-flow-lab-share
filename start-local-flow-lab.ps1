$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

Write-Host "RCSA Local Flow Lab"
Write-Host "Carpeta: $root"
Write-Host ""
Write-Host "Abriendo http://127.0.0.1:4400"
Write-Host "Presiona Ctrl+C para detener este servidor."
Write-Host ""

if (-not (Get-Command node -ErrorAction SilentlyContinue)) {
  throw "Node.js 20+ no esta instalado o no esta en PATH."
}

Start-Process "http://127.0.0.1:4400"
node server.js
