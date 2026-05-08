#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

echo "RCSA Local Flow Lab"
echo "Carpeta: $ROOT_DIR"
echo ""

if ! command -v node >/dev/null 2>&1; then
  echo "Error: Node.js 20+ no esta instalado o no esta en PATH."
  echo "Instala Node.js y vuelve a ejecutar este script."
  exit 1
fi

echo "Abre http://127.0.0.1:4400"
echo "Presiona Ctrl+C para detener este servidor."
echo ""

node server.js
