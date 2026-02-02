#!/usr/bin/env bash
set -euo pipefail

# Always run from repo root
cd "$(dirname "$0")"

python_cmd=${PYTHON:-python3}

if [ ! -d ".venv" ]; then
  echo "[*] Creando entorno virtual (.venv)..."
  "$python_cmd" -m venv .venv
fi

echo "[*] Activando entorno virtual..."
source .venv/bin/activate

echo "[*] Instalando dependencias..."
pip install --upgrade pip >/dev/null
pip install -r requirements.txt >/dev/null

echo "[*] Lanzando CERTIVA en http://localhost:8000 ..."
exec uvicorn src.webapp:app --host 0.0.0.0 --port 8000
