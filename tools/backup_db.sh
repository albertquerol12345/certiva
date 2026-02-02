#!/usr/bin/env bash
# Copia de seguridad rápida de la base SQLite de CERTIVA (db/docs.sqlite).
set -euo pipefail
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="$BASE_DIR/db/docs.sqlite"
DEST_DIR="$BASE_DIR/OUT/backups"
timestamp="$(date +"%Y%m%d_%H%M%S")"
mkdir -p "$DEST_DIR"
if [ ! -f "$SRC" ]; then
  echo "No existe $SRC" >&2
  exit 1
fi
cp "$SRC" "$DEST_DIR/docs_${timestamp}.sqlite"
echo "Backup creado en $DEST_DIR/docs_${timestamp}.sqlite"
