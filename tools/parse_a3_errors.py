"""
CLI para parsear logs de errores de importación A3 y generar un txt/json más legible.
Uso:
  python -m tools.parse_a3_errors --file a3_errors.log --format txt
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List

from src import a3_validator


def parse_text(text: str) -> List[a3_validator.Error]:
    return a3_validator.parse_a3_error_log(text)


def main() -> None:
    parser = argparse.ArgumentParser(description="Parsear logs de errores A3")
    parser.add_argument("--file", type=Path, help="Ruta del log (opcional, usa stdin si no se indica)")
    parser.add_argument("--format", choices=["txt", "json"], default="txt", help="Formato de salida")
    args = parser.parse_args()

    content = ""
    if args.file:
        content = args.file.read_text(encoding="utf-8", errors="ignore")
    else:
        content = sys.stdin.read()
    errors = parse_text(content)
    if args.format == "json":
        import json

        print(json.dumps(errors, ensure_ascii=False, indent=2))
    else:
        for line_no, field, msg in errors:
            prefix = f"Línea {line_no}" if line_no else "General"
            print(f"{prefix} · {field}: {msg}")


if __name__ == "__main__":
    main()
