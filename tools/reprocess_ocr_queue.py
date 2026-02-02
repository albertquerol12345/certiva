from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import List

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from src import pipeline, utils  # noqa: E402


def drain_queue(limit: int, sleep_seconds: float) -> List[str]:
    rows = utils.iter_ocr_queue(limit)
    processed: List[str] = []
    for row in rows:
        doc_id = row["doc_id"]
        tenant = row["tenant"]
        path = Path(row["path"])
        if not path.exists():
            utils.mark_ocr_retry(doc_id, success=False, error="Archivo no existe")
            continue
        try:
            pipeline.process_file(path, tenant=tenant, force=True)
            utils.mark_ocr_retry(doc_id, success=True)
            processed.append(doc_id)
        except Exception as exc:  # pragma: no cover - surfaces in logs
            utils.mark_ocr_retry(doc_id, success=False, error=str(exc))
            time.sleep(sleep_seconds)
    return processed


def main() -> None:  # pragma: no cover
    parser = argparse.ArgumentParser(description="Reprocesa la cola de OCR fallidos")
    parser.add_argument("--limit", type=int, default=5, help="Máximo de items a reprocesar")
    parser.add_argument(
        "--sleep",
        type=float,
        default=2.0,
        help="Espera entre reintentos para no disparar límites de Azure",
    )
    args = parser.parse_args()
    processed = drain_queue(args.limit, args.sleep)
    print(f"Reprocesados: {len(processed)} -> {processed}")


if __name__ == "__main__":
    main()
