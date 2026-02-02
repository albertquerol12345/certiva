from __future__ import annotations

import argparse
import random
import sys
import time
from pathlib import Path
from typing import List, Optional, Tuple

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from src import azure_ocr_monitor, config  # noqa: E402
from src.ocr_providers import AzureOCRProvider  # noqa: E402


def probe(path: Path, count: int, seed: int, *, max_rps: Optional[float] = None, timeout: int = 120) -> Tuple[str, bool]:
    files: List[Path] = sorted(path.glob("*.pdf"))
    if not files:
        raise SystemExit(f"No se encontraron PDFs en {path}")
    random.Random(seed).shuffle(files)
    files = files[:count]
    provider = AzureOCRProvider(
        config.settings.azure_formrec_endpoint,
        config.settings.azure_formrec_key,
        config.settings.azure_formrec_model_id or "prebuilt-invoice",
        enable_cache=False,
        max_rps=max_rps or config.settings.azure_ocr_max_rps,
        read_timeout=timeout,
        max_attempts=4,
        max_concurrency=1,
    )
    start = time.perf_counter()
    for idx, file_path in enumerate(files, 1):
        try:
            provider.analyze_document(file_path, config.settings.default_tenant)
        except Exception as exc:
            print(f"[{idx}/{len(files)}] {file_path.name}: ERROR {exc}")
        else:
            print(f"[{idx}/{len(files)}] {file_path.name}: OK")
    elapsed = time.perf_counter() - start
    stats = azure_ocr_monitor.snapshot(reset=True)
    status_counts = stats.get("status_counts", {})
    errors = {
        status: count
        for status, count in status_counts.items()
        if status not in {200, 304} and count
    }
    report_lines = [
        "=== Azure OCR Probe ===",
        f"Directorio: {path}",
        f"Documentos procesados: {len(files)}",
        f"Tiempo total: {elapsed:.2f}s",
        f"RPS efectivo: {len(files) / elapsed:.2f}" if elapsed else "RPS efectivo: n/a",
        f"Status counts: {status_counts}",
        f"Reintentos: {stats.get('retry_total',0)}",
        f"Cache hits: {stats.get('cache_hits',0)} / {stats.get('cache_hits',0)+stats.get('cache_misses',0)}",
        f"Latencia OCR p50/p95 (ms): "
        f"{_percentile(stats.get('latency_samples', []), 0.5)} / {_percentile(stats.get('latency_samples', []), 0.95)}",
    ]
    ts = time.strftime("%Y%m%d_%H%M%S")
    target = BASE_DIR / "OUT" / f"AZURE_PROBE_{ts}.txt"
    output = "\n".join(report_lines)
    target.write_text(output, encoding="utf-8")
    return output, not bool(errors)


def _percentile(values: List[float], pct: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    k = (len(values) - 1) * pct
    f = int(k)
    c = min(f + 1, len(values) - 1)
    if f == c:
        return round(values[f], 2)
    return round(values[f] * (c - k) + values[c] * (k - f), 2)


def main() -> None:  # pragma: no cover
    parser = argparse.ArgumentParser(description="Probe Azure OCR throughput")
    parser.add_argument("--path", type=Path, required=True, help="Carpeta con PDFs")
    parser.add_argument("--count", "-n", type=int, default=5, help="Número de PDFs a probar")
    parser.add_argument("--seed", type=int, default=123, help="Semilla para aleatorizar los PDFs")
    parser.add_argument("--rps", type=float, default=None, help="Max RPS durante el probe")
    parser.add_argument("--timeout", type=int, default=120, help="Timeout de lectura por documento")
    args = parser.parse_args()
    report, ok = probe(args.path, args.count, args.seed, max_rps=args.rps, timeout=args.timeout)
    print(report)
    print("[+] Informe guardado en OUT/AZURE_PROBE_<ts>.txt")
    if not ok:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
