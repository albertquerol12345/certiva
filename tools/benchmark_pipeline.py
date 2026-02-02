from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Iterable, List

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from tests import generate_realistic_samples as tests_generate  # noqa: E402

from src import config, pipeline, utils  # noqa: E402
from src.llm_providers import DummyLLMProvider  # noqa: E402
from src.ocr_providers import DummyOCRProvider  # noqa: E402

DATASET_DIR = utils.BASE_DIR / "IN" / "benchmark_inputs"
BENCHMARK_PATH = utils.BASE_DIR / "OUT" / "BENCHMARK.txt"


def ensure_dataset(count: int, seed: int) -> List[Path]:
    DATASET_DIR.mkdir(parents=True, exist_ok=True)
    existing = sorted(DATASET_DIR.glob("*.pdf"))
    if len(existing) >= count:
        return existing[:count]
    tests_generate.generate_samples(
        count=count,
        out_tests=utils.BASE_DIR / "tests" / "benchmark_generated",
        out_in=DATASET_DIR,
        seed=seed,
        purge=True,
    )
    return sorted(DATASET_DIR.glob("*.pdf"))[:count]


def _run_with_concurrency(paths: Iterable[Path], tenant: str, concurrency: int) -> Dict[str, any]:
    started = time.perf_counter()
    doc_ids: List[str] = []
    ocr_override = DummyOCRProvider()
    llm_override = DummyLLMProvider()
    config.set_ocr_provider_override(ocr_override)
    config.set_llm_provider_override(llm_override)
    try:
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {executor.submit(pipeline.process_file, path, tenant, True): path for path in paths}
            for future in as_completed(futures):
                doc_id = future.result()
                if doc_id:
                    doc_ids.append(doc_id)
    finally:
        config.set_ocr_provider_override(None)
        config.set_llm_provider_override(None)
    elapsed = time.perf_counter() - started
    throughput = (len(doc_ids) / elapsed * 60) if elapsed else 0.0
    metrics = _collect_metrics(doc_ids)
    metrics.update(
        {
            "concurrency": concurrency,
            "docs": len(doc_ids),
            "elapsed_sec": round(elapsed, 2),
            "docs_per_min": round(throughput, 2),
        }
    )
    return metrics


def _collect_metrics(doc_ids: List[str]) -> Dict[str, float]:
    metrics = {
        "total_time_ms": [],
        "ocr_time_ms": [],
        "rules_time_ms": [],
        "llm_time_ms": [],
    }
    if not doc_ids:
        return metrics
    placeholders = ",".join("?" * len(doc_ids))
    with utils.get_connection() as conn:
        rows = conn.execute(
            f"SELECT doc_id,total_time_ms,ocr_time_ms,rules_time_ms,llm_time_ms FROM docs WHERE doc_id IN ({placeholders})",
            doc_ids,
        ).fetchall()
    for row in rows:
        for key in metrics:
            value = row[key]
            if value is not None:
                metrics[key].append(float(value))
    return metrics


def _percentile(values: List[float], pct: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    if len(values) == 1:
        return round(values[0], 2)
    k = (len(values) - 1) * pct
    f = int(k)
    c = min(f + 1, len(values) - 1)
    if f == c:
        return round(values[f], 2)
    return round(values[f] * (c - k) + values[c] * (k - f), 2)


def _format_metrics(result: Dict[str, any]) -> List[str]:
    lines = [
        f"Concurrency: {result['concurrency']}",
        f"  Documentos: {result['docs']}",
        f"  Tiempo total: {result['elapsed_sec']} s",
        f"  Throughput: {result['docs_per_min']} docs/min",
    ]
    for key in ("total_time_ms", "ocr_time_ms", "rules_time_ms", "llm_time_ms"):
        values = result.get(key) or []
        lines.append(
            f"  {key} p50/p95: {_percentile(values, 0.5)} / {_percentile(values, 0.95)} ms"
        )
    return lines


def build_report(results: List[Dict[str, any]], path: Path) -> str:
    lines = ["=== CERTIVA Benchmark ===", ""]
    for result in results:
        lines.extend(_format_metrics(result))
        lines.append("")
    report = "\n".join(lines).strip() + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report, encoding="utf-8")
    return report


def run_benchmark(count: int, tenant: str, seed: int, concurrency_levels: List[int]) -> List[Dict[str, any]]:
    pdfs = ensure_dataset(count, seed)
    if not pdfs:
        raise SystemExit("No hay PDFs para el benchmark")
    results: List[Dict[str, any]] = []
    for level in concurrency_levels:
        result = _run_with_concurrency(pdfs, tenant, max(1, level))
        results.append(result)
    report = build_report(results, BENCHMARK_PATH)
    print(report)
    print(f"[+] Benchmark guardado en {BENCHMARK_PATH}")
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark dummy del pipeline")
    parser.add_argument("--count", type=int, default=50, help="Número de documentos a procesar")
    parser.add_argument("--tenant", default=config.settings.default_tenant, help="Tenant objetivo")
    parser.add_argument("--seed", type=int, default=123, help="Semilla para generar PDFs")
    parser.add_argument(
        "--concurrency",
        nargs="*",
        type=int,
        default=[1, 2, 4],
        help="Lista de niveles de concurrencia (por defecto 1 2 4)",
    )
    args = parser.parse_args()
    if args.count <= 0:
        raise SystemExit("--count debe ser positivo")
    levels = args.concurrency or [1]
    run_benchmark(args.count, args.tenant, args.seed, levels)


if __name__ == "__main__":
    main()
