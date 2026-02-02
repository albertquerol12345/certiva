from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path
from typing import Dict, List

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

try:  # pragma: no cover - defensive
    from tools import run_small_synthetic_experiment as synth_helpers
except ImportError as exc:  # pragma: no cover
    raise SystemExit("No se pudo importar tools.run_small_synthetic_experiment") from exc

from src import config, utils  # noqa: E402
from src.launcher import process_folder_batch  # noqa: E402

CLEAN_DIR = BASE_DIR / "IN" / "lote_sintetico_grande"
DIRTY_DIR = BASE_DIR / "IN" / "lote_sintetico_grande_dirty"
REPORT_DIR = BASE_DIR / "OUT"


def ensure_inputs(kind: str, count: int, seed: int) -> Path:
    if kind == "clean":
        synth_helpers.ensure_clean_lot(count=count, seed=seed)
        return CLEAN_DIR
    if kind == "dirty":
        synth_helpers.ensure_clean_lot(count=count, seed=seed)
        synth_helpers.ensure_dirty_lot(count=count, seed=seed + 997)
        return DIRTY_DIR
    raise ValueError(f"Entrada desconocida: {kind}")


def _collect_doc_stats(doc_ids: List[str]) -> Dict[str, float | int | List[float]]:
    stats: Dict[str, float | int | List[float]] = {
        "doc_ids": list(doc_ids),
        "doc_count": len(doc_ids),
        "posted": 0,
        "incident": 0,
        "mini_docs": 0,
        "premium_docs": 0,
        "tokens_in": 0.0,
        "tokens_out": 0.0,
        "cost_eur": 0.0,
        "total_time_ms": [],
        "ocr_time_ms": [],
        "llm_time_ms": [],
    }
    if not doc_ids:
        return stats
    placeholders = ",".join("?" * len(doc_ids))
    query = (
        "SELECT doc_id,status,llm_model_used,total_time_ms,ocr_time_ms,llm_time_ms,"
        "llm_tokens_in,llm_tokens_out,llm_cost_eur FROM docs WHERE doc_id IN (" + placeholders + ")"
    )
    with utils.get_connection() as conn:
        rows = conn.execute(query, doc_ids).fetchall()
    for row in rows:
        status = (row["status"] or "").upper()
        if status == "POSTED":
            stats["posted"] += 1
        else:
            stats["incident"] += 1
        model = (row["llm_model_used"] or "unknown").lower()
        if model == "premium":
            stats["premium_docs"] += 1
        elif model == "mini":
            stats["mini_docs"] += 1
        if row["llm_tokens_in"]:
            stats["tokens_in"] += float(row["llm_tokens_in"])
        if row["llm_tokens_out"]:
            stats["tokens_out"] += float(row["llm_tokens_out"])
        if row["llm_cost_eur"]:
            stats["cost_eur"] += float(row["llm_cost_eur"])
        for key in ("total_time_ms", "ocr_time_ms", "llm_time_ms"):
            val = row[key]
            if val is not None:
                stats[key].append(float(val))
    return stats


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


def _format_block(label: str, stats: Dict[str, float | int | List[float]], summary: Dict[str, float]) -> List[str]:
    doc_count = stats.get("doc_count", 0)
    posted = stats.get("posted", 0)
    premium_docs = stats.get("premium_docs", 0)
    auto_ratio = (posted / doc_count * 100) if doc_count else 0.0
    premium_ratio = (premium_docs / doc_count * 100) if doc_count else 0.0
    lines = [
        f"Input {label}:",
        f"  Documentos: {doc_count} · Auto-post: {auto_ratio:.1f}%",
        f"  Premium docs: {premium_docs} ({premium_ratio:.1f}%)",
        f"  Tokens IN/OUT: {round(stats['tokens_in'],2)} / {round(stats['tokens_out'],2)}",
        f"  Coste estimado LLM (€): {round(stats['cost_eur'],4)}",
        f"  Latencias totales ms p50/p95: {summary['total_time_ms_p50']} / {summary['total_time_ms_p95']}",
        f"  OCR ms p50/p95: {summary['ocr_time_ms_p50']} / {summary['ocr_time_ms_p95']}",
        f"  LLM ms p50/p95: {summary['llm_time_ms_p50']} / {summary['llm_time_ms_p95']}",
    ]
    return lines


def process_input(label: str, directory: Path, tenant: str) -> Dict[str, any]:
    batch_dir, doc_ids = process_folder_batch(directory, tenant, force_dummy=False, quiet=True)
    metrics = synth_helpers.collect_doc_metrics(doc_ids)
    summary = synth_helpers.summarize_metrics(metrics)
    raw_stats = _collect_doc_stats(doc_ids)
    return {
        "label": label,
        "batch_dir": batch_dir,
        "doc_ids": doc_ids,
        "metrics": metrics,
        "summary": summary,
        "raw": raw_stats,
    }


def build_report(results: List[Dict[str, any]], path: Path) -> str:
    lines = ["=== CERTIVA Benchmark ===", ""]
    for result in results:
        lines.extend(_format_block(result["label"], result["raw"], result["summary"]))
        lines.append("")
    if len(results) >= 2:
        first, second = results[:2]
        lines.append("Comparativa clean vs dirty:")
        auto_first = (first["raw"]["posted"] / first["raw"]["doc_count"] * 100) if first["raw"]["doc_count"] else 0
        auto_second = (second["raw"]["posted"] / second["raw"]["doc_count"] * 100) if second["raw"]["doc_count"] else 0
        prem_first = (first["raw"]["premium_docs"] / first["raw"]["doc_count"] * 100) if first["raw"]["doc_count"] else 0
        prem_second = (second["raw"]["premium_docs"] / second["raw"]["doc_count"] * 100) if second["raw"]["doc_count"] else 0
        cost_delta = round(second["raw"]["cost_eur"] - first["raw"]["cost_eur"], 4)
        lines.append(f"  Δ% auto-post: {round(auto_second - auto_first, 2)} pp")
        lines.append(f"  Δ% premium: {round(prem_second - prem_first, 2)} pp")
        lines.append(f"  Δ coste (€): {cost_delta}")
        lines.append(
            f"  Δ total_time p50: {round(second['summary']['total_time_ms_p50'] - first['summary']['total_time_ms_p50'], 2)} ms"
        )
    report = "\n".join(lines).strip() + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report, encoding="utf-8")
    return report


def run_benchmark(inputs: List[str], tenant: str, count: int, seed: int) -> str:
    results: List[Dict[str, any]] = []
    for label in inputs:
        directory = ensure_inputs(label, count, seed)
        results.append(process_input(label, directory, tenant))
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d_%H%M%S")
    report_path = REPORT_DIR / f"BENCHMARK_{ts}.txt"
    return build_report(results, report_path)


def main() -> None:  # pragma: no cover - CLI thin wrapper
    parser = argparse.ArgumentParser(description="Benchmark clean vs dirty")
    parser.add_argument("--count", type=int, default=50, help="Número de documentos por lote")
    parser.add_argument("--seed", type=int, default=123, help="Semilla para la generación")
    parser.add_argument("--tenant", default=config.settings.default_tenant)
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Reservado (pipeline concurrency)",
    )
    parser.add_argument(
        "--input",
        action="append",
        choices=["clean", "dirty"],
        help="Entradas a procesar (por defecto ambas)",
    )
    parser.add_argument("--out-report", type=Path, help="Sobrescribir ruta del informe")
    args = parser.parse_args()
    inputs = args.input or ["clean", "dirty"]
    report = run_benchmark(inputs, args.tenant, args.count, args.seed)
    if args.out_report:
        Path(args.out_report).write_text(report, encoding="utf-8")
    print(report)
    print("[+] Benchmark guardado")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as exc:  # pragma: no cover
        print(f"Error en benchmark: {exc}", file=sys.stderr)
        raise
