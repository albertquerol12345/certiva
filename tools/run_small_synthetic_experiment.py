from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:  # ensure repo root is importable
    sys.path.insert(0, str(BASE_DIR))

try:  # pragma: no cover - defensive import
    from tests import augment as tests_augment
    from tests import generate_realistic_samples as tests_generate
except ImportError as exc:  # pragma: no cover - fail fast
    raise SystemExit(
        "No se pudieron importar tests.augment/generate_realistic_samples. "
        "Ejecuta este script desde la raíz del repo y asegúrate de que tests/__init__.py existe."
    ) from exc

from src import config, utils
from src.experiments.dual_llm_tuning import evaluate_threshold_policy
from src.launcher import process_folder_batch

try:  # pragma: no cover - optional dependency for probe
    from tools import azure_probe
except Exception:  # pragma: no cover
    azure_probe = None
DEFAULT_TENANT = config.settings.default_tenant
CLEAN_DIR = BASE_DIR / "IN" / "lote_sintetico_grande"
DIRTY_DIR = BASE_DIR / "IN" / "lote_sintetico_grande_dirty"
TESTS_DIR = BASE_DIR / "tests" / "realistic_big"
SUMMARY_COMPARISON = BASE_DIR / "OUT" / "ANALISIS_COMPARATIVO_SINTETICO.txt"
DIRTY_SEED_OFFSET = 713


@dataclass
class BatchOutcome:
    label: str
    batch_dir: Path
    doc_ids: List[str]
    resumen: Dict[str, str]
    metrics: Dict[str, Any]
    suggestion: Dict[str, Any]


def _check_required_env() -> None:
    required = {
        "OCR_PROVIDER_TYPE": config.settings.ocr_provider_type == "azure",
        "AZURE_FORMREC_ENDPOINT": bool(config.settings.azure_formrec_endpoint),
        "AZURE_FORMREC_KEY": bool(config.settings.azure_formrec_key),
        "LLM_PROVIDER_TYPE": config.settings.llm_provider_type == "openai",
        "LLM_STRATEGY": getattr(config.settings, "llm_strategy", "") == "dual_cascade",
        "OPENAI_API_KEY": bool(config.settings.openai_api_key),
        "OPENAI_MODEL_MINI": bool(config.settings.openai_model_mini or config.settings.openai_model),
        "OPENAI_MODEL_PREMIUM": bool(config.settings.openai_model_premium or config.settings.openai_model),
    }
    missing = [key for key, ok in required.items() if not ok]
    if missing:
        msg = "Faltan las siguientes variables/configuración en .env: " + ", ".join(missing)
        raise SystemExit(msg)


def ensure_clean_lot(count: int = 50, seed: int = 123, *, purge: bool = False) -> Path:
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    existing = list(CLEAN_DIR.glob("*.pdf"))
    if len(existing) >= count and not purge:
        return CLEAN_DIR
    print(f"[+] Generando {count} facturas sintéticas limpias…")
    tests_generate.generate_samples(
        count=count,
        out_tests=TESTS_DIR,
        out_in=CLEAN_DIR,
        seed=seed,
        purge=True,
    )
    return CLEAN_DIR


def ensure_dirty_lot(
    *,
    count: int = 50,
    seed: int = 456,
    source_dir: Optional[Path] = None,
    purge: bool = False,
) -> Path:
    source = source_dir or CLEAN_DIR
    if not source.exists():
        raise SystemExit("No existe el lote limpio para generar la versión dirty.")
    DIRTY_DIR.mkdir(parents=True, exist_ok=True)
    existing = list(DIRTY_DIR.glob("*.pdf"))
    if len(existing) >= count and not purge:
        return DIRTY_DIR
    print(f"[+] Generando lote dirty ({count} PDFs) a partir de {source}…")
    tests_augment.augment_folder(
        source,
        DIRTY_DIR,
        seed=seed,
        purge=True,
        limit=count,
    )
    return DIRTY_DIR


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
    interpolated = values[f] * (c - k) + values[c] * (k - f)
    return round(interpolated, 2)


def collect_doc_metrics(doc_ids: List[str]) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {
        "doc_ids": list(doc_ids),
        "doc_count": len(doc_ids),
        "mini_docs": 0,
        "premium_docs": 0,
        "premium_gross_values": [],
        "total_time_ms": [],
        "ocr_time_ms": [],
        "rules_time_ms": [],
        "llm_time_ms": [],
        "confidence": [],
        "posted_docs": 0,
        "incident_docs": 0,
        "premium_ratio": 0.0,
        "current_threshold": config.settings.llm_premium_threshold_gross,
    }
    if not doc_ids:
        return metrics
    placeholders = ",".join("?" * len(doc_ids))
    query = (
        "SELECT doc_id,total_time_ms,ocr_time_ms,rules_time_ms,llm_time_ms,global_conf,"
        "llm_model_used,status FROM docs WHERE doc_id IN (" + placeholders + ")"
    )
    with utils.get_connection() as conn:
        rows = conn.execute(query, doc_ids).fetchall()
    posted = 0
    for row in rows:
        for key in ("total_time_ms", "ocr_time_ms", "rules_time_ms", "llm_time_ms"):
            value = row[key]
            if value is not None:
                metrics[key].append(float(value))
        if row["global_conf"] is not None:
            metrics["confidence"].append(float(row["global_conf"]))
        status = (row["status"] or "").upper()
        if status == "POSTED":
            posted += 1
        model = (row["llm_model_used"] or "mini").lower()
        if model == "premium":
            metrics["premium_docs"] += 1
            json_path = utils.BASE_DIR / "OUT" / "json" / f"{row['doc_id']}.json"
            if json_path.exists():
                try:
                    payload = utils.read_json(json_path)
                    gross = float(((payload.get("totals") or {}).get("gross")) or 0.0)
                except Exception:  # pragma: no cover - lectura defensiva
                    continue
                metrics["premium_gross_values"].append(gross)
        else:
            metrics["mini_docs"] += 1
    metrics["posted_docs"] = posted
    metrics["incident_docs"] = max(0, metrics["doc_count"] - posted)
    if metrics["doc_count"]:
        metrics["premium_ratio"] = metrics["premium_docs"] / metrics["doc_count"]
    return metrics


def summarize_metrics(metrics: Dict[str, Any]) -> Dict[str, float]:
    summary: Dict[str, float] = {}
    for key in ("total_time_ms", "ocr_time_ms", "rules_time_ms", "llm_time_ms"):
        values = metrics.get(key) or []
        summary[f"{key}_p50"] = _percentile(values, 0.5)
        summary[f"{key}_p95"] = _percentile(values, 0.95)
    conf_values = metrics.get("confidence") or []
    summary["confidence_p50"] = _percentile(conf_values, 0.5)
    summary["confidence_p95"] = _percentile(conf_values, 0.95)
    return summary


def parse_summary(resumen_path: Path) -> Dict[str, str]:
    data: Dict[str, str] = {}
    issues: Dict[str, int] = {}
    for line in resumen_path.read_text(encoding="utf-8").splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.rstrip()
        value = value.strip()
        if key.startswith("  -"):
            issue = key[3:]
            try:
                issues[issue] = int(value)
            except ValueError:
                continue
        else:
            data[key.strip()] = value
    if issues:
        data["Issues frecuentes"] = "; ".join(f"{k}={v}" for k, v in list(issues.items())[:5])
    return data


def _pct(value: int, total: int) -> float:
    if not total:
        return 0.0
    return round(100.0 * value / total, 2)


def _run_probe_for_folder(label: str, folder: Path) -> None:
    if config.settings.ocr_provider_type != "azure":
        return
    if azure_probe is None:
        raise SystemExit("No se pudo importar tools.azure_probe para ejecutar el probe previo.")
    pdfs = list(folder.glob("*.pdf"))
    if not pdfs:
        return
    sample = max(1, min(5, len(pdfs)))
    print(f"[+] azure_probe ({label}) con {sample} PDFs para validar Azure OCR…")
    report, ok = azure_probe.probe(
        folder,
        count=sample,
        seed=123,
        max_rps=config.settings.azure_ocr_max_rps,
        timeout=config.settings.azure_ocr_read_timeout_sec,
    )
    if not ok:
        print(report)
        raise SystemExit("azure_probe detectó 429/timeout. Ajusta AZURE_MAX_RPS y reintenta.")


def _format_outcome(outcome: BatchOutcome) -> tuple[List[str], Dict[str, float]]:
    metrics = outcome.metrics
    stats = summarize_metrics(metrics)
    doc_count = metrics.get("doc_count", 0)
    posted = metrics.get("posted_docs", 0)
    incidents = metrics.get("incident_docs", 0)
    mini_docs = metrics.get("mini_docs", 0)
    premium_docs = metrics.get("premium_docs", 0)
    suggestion = outcome.suggestion
    issues = outcome.resumen.get("Issues frecuentes", "-")
    lines = [
        f"Lote {outcome.label}: {outcome.batch_dir}",
        f"  Documentos: {doc_count}",
        f"  OK (POSTED): {posted} ({_pct(posted, doc_count)}%)",
        f"  Incidencias: {incidents} ({_pct(incidents, doc_count)}%)",
        f"  mini_docs: {mini_docs} ({_pct(mini_docs, doc_count)}%)",
        f"  premium_docs: {premium_docs} ({round(100*metrics.get('premium_ratio',0.0),2)}%)",
        f"  Issues frecuentes: {issues}",
        f"  Latencias totales ms p50/p95: {stats['total_time_ms_p50']} / {stats['total_time_ms_p95']}",
        f"  OCR ms p50/p95: {stats['ocr_time_ms_p50']} / {stats['ocr_time_ms_p95']}",
        f"  Rules ms p50/p95: {stats['rules_time_ms_p50']} / {stats['rules_time_ms_p95']}",
        f"  LLM ms p50/p95: {stats['llm_time_ms_p50']} / {stats['llm_time_ms_p95']}",
        f"  Confianza global p50/p95: {stats['confidence_p50']} / {stats['confidence_p95']}",
        f"  Threshold actual: {suggestion['current_threshold']:.2f}",
        f"  Threshold sugerido: {suggestion['suggested_threshold']:.2f} ({suggestion['reason']})",
    ]
    return lines, stats


def _format_comparison(outcomes: List[BatchOutcome], stats_map: Dict[str, Dict[str, float]]) -> List[str]:
    if len(outcomes) < 2:
        return []
    first, second = outcomes[:2]
    first_metrics = first.metrics
    second_metrics = second.metrics
    lines = ["Comparativa (primer vs segundo lote):"]
    lines.append(
        f"  Δ% OK: {round(_pct(second_metrics.get('posted_docs',0), second_metrics.get('doc_count',0)) - _pct(first_metrics.get('posted_docs',0), first_metrics.get('doc_count',0)), 2)} pp"
    )
    lines.append(
        f"  Δ premium_ratio: {round(100*second_metrics.get('premium_ratio',0.0) - 100*first_metrics.get('premium_ratio',0.0), 2)} pp"
    )
    lines.append(
        f"  Δ confianza p50: {round(stats_map[second.label]['confidence_p50'] - stats_map[first.label]['confidence_p50'], 2)}"
    )
    lines.append(f"  Issues {first.label}: {first.resumen.get('Issues frecuentes', '-')}")
    lines.append(f"  Issues {second.label}: {second.resumen.get('Issues frecuentes', '-')}")
    return lines


def build_report(outcomes: List[BatchOutcome], report_path: Optional[Path] = None) -> str:
    if not outcomes:
        return "Sin lotes para reportar."
    lines: List[str] = ["=== CERTIVA · Experimento Sintético Pequeño ===", ""]
    stats_map: Dict[str, Dict[str, float]] = {}
    for outcome in outcomes:
        section, stats = _format_outcome(outcome)
        stats_map[outcome.label] = stats
        lines.extend(section)
        lines.append("")
    lines.extend(_format_comparison(outcomes, stats_map))
    report = "\n".join(lines).strip() + "\n"
    if report_path:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(report, encoding="utf-8")
    return report


def process_and_analyze(label: str, input_dir: Path, tenant: str) -> BatchOutcome:
    print(f"[+] Procesando lote {label} ({input_dir})…")
    _run_probe_for_folder(label, input_dir)
    batch_dir, doc_ids = process_folder_batch(input_dir, tenant, force_dummy=False, quiet=True, skip_probe=True)
    resumen = parse_summary(batch_dir / "RESUMEN.txt")
    metrics = collect_doc_metrics(doc_ids)
    suggestion = evaluate_threshold_policy(
        metrics["current_threshold"],
        metrics.get("premium_ratio", 0.0),
        metrics.get("premium_gross_values", []),
    )
    return BatchOutcome(label=label, batch_dir=batch_dir, doc_ids=doc_ids, resumen=resumen, metrics=metrics, suggestion=suggestion)


def run_experiment(
    *,
    count: int = 50,
    seed: int = 123,
    tenant: str = DEFAULT_TENANT,
    process_clean: bool = True,
    process_dirty: bool = True,
    generator_only: bool = False,
    out_report: Optional[Path] = None,
) -> List[BatchOutcome]:
    if not generator_only:
        _check_required_env()
    outcomes: List[BatchOutcome] = []
    clean_dir: Optional[Path] = None
    dirty_dir: Optional[Path] = None
    if process_clean or process_dirty or generator_only:
        clean_dir = ensure_clean_lot(count=count, seed=seed)
    if process_dirty or generator_only:
        dirty_dir = ensure_dirty_lot(count=count, seed=seed + DIRTY_SEED_OFFSET, source_dir=clean_dir)
    if generator_only:
        print("[+] Generación completada (modo generator-only). No se procesaron los lotes.")
        return outcomes
    if process_clean and clean_dir is not None:
        outcomes.append(process_and_analyze("limpio", clean_dir, tenant))
    if process_dirty and (dirty_dir or DIRTY_DIR.exists()):
        outcomes.append(process_and_analyze("dirty", dirty_dir or DIRTY_DIR, tenant))
    if not outcomes:
        print("No se seleccionó ningún lote para procesar.")
        return outcomes
    report_path = out_report or SUMMARY_COMPARISON
    report = build_report(outcomes, report_path)
    print(report)
    print(f"[+] Informe guardado en {report_path}")
    return outcomes


def main() -> None:  # pragma: no cover - CLI
    parser = argparse.ArgumentParser(description="Ejecuta lote sintético pequeño (limpio vs dirty)")
    parser.add_argument("--count", type=int, default=50, help="Nº de facturas por lote")
    parser.add_argument("--seed", type=int, default=123, help="Semilla base para los generadores")
    parser.add_argument(
        "--generator-only",
        action="store_true",
        help="Sólo generar los PDFs (limpio y dirty) sin lanzar OCR/LLM",
    )
    parser.add_argument("--process-clean", action="store_true", help="Incluir el lote limpio en el procesamiento")
    parser.add_argument("--process-dirty", action="store_true", help="Incluir el lote dirty en el procesamiento")
    parser.add_argument("--tenant", default=DEFAULT_TENANT, help="Tenant destino para el procesamiento")
    parser.add_argument(
        "--out-report",
        type=Path,
        default=SUMMARY_COMPARISON,
        help="Ruta del informe comparativo (default: OUT/ANALISIS_...)",
    )
    args = parser.parse_args()
    if args.count <= 0:
        raise SystemExit("--count debe ser mayor que cero")
    selected = args.process_clean or args.process_dirty
    process_clean = args.process_clean if selected else True
    process_dirty = args.process_dirty if selected else True
    run_experiment(
        count=args.count,
        seed=args.seed,
        tenant=args.tenant,
        process_clean=process_clean,
        process_dirty=process_dirty,
        generator_only=args.generator_only,
        out_report=args.out_report,
    )


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as exc:  # pragma: no cover - guardado
        print(f"Error en el experimento: {exc}", file=sys.stderr)
        raise
