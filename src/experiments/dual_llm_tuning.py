from __future__ import annotations

import logging
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

from .. import pipeline, utils
from ..batch_writer import build_batch_outputs
from ..config import settings

logger = logging.getLogger(__name__)

SUGGESTION_FILENAME = "LLM_TUNING_SUGGESTIONS.txt"
TARGET_MIN_RATIO = 0.05
TARGET_MAX_RATIO = 0.30


def _validate_dual_setup() -> None:
    issues: List[str] = []
    if settings.ocr_provider_type != "azure":
        issues.append(f"OCR_PROVIDER_TYPE debe ser 'azure' (actual: {settings.ocr_provider_type!r})")
    if settings.llm_provider_type != "openai":
        issues.append(f"LLM_PROVIDER_TYPE debe ser 'openai' (actual: {settings.llm_provider_type!r})")
    if settings.llm_strategy != "dual_cascade":
        issues.append(f"LLM_STRATEGY debe ser 'dual_cascade' (actual: {settings.llm_strategy!r})")
    if issues:
        message = " · ".join(issues)
        logger.error("Configuración incompatible para el experimento dual LLM: %s", message)
        raise RuntimeError(message)


def _process_batch(batch_dir: Path, tenant: str) -> List[str]:
    if not batch_dir.exists():
        raise FileNotFoundError(f"No existe la carpeta {batch_dir}")
    files = sorted(p for p in batch_dir.glob("*.pdf") if p.is_file())
    if not files:
        raise RuntimeError(f"No hay PDFs en {batch_dir}")
    doc_ids: List[str] = []
    logger.info("Procesando %d PDFs para el experimento dual LLM...", len(files))
    for pdf in files:
        doc_id = pipeline.process_file(pdf, tenant=tenant, force=True)
        if doc_id:
            doc_ids.append(doc_id)
            logger.info("  ✓ %s → %s", pdf.name, doc_id[:8])
    if not doc_ids:
        raise RuntimeError("No se pudo procesar ningún documento en el lote.")
    return list(dict.fromkeys(doc_ids))


def _fetch_doc_rows(doc_ids: Iterable[str]) -> List[Dict[str, Any]]:
    doc_ids = list(doc_ids)
    if not doc_ids:
        return []
    placeholders = ",".join("?" for _ in doc_ids)
    with utils.get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT doc_id, filename, tenant, status, issues,
                   llm_provider, llm_model_used, ocr_provider
            FROM docs
            WHERE doc_id IN ({placeholders})
            """,
            doc_ids,
        ).fetchall()
    return [dict(row) for row in rows]


def _read_gross(doc_id: str) -> float | None:
    json_path = utils.BASE_DIR / "OUT" / "json" / f"{doc_id}.json"
    if not json_path.exists():
        return None
    try:
        payload = utils.read_json(json_path)
    except Exception:  # pragma: no cover - defensivo
        return None
    totals = payload.get("totals") or {}
    gross = totals.get("gross")
    try:
        return float(gross)
    except (TypeError, ValueError):
        return None


def _collect_doc_stats(doc_ids: List[str]) -> Dict[str, Any]:
    rows = _fetch_doc_rows(doc_ids)
    llm_counts: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()
    mini_gross: List[float] = []
    premium_gross: List[float] = []
    for row in rows:
        model = (row.get("llm_model_used") or "unknown").lower()
        llm_counts.update([model])
        status = (row.get("status") or "UNKNOWN").upper()
        status_counts.update([status])
        gross_value = _read_gross(row["doc_id"])
        if gross_value is None:
            continue
        if model == "mini":
            mini_gross.append(gross_value)
        elif model == "premium":
            premium_gross.append(gross_value)
    total_docs = len(rows)
    posted_docs = status_counts.get("POSTED", 0)
    premium_docs = llm_counts.get("premium", 0)
    mini_docs = llm_counts.get("mini", 0)
    premium_ratio = (premium_docs / total_docs) if total_docs else 0.0
    return {
        "doc_ids": doc_ids,
        "total_docs": total_docs,
        "posted_docs": posted_docs,
        "incident_docs": total_docs - posted_docs,
        "mini_docs": mini_docs,
        "premium_docs": premium_docs,
        "premium_ratio": premium_ratio,
        "llm_counts": dict(llm_counts),
        "status_counts": dict(status_counts),
        "mini_gross_values": mini_gross,
        "premium_gross_values": premium_gross,
        "current_threshold": settings.llm_premium_threshold_gross,
    }


def _percentile(values: List[float], pct: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    if len(values) == 1:
        return values[0]
    k = (len(values) - 1) * pct
    f = int(k)
    c = min(f + 1, len(values) - 1)
    if f == c:
        return values[f]
    d0 = values[f] * (c - k)
    d1 = values[c] * (k - f)
    return d0 + d1


def evaluate_threshold_policy(current_threshold: float, premium_ratio: float, premium_values: List[float]) -> Dict[str, Any]:
    """Devuelve una sugerencia de umbral basada en la ratio premium obtenida."""
    result = {
        "current_threshold": current_threshold,
        "suggested_threshold": current_threshold,
        "reason": "sin_datos_premium" if not premium_values else "ratio_en_objetivo",
        "premium_ratio": premium_ratio,
    }
    if not premium_values:
        return result
    if premium_ratio > TARGET_MAX_RATIO:
        suggested = _percentile(premium_values, 0.70)
        result.update(
            {
                "suggested_threshold": round(suggested, 2),
                "reason": f"premium_ratio {premium_ratio:.2f} > {TARGET_MAX_RATIO:.2f} → subir umbral hacia percentil 70",
            }
        )
    elif premium_ratio < TARGET_MIN_RATIO:
        suggested = _percentile(premium_values, 0.30)
        result.update(
            {
                "suggested_threshold": round(suggested, 2),
                "reason": f"premium_ratio {premium_ratio:.2f} < {TARGET_MIN_RATIO:.2f} → bajar umbral hacia percentil 30",
            }
        )
    else:
        result["reason"] = f"premium_ratio {premium_ratio:.2f} dentro del rango objetivo [{TARGET_MIN_RATIO:.2f}, {TARGET_MAX_RATIO:.2f}]"
    return result


def _format_values(values: List[float]) -> str:
    if not values:
        return "-"
    return ", ".join(f"{value:.2f}" for value in sorted(values))


def _write_suggestions(batch_dir: Path, stats: Dict[str, Any], suggestion: Dict[str, Any]) -> Path:
    lines = [
        f"Dual LLM tuning suggestions for batch {batch_dir.name}",
        "",
        f"Total docs: {stats['total_docs']}",
        f"Docs mini: {stats['mini_docs']}",
        f"Docs premium: {stats['premium_docs']}",
        f"Premium ratio: {stats['premium_ratio']:.2%}",
        f"A3 (POSTED): {stats['posted_docs']}",
        f"Incidencias: {stats['incident_docs']}",
        "",
        f"LLM model breakdown: {stats['llm_counts']}",
        f"Status breakdown: {stats['status_counts']}",
        "",
        f"Mini gross values: {_format_values(stats['mini_gross_values'])}",
        f"Premium gross values: {_format_values(stats['premium_gross_values'])}",
        "",
        f"Current LLM_PREMIUM_THRESHOLD_GROSS: {stats['current_threshold']:.2f}",
        f"Suggested threshold: {suggestion['suggested_threshold']:.2f}",
        f"Reason: {suggestion['reason']}",
    ]
    suggestion_path = batch_dir / SUGGESTION_FILENAME
    suggestion_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Sugerencias escritas en %s", suggestion_path)
    return suggestion_path


def run_dual_llm_experiment(batch_dir: Path, tenant: str) -> Dict[str, Any]:
    """
    Ejecuta un lote con la cascada dual LLM y genera sugerencias de umbral.
    Devuelve un dict con batch_dir, stats y suggestion.
    """
    utils.configure_logging()
    _validate_dual_setup()
    doc_ids = _process_batch(batch_dir, tenant)
    batch_name = f"dual_llm_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    batch_output = build_batch_outputs(doc_ids, tenant, batch_name)
    stats = _collect_doc_stats(doc_ids)
    suggestion = evaluate_threshold_policy(
        stats["current_threshold"],
        stats["premium_ratio"],
        stats["premium_gross_values"],
    )
    suggestion_path = _write_suggestions(batch_output, stats, suggestion)
    print("\n--- Dual LLM experiment summary ---")
    print(f"Lote: {batch_output}")
    print(f"Docs totales: {stats['total_docs']} · mini: {stats['mini_docs']} · premium: {stats['premium_docs']}")
    print(f"A3 vs incidencias → {stats['posted_docs']} / {stats['incident_docs']}")
    print(f"Premium ratio: {stats['premium_ratio']:.2%}")
    print(f"Sugerencia de umbral: {suggestion['suggested_threshold']:.2f} (razón: {suggestion['reason']})")
    print(f"Sugerencias detalladas: {suggestion_path}")
    return {
        "batch_dir": batch_output,
        "suggestion_path": suggestion_path,
        "stats": stats,
        "suggestion": suggestion,
    }


__all__ = ["run_dual_llm_experiment", "evaluate_threshold_policy"]
