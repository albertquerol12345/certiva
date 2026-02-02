"""Helpers to empaquetar lotes A3 (asientos, incidencias, resumen)."""
from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from . import utils, llm_debug, provider_health, azure_ocr_monitor
from .config import settings
from .exporter import A3_CSV_COLUMNS
from .a3_validator import validate_a3_csv

INCIDENCIAS_COLUMNS = [
    "doc_id",
    "filename",
    "supplier",
    "invoice_number",
    "gross",
    "issues",
    "status",
    "suggested_account",
    "suggested_iva_type",
]


def _fetch_docs(doc_ids: List[str]) -> List[Tuple]:
    if not doc_ids:
        return []
    placeholders = ",".join("?" * len(doc_ids))
    with utils.get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT doc_id, filename, tenant, status, issues, doc_type, ocr_provider, llm_provider,
                   llm_model_used, ocr_time_ms, rules_time_ms, llm_time_ms, total_time_ms, created_at, updated_at,
                   global_conf, llm_tokens_in, llm_tokens_out, llm_cost_eur, page_count
            FROM docs
            WHERE doc_id IN ({placeholders})
            """,
            doc_ids,
        ).fetchall()
    return rows


def _fetch_review_entry(doc_id: str) -> Optional[Tuple[str, str]]:
    with utils.get_connection() as conn:
        row = conn.execute("SELECT reason, suggested FROM review_queue WHERE doc_id = ?", (doc_id,)).fetchone()
        if not row:
            return None
        return row["reason"], row["suggested"]


def _read_normalized(doc_id: str) -> Optional[dict]:
    json_path = utils.BASE_DIR / "OUT" / "json" / f"{doc_id}.json"
    if not json_path.exists():
        return None
    return utils.read_json(json_path)


def _merge_doc_csv(doc_ids: List[str], dest_path: Path) -> Path:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    with dest_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(A3_CSV_COLUMNS)
        for doc_id in doc_ids:
            per_doc = utils.BASE_DIR / "OUT" / "csv" / f"{doc_id}.csv"
            if not per_doc.exists():
                continue
            with per_doc.open("r", encoding="utf-8") as source_fh:
                reader = csv.reader(source_fh)
                header_seen = False
                for idx, row in enumerate(reader):
                    if idx == 0:
                        header_seen = True
                        continue
                    writer.writerow(row)
                if not header_seen:
                    continue
    return dest_path


def _write_incidencias(
    doc_ids: List[str],
    dest_path: Path,
    review_cache: dict[str, Tuple[str, str]],
    doc_info: dict[str, Tuple],
):
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    with dest_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(INCIDENCIAS_COLUMNS)
        for doc_id in doc_ids:
            normalized = _read_normalized(doc_id) or {}
            totals = (normalized.get("totals") or {})
            supplier = (normalized.get("supplier") or {}).get("name", "")
            invoice_number = (normalized.get("invoice") or {}).get("number", "")
            row = doc_info.get(doc_id)
            status = row["status"] if row else "UNKNOWN"
            filename = row["filename"] if row else ""
            issues = []
            if row and row["issues"]:
                try:
                    issues = json.loads(row["issues"])
                except json.JSONDecodeError:
                    issues = [row["issues"]]
            suggested_account = ""
            suggested_iva = ""
            review_entry = review_cache.get(doc_id)
            if review_entry:
                try:
                    suggested = json.loads(review_entry[1] or "{}")
                    if isinstance(suggested, dict):
                        suggestion_payload = suggested.get("suggestion") or suggested
                    else:
                        suggestion_payload = {}
                except json.JSONDecodeError:
                    suggestion_payload = {}
                suggested_account = str(suggestion_payload.get("account") or "")
                suggested_iva = str(suggestion_payload.get("iva_type") or "")
            writer.writerow(
                [
                    doc_id,
                    filename,
                    supplier,
                    invoice_number,
                    totals.get("gross", ""),
                    "|".join(issues),
                    status,
                    suggested_account,
                    suggested_iva,
                ]
            )


def _write_resumen(
    dest_path: Path,
    tenant: str,
    batch_name: str,
    doc_rows: List[Tuple],
    ok_ids: List[str],
    incident_ids: List[str],
    a3_errors: List[Tuple[int, str, str]],
    provider_state: Optional[Dict[Tuple[str, str], Dict[str, Any]]] = None,
    azure_stats: Optional[Dict[str, Any]] = None,
    expected_files: Optional[int] = None,
    expected_pages: Optional[int] = None,
) -> None:
    total_docs = len(doc_rows)
    issue_counter = Counter()
    ocr_times: List[float] = []
    llm_times: List[float] = []
    rules_times: List[float] = []
    provider_counts = Counter()
    llm_provider_counts = Counter()
    confidence_values: List[float] = []
    llm_model_counts = Counter()
    ok_llm_model_counts = Counter()
    for row in doc_rows:
        issues = []
        if row["issues"]:
            try:
                issues = json.loads(row["issues"])
            except json.JSONDecodeError:
                issues = [row["issues"]]
        issue_counter.update(issues)
        if row["ocr_time_ms"]:
            ocr_times.append(float(row["ocr_time_ms"]))
        if row["llm_time_ms"]:
            llm_times.append(float(row["llm_time_ms"]))
        if row["rules_time_ms"]:
            rules_times.append(float(row["rules_time_ms"]))
        provider_counts.update([row["ocr_provider"] or "unknown"])
        llm_provider_counts.update([row["llm_provider"] or "unknown"])
        if row["global_conf"]:
            try:
                confidence_values.append(float(row["global_conf"]))
            except (TypeError, ValueError):
                pass
        model_used = row["llm_model_used"] or "unknown"
        model_key = str(model_used).lower()
        llm_model_counts.update([model_key])
        if row["doc_id"] in ok_ids:
            ok_llm_model_counts.update([model_key])

    def _avg(values: List[float]) -> float:
        return round(sum(values) / len(values), 2) if values else 0.0

    ok_count = len(ok_ids)
    incident_count = len(incident_ids)
    ok_pct = round((ok_count / total_docs * 100), 1) if total_docs else 0.0
    incident_pct = round((incident_count / total_docs * 100), 1) if total_docs else 0.0
    page_counts = [int(row["page_count"] or 0) for row in doc_rows]
    pages_total = sum(page_counts)
    missing_page_docs = [row["doc_id"] for row in doc_rows if not row["page_count"]]
    zero_page_docs = [row["doc_id"] for row in doc_rows if row["page_count"] == 0]
    batch_warnings: List[str] = []
    mini_docs = ok_llm_model_counts.get("mini", 0)
    premium_docs = ok_llm_model_counts.get("premium", 0)
    mini_ratio = round((mini_docs / total_docs * 100), 1) if total_docs else 0.0
    premium_ratio = round((premium_docs / total_docs * 100), 1) if total_docs else 0.0
    current_threshold = getattr(settings, "llm_premium_threshold_gross", 0.0)
    suggestion_value = None
    suggestion_file = dest_path.parent / "LLM_TUNING_SUGGESTIONS.txt"
    if suggestion_file.exists():
        try:
            for line in suggestion_file.read_text(encoding="utf-8").splitlines():
                if line.lower().startswith("suggested threshold"):
                    suggestion_value = line.split(":", 1)[-1].strip()
                    break
        except Exception:
            suggestion_value = None

    def _percentile(values: List[float], pct: float) -> float:
        if not values:
            return 0.0
        values = sorted(values)
        k = (len(values) - 1) * pct
        f = int(k)
        c = min(f + 1, len(values) - 1)
        if f == c:
            return round(values[f], 4)
        return round(values[f] * (c - k) + values[c] * (k - f), 4)

    llm_costs: Dict[str, Dict[str, float]] = {}
    for row in doc_rows:
        model_key = (row["llm_model_used"] or "unknown").lower()
        bucket = llm_costs.setdefault(model_key, {"docs": 0, "tokens_in": 0.0, "tokens_out": 0.0, "cost": 0.0})
        bucket["docs"] += 1
        if row["llm_tokens_in"]:
            bucket["tokens_in"] += float(row["llm_tokens_in"])
        if row["llm_tokens_out"]:
            bucket["tokens_out"] += float(row["llm_tokens_out"])
        if row["llm_cost_eur"]:
            bucket["cost"] += float(row["llm_cost_eur"])

    lines = [
        f"Lote: {batch_name}",
        f"Tenant: {tenant}",
        f"Documentos tratados: {total_docs}",
        f"Documentos OK: {ok_count} ({ok_pct}%)",
        f"Documentos con incidencias: {incident_count} ({incident_pct}%)",
        f"Páginas contabilizadas: {pages_total} (docs sin conteo: {len(missing_page_docs)})",
    ]
    if expected_files is not None:
        lines.append(f"Docs esperados: {expected_files} · Diferencia: {expected_files - total_docs}")
    if expected_pages is not None:
        lines.append(f"Páginas esperadas: {expected_pages} · Diferencia: {expected_pages - pages_total}")
    if missing_page_docs or zero_page_docs:
        lines.append(
            f"  Aviso conteo páginas → sin conteo: {missing_page_docs[:5]}{'...' if len(missing_page_docs) > 5 else ''}; "
            f"páginas 0: {zero_page_docs[:5]}{'...' if len(zero_page_docs) > 5 else ''}"
        )
        batch_warnings.append("PAGECOUNT_MISMATCH")
    lines += [
        "",
        "Issues frecuentes:",
    ]
    for code, count in issue_counter.most_common(5):
        lines.append(f"  - {code or 'N/A'}: {count}")
    lines += [
        "",
        f"OCR provider breakdown: {dict(provider_counts)}",
        f"LLM provider breakdown: {dict(llm_provider_counts)}",
        "",
        "Confianza global:",
        f"  media: {_avg(confidence_values)}",
        f"  p50: {_percentile(confidence_values, 0.5)}",
        f"  p90: {_percentile(confidence_values, 0.9)}",
        "",
        "LLM detalle:",
        f"  strategy: {getattr(settings, 'llm_strategy', 'unknown')}",
        f"  mini_docs (OK): {mini_docs}",
        f"  premium_docs (OK): {premium_docs}",
        f"  mini_ratio: {mini_ratio}%",
        f"  premium_ratio: {premium_ratio}%",
        f"  current_threshold_gross: {current_threshold}",
        f"  modelos (todos los docs): {dict(llm_model_counts)}",
    ]
    if suggestion_value:
        lines.append(f"  suggested_threshold_gross: {suggestion_value}")
    lines += [
        "",
        "LLM costes/tokens:",
    ]
    for label in sorted(llm_costs):
        bucket = llm_costs[label]
        lines.append(
            f"  {label}: docs={bucket['docs']} · tokens_in={round(bucket['tokens_in'],2)} · "
            f"tokens_out={round(bucket['tokens_out'],2)} · coste=€{round(bucket['cost'],4)}"
        )
    total_cost = round(sum(bucket["cost"] for bucket in llm_costs.values()), 4)
    cost_per_doc_ok = round(total_cost / ok_count, 4) if ok_count else 0.0
    cost_per_doc_total = round(total_cost / total_docs, 4) if total_docs else 0.0
    lines.append(f"  Coste total LLM (€): {total_cost}")
    lines.append(f"  Coste medio por doc OK (€): {cost_per_doc_ok}")
    lines.append(f"  Coste medio por doc total (€): {cost_per_doc_total}")
    lines += [
        "",
        f"Tiempos medios (ms) → OCR: {_avg(ocr_times)} · Rules: {_avg(rules_times)} · LLM: {_avg(llm_times)}",
        f"Generado: {datetime.now(timezone.utc).isoformat()}",
    ]
    if azure_stats:
        latency_samples = azure_stats.get("latency_samples", [])
        delay_samples = azure_stats.get("delay_samples", [])
        cache_hits = azure_stats.get("cache_hits", 0)
        cache_total = cache_hits + azure_stats.get("cache_misses", 0)
        cache_pct = round((cache_hits / cache_total * 100), 1) if cache_total else 0.0
        lines += [
            "",
            "Azure OCR health:",
            f"  llamadas: {azure_stats.get('calls_total',0)} · reintentos: {azure_stats.get('retry_total',0)}",
            f"  status: {azure_stats.get('status_counts',{})}",
            f"  cache hit: {cache_pct}%",
            f"  latencia ms p50/p95: {_percentile(latency_samples,0.5)} / {_percentile(latency_samples,0.95)}",
            f"  delay throttle ms p50/p95: {_percentile(delay_samples,0.5)} / {_percentile(delay_samples,0.95)}",
        ]
    if provider_state:
        lines += ["", "Provider health:"]
        for key, data in sorted(provider_state.items()):
            kind, name = key
            status = "DEGRADED" if data.get("degraded") else "OK"
            avg_ttd = round(float(data.get("avg_time_to_degrade") or 0.0), 2)
            lines.append(
                f"  {kind.upper()} {name}: {status} (fails={data.get('total_failures',0)} "
                f"threshold={data.get('threshold',0)} avg_ttd={avg_ttd}s)"
            )
    if a3_errors:
        lines += [
            "",
            "Errores CSV A3 (top 5):",
        ]
        for line_num, field, message in a3_errors[:5]:
            lines.append(f"  - Línea {line_num} · {field}: {message}")
        lines.append(f"Total errores CSV A3: {len(a3_errors)}")
        batch_warnings.append("A3_VALIDATION_ERROR")
    if expected_files is not None and expected_files != total_docs:
        batch_warnings.append("BATCH_FILES_MISMATCH")
    if expected_pages is not None and expected_pages != pages_total:
        batch_warnings.append("BATCH_PAGES_MISMATCH")
    if batch_warnings:
        lines += ["", f"Batch warnings: {', '.join(batch_warnings)}"]
        try:
            utils.persist_batch_warnings(batch_name, batch_warnings)
        except Exception as exc:  # pragma: no cover - defensivo
            logger.warning("No se pudieron persistir batch warnings: %s", exc)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    dest_path.write_text("\n".join(lines), encoding="utf-8")


def build_batch_outputs(
    doc_ids: Iterable[Optional[str]],
    tenant: str,
    batch_name: str,
    expected_files: Optional[int] = None,
    expected_pages: Optional[int] = None,
) -> Path:
    valid_ids = [doc_id for doc_id in doc_ids if doc_id]
    batch_dir = utils.BASE_DIR / "OUT" / tenant / batch_name
    batch_dir.mkdir(parents=True, exist_ok=True)
    if not valid_ids:
        (batch_dir / "RESUMEN.txt").write_text(
            "Lote vacío. No se procesaron documentos.", encoding="utf-8"
        )
        return batch_dir
    doc_rows = _fetch_docs(valid_ids)
    info = {row["doc_id"]: row for row in doc_rows}
    ok_ids = [doc_id for doc_id in valid_ids if info.get(doc_id) and info[doc_id]["status"] == "POSTED"]
    incident_ids = [doc_id for doc_id in valid_ids if doc_id not in ok_ids]
    review_cache: dict[str, Tuple[str, str]] = {}
    for doc_id in incident_ids:
        entry = _fetch_review_entry(doc_id)
        if entry:
            review_cache[doc_id] = entry
    merged_csv = _merge_doc_csv(ok_ids, batch_dir / "a3_asientos.csv")
    a3_errors = []
    if ok_ids and merged_csv.exists():
        a3_errors = validate_a3_csv(merged_csv, tenant=tenant)
        if a3_errors:
            invalid_csv = merged_csv.with_name("a3_asientos.INVALID.csv")
            merged_csv.rename(invalid_csv)
            errors_txt = merged_csv.with_name("a3_errors.txt")
            lines = [f"Línea {ln}: {field} -> {msg}" for ln, field, msg in a3_errors]
            errors_txt.write_text("\n".join(lines), encoding="utf-8")
            issues = ["A3_VALIDATION_ERROR"]
            utils.persist_issues(ok_ids[0], issues)  # marca al menos un doc para reflejar warning
    _write_incidencias(incident_ids, batch_dir / "incidencias.csv", review_cache, info)
    for doc_id in valid_ids:
        llm_debug.copy_into_batch(doc_id, batch_dir)
    state = provider_health.snapshot()
    azure_stats = azure_ocr_monitor.snapshot(reset=True)
    _write_resumen(
        batch_dir / "RESUMEN.txt",
        tenant,
        batch_name,
        doc_rows,
        ok_ids,
        incident_ids,
        a3_errors,
        state,
        azure_stats,
        expected_files=expected_files,
        expected_pages=expected_pages,
    )
    provider_health.reset_all()
    return batch_dir
