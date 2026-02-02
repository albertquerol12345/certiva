import argparse
import json
import logging
from collections import Counter
from datetime import datetime, date
from statistics import mean
from typing import Dict, List, Optional

from . import utils, rules_engine, bank_matcher, reports, azure_ocr_monitor
from .config import settings

logger = logging.getLogger(__name__)


def _parse_ts(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _percentile(values: List[float], pct: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    k = (len(values) - 1) * pct
    f = int(k)
    c = min(f + 1, len(values) - 1)
    if f == c:
        return values[int(k)]
    d0 = values[f] * (c - k)
    d1 = values[c] * (k - f)
    return d0 + d1


def _gather_llm_stats(tenant: Optional[str] = None) -> Dict[str, float]:
    with utils.get_connection() as conn:
        params: List = []
        where = ""
        if tenant:
            where = " WHERE tenant = ?"
            params.append(tenant)
        total = conn.execute(f"SELECT COUNT(*) FROM llm_calls{where}", params).fetchone()[0]
        error_clause = "WHERE error IS NOT NULL"
        error_params: List = []
        if tenant:
            error_clause += " AND tenant = ?"
            error_params.append(tenant)
        errors = conn.execute(f"SELECT COUNT(*) FROM llm_calls {error_clause}", error_params).fetchone()[0]
        avg_latency = (
            conn.execute(f"SELECT AVG(latency_ms) FROM llm_calls{where}", params).fetchone()[0] or 0.0
        )
        recent = conn.execute(
            f"SELECT task, COUNT(*) AS cnt FROM llm_calls{where} GROUP BY task ORDER BY cnt DESC LIMIT 5",
            params,
        ).fetchall()
        cost = 0.0
        today_cost = 0.0
        # La columna cost_eur puede no existir en DBs antiguas; capturamos el error.
        try:
            cost = (
                conn.execute(
                    f"SELECT COALESCE(SUM(cost_eur),0) FROM llm_calls{where}",
                    params,
                ).fetchone()[0]
                or 0.0
            )
            today_cost = (
                conn.execute(
                    (
                        f"SELECT COALESCE(SUM(cost_eur),0) FROM llm_calls{where} AND date(created_at) = date('now')"
                        if tenant
                        else "SELECT COALESCE(SUM(cost_eur),0) FROM llm_calls WHERE date(created_at) = date('now')"
                    ),
                    params,
                ).fetchone()[0]
                or 0.0
            )
        except Exception:
            cost = 0.0
            today_cost = 0.0
    return {
        "total_calls": total,
        "errors": errors,
        "avg_latency_ms": avg_latency,
        "by_task": [(row["task"], row["cnt"]) for row in recent],
        "cost_total_eur": float(cost or 0),
        "cost_today_eur": float(today_cost or 0),
    }


def gather_stats(tenant: Optional[str] = None) -> Dict[str, float]:
    with utils.get_connection() as conn:
        doc_query = "SELECT * FROM docs"
        params: List = []
        if tenant:
            doc_query += " WHERE tenant = ?"
            params.append(tenant)
        docs = conn.execute(doc_query, params).fetchall()
        provider_rows = conn.execute(
            "SELECT COALESCE(ocr_provider, 'unknown') AS provider, COUNT(*) AS cnt FROM docs GROUP BY provider"
        ).fetchall()
        llm_rows = conn.execute(
            "SELECT COALESCE(llm_provider, 'unknown') AS provider, COUNT(*) AS cnt FROM docs GROUP BY provider"
        ).fetchall()
        llm_model_rows = conn.execute(
            "SELECT COALESCE(llm_model_used, 'unknown') AS model, COUNT(*) AS cnt FROM docs GROUP BY model"
        ).fetchall()
        if tenant:
            audit_hits_rows = conn.execute(
                "SELECT doc_id FROM audit WHERE step LIKE 'HITL%' AND doc_id IN (SELECT doc_id FROM docs WHERE tenant = ?)",
                (tenant,),
            ).fetchall()
            learned_rules = conn.execute(
                "SELECT COUNT(*) FROM audit WHERE step = 'LEARN_RULE' AND doc_id IN (SELECT doc_id FROM docs WHERE tenant = ?)",
                (tenant,),
            ).fetchone()[0]
        else:
            audit_hits_rows = conn.execute("SELECT doc_id FROM audit WHERE step LIKE 'HITL%'").fetchall()
            learned_rules = conn.execute("SELECT COUNT(*) FROM audit WHERE step = 'LEARN_RULE'").fetchone()[0]
        audit_hits = {row[0] for row in audit_hits_rows}
    total_posted = [doc for doc in docs if doc["status"] == "POSTED" and doc["posted_ts"]]
    auto_post = [doc for doc in total_posted if doc["doc_id"] not in audit_hits]

    total_durations: List[float] = []
    ocr_durations: List[float] = []
    validation_durations: List[float] = []
    entry_durations: List[float] = []

    for doc in total_posted:
        received = _parse_ts(doc["received_ts"])
        posted = _parse_ts(doc["posted_ts"])
        ocr_ts = _parse_ts(doc["ocr_ts"])
        validated_ts = _parse_ts(doc["validated_ts"])
        entry_ts = _parse_ts(doc["entry_ts"])
        if received and posted:
            total_durations.append((posted - received).total_seconds() / 60)
        if received and ocr_ts:
            ocr_durations.append((ocr_ts - received).total_seconds())
        if ocr_ts and validated_ts:
            validation_durations.append((validated_ts - ocr_ts).total_seconds())
        if validated_ts and posted:
            entry_durations.append((posted - validated_ts).total_seconds())

    duplicate_count = len([doc for doc in docs if doc["duplicate_flag"]])
    page_counts: List[int] = []
    missing_page_docs = 0
    zero_page_docs = 0
    for doc in docs:
        count = doc["page_count"] if "page_count" in doc.keys() else None
        if count is None:
            missing_page_docs += 1
            continue
        try:
            c_int = int(count)
        except (TypeError, ValueError):
            missing_page_docs += 1
            continue
        if c_int == 0:
            zero_page_docs += 1
        page_counts.append(max(0, c_int))

    doc_type_totals: Counter[str] = Counter()
    doc_type_posted: Counter[str] = Counter()
    doc_type_auto: Counter[str] = Counter()
    for doc in docs:
        doc_type = (doc["doc_type"] or "unknown").lower()
        doc_type_totals[doc_type] += 1
    for doc in total_posted:
        doc_type = (doc["doc_type"] or "unknown").lower()
        doc_type_posted[doc_type] += 1
        if doc["doc_id"] not in audit_hits:
            doc_type_auto[doc_type] += 1
    doc_type_summary = []
    for doc_type, total in sorted(doc_type_totals.items()):
        posted = doc_type_posted.get(doc_type, 0)
        auto = doc_type_auto.get(doc_type, 0)
        pct = (auto / posted * 100) if posted else 0.0
        doc_type_summary.append(
            {
                "doc_type": doc_type,
                "total": total,
                "posted": posted,
                "auto_post": auto,
                "auto_post_pct": pct,
            }
        )

    ar_docs = [doc for doc in docs if (doc["doc_type"] or "").lower().startswith("sales")]
    ar_paid = [doc for doc in ar_docs if (doc["reconciled_pct"] or 0) >= 0.999]
    ar_partial = [doc for doc in ar_docs if 0 < (doc["reconciled_pct"] or 0) < 0.999]
    ar_overdue = 0
    today = date.today()
    for doc in ar_docs:
        if (doc["reconciled_pct"] or 0) >= 0.999:
            continue
        entry_path = utils.BASE_DIR / "OUT" / "json" / f"{doc['doc_id']}.entry.json"
        due_iso = None
        if entry_path.exists():
            try:
                entry_data = utils.read_json(entry_path)
            except Exception:  # pragma: no cover - defensive
                entry_data = {}
            due_iso = utils.normalize_date(entry_data.get("due_date") or entry_data.get("invoice", {}).get("due"))
        if due_iso:
            try:
                if datetime.fromisoformat(due_iso).date() < today:
                    ar_overdue += 1
            except ValueError:
                continue

    bank_stats = bank_matcher.gather_bank_stats(tenant=tenant)
    jobs = [
        {
            "id": job["id"],
            "name": job["name"],
            "job_type": job["job_type"],
            "schedule": job["schedule"],
            "enabled": job["enabled"],
            "last_run_at": job["last_run_at"],
            "last_status": job["last_status"],
        }
        for job in utils.list_jobs()
    ]
    today = date.today()
    first_day = today.replace(day=1)
    target_tenant = tenant or (settings.default_tenant if hasattr(settings, "default_tenant") else None)
    pnl_summary = reports.build_pnl(target_tenant, first_day.isoformat(), today.isoformat())
    vat_summary = reports.build_vat_report(target_tenant, first_day.isoformat(), today.isoformat())
    aging_summary = reports.build_aging(target_tenant, today.isoformat(), "AR")
    cashflow_summary = reports.build_cashflow_forecast(target_tenant, today.isoformat(), 3)
    llm_stats = _gather_llm_stats(tenant)
    provider_breakdown = {
        "ocr": [(row["provider"], row["cnt"]) for row in provider_rows],
        "llm": [(row["provider"], row["cnt"]) for row in llm_rows],
    }
    llm_model_breakdown = [(row["model"], row["cnt"]) for row in llm_model_rows]
    rules_mean = [doc["rules_time_ms"] for doc in docs if doc["rules_time_ms"]]
    llm_mean_values = [doc["llm_time_ms"] for doc in docs if doc["llm_time_ms"]]
    ocr_ms_mean = (mean(ocr_durations) * 1000) if ocr_durations else 0.0
    azure_stats = azure_ocr_monitor.snapshot(reset=False)
    batch_warnings = Counter()
    for doc in docs:
        issues_field = doc["issues"]
        if not issues_field:
            continue
        try:
            parsed = json.loads(issues_field)
            for code in parsed:
                if str(code).startswith("BATCH_"):
                    batch_warnings.update([code])
        except Exception:
            continue
    return {
        "docs_total": len(docs),
        "posted": len(total_posted),
        "auto_post_pct": (len(auto_post) / len(total_posted) * 100) if total_posted else 0.0,
        "total_p50": _percentile(total_durations, 0.5),
        "total_p90": _percentile(total_durations, 0.9),
        "total_mean": mean(total_durations) if total_durations else 0.0,
        "ocr_mean": mean(ocr_durations) if ocr_durations else 0.0,
        "validation_mean": mean(validation_durations) if validation_durations else 0.0,
        "entry_mean": mean(entry_durations) if entry_durations else 0.0,
        "ocr_ms_mean": ocr_ms_mean,
        "rules_ms_mean": mean(rules_mean) if rules_mean else 0.0,
        "llm_ms_mean": mean(llm_mean_values) if llm_mean_values else 0.0,
        "duplicates": duplicate_count,
        "rules_learned": learned_rules,
        "doc_types": doc_type_summary,
        "bank": bank_stats,
        "batch_warnings": batch_warnings,
        "pages": {
            "total_pages": sum(page_counts),
            "docs_with_page": len(page_counts),
            "missing_page_docs": missing_page_docs,
            "zero_page_docs": zero_page_docs,
        },
        "ar_summary": {
            "total": len(ar_docs),
            "paid": len(ar_paid),
            "partial": len(ar_partial),
            "pending": len(ar_docs) - len(ar_paid),
            "paid_pct": (len(ar_paid) / len(ar_docs) * 100) if ar_docs else 0.0,
            "overdue": ar_overdue,
        },
        "jobs": jobs,
        "pnl_summary": pnl_summary,
        "vat_summary": vat_summary,
        "aging_summary": aging_summary,
        "cashflow_summary": cashflow_summary,
        "llm_stats": llm_stats,
        "llm_model_breakdown": llm_model_breakdown,
        "provider_breakdown": provider_breakdown,
        "azure_ocr_stats": azure_stats,
    }


def gather_preflight(tenant: Optional[str] = None) -> Dict[str, Counter]:
    with utils.get_connection() as conn:
        query = "SELECT doc_id, status, issues, duplicate_flag FROM docs"
        params: List = []
        if tenant:
            query += " WHERE tenant = ?"
            params.append(tenant)
        docs = conn.execute(query, params).fetchall()
    issue_counter: Counter[str] = Counter()
    status_counter: Counter[str] = Counter()
    duplicates = 0
    for doc in docs:
        status_counter[doc["status"]] += 1
        if doc["duplicate_flag"]:
            duplicates += 1
        if doc["issues"]:
            try:
                issues = json.loads(doc["issues"])
            except json.JSONDecodeError:
                issues = []
            for issue in issues:
                issue_counter[issue] += 1
    return {
        "status_counts": status_counter,
        "issue_counts": issue_counter,
        "duplicates": duplicates,
        "total": len(docs),
    }


def print_stats(tenant: Optional[str] = None) -> None:
    utils.configure_logging()
    data = gather_stats(tenant)
    print("Documentos totales:", data["docs_total"])
    print("Publicado:", data["posted"])
    print(f"Auto-post: {data['auto_post_pct']:.1f}%")
    print(f"Tiempo total medio (min): {data['total_mean']:.2f}")
    print(f"P50 total (min): {data['total_p50']:.2f}")
    print(f"P90 total (min): {data['total_p90']:.2f}")
    print(f"OCR medio (s): {data['ocr_mean']:.2f}")
    print(f"Validación medio (s): {data['validation_mean']:.2f}")
    print(f"Entrada->Post medio (s): {data['entry_mean']:.2f}")
    print("Duplicados detectados:", data["duplicates"])
    print("Reglas aprendidas:", data["rules_learned"])
    if data["doc_types"]:
        print("\nAuto-post por tipo de documento:")
        for item in data["doc_types"]:
            print(
                f"  - {item['doc_type']}: {item['auto_post']}/{item['posted']} "
                f"auto-post ({item['auto_post_pct']:.1f}% sobre publicados, total {item['total']})"
            )
    bank = data.get("bank") or {}
    if bank:
        print(
            "\nConciliación bancaria: "
            f"{bank['docs_fully']} completos / {bank['docs_total']} · "
            f"{bank['docs_partial']} parciales · {bank['docs_unmatched']} sin conciliar"
        )
        print(
            f"Movimientos conciliados: {bank['tx_matched']} / {bank['tx_total']}"
        )
    ar = data.get("ar_summary") or {}
    if ar.get("total"):
        print("\nCartera ventas:")
        print(
            f"  Facturas de venta: {ar['total']} | Cobradas: {ar['paid']} "
            f"({ar['paid_pct']:.1f}%) | Parcialmente cobradas: {ar['partial']} | Pendientes: {ar['pending']}"
        )
        if ar.get("overdue"):
            print(f"  Pendientes vencidas: {ar['overdue']}")
    jobs = data.get("jobs") or []
    if jobs:
        print("\nJobs configurados:")
        for job in jobs:
            status = job["last_status"] or "-"
            print(
                f"  - [{job['id']}] {job['name']} ({job['job_type']}) "
                f"schedule={job['schedule'] or 'manual'} enabled={job['enabled']} last={status}"
            )
    llm = data.get("llm_stats") or {}
    if llm:
        print(
            "\nLLM: total {total} · errores {err} · latencia media {lat:.1f} ms".format(
                total=llm.get("total_calls", 0),
                err=llm.get("errors", 0),
                lat=llm.get("avg_latency_ms", 0.0),
            )
        )
        if llm.get("by_task"):
            for task, count in llm["by_task"]:
                print(f"  - {task}: {count} llamadas")


def print_preflight(tenant: Optional[str] = None) -> None:
    utils.configure_logging()
    data = gather_preflight(tenant)
    print("Pre-SII pre-flight checklist")
    print("----------------------------")
    print("Documentos analizados:", data["total"])
    print("Estados:")
    for status, count in data["status_counts"].most_common():
        print(f"  - {status}: {count}")
    print("Issues detectados:")
    if data["issue_counts"]:
        for code, count in data["issue_counts"].most_common():
            label = rules_engine.ISSUE_MESSAGES.get(code, code)
            print(f"  - {code} ({label}): {count}")
    else:
        print("  - Ninguno 🎉")
    print("Duplicados marcados:", data["duplicates"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Estadísticas CERTIVA")
    parser.add_argument(
        "command",
        choices=["stats", "preflight", "business-summary"],
        nargs="?",
        default="stats",
        help="Tipo de informe a mostrar",
    )
    parser.add_argument("--tenant", help="Tenant opcional para filtrar métricas")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.command == "preflight":
        print_preflight(args.tenant)
    elif args.command == "business-summary":
        stats = gather_stats(tenant=args.tenant)
        preflight = gather_preflight(tenant=args.tenant)
        llm = stats.get("llm_stats") or {}
        pages = stats.get("pages") or {}
        auto_pct = stats.get("auto_post_pct", 0.0)
        total_docs = stats.get("docs_total", 0)
        posted = stats.get("posted", 0)
        print(f"=== CERTIVA · Resumen negocio (tenant={args.tenant or 'all'}) ===")
        print(f"Docs totales: {total_docs} · Posteados: {posted} · Auto-post: {auto_pct:.1f}%")
        print(f"Pendientes HITL: {preflight.get('total', 0)} · Issues top: {preflight.get('issue_counts', Counter()).most_common(3)}")
        print(f"LLM llamadas: {llm.get('total_calls',0)} · errores: {llm.get('errors',0)} · lat media ms: {llm.get('avg_latency_ms',0)}")
        print(f"Páginas: {pages.get('total_pages',0)} · sin conteo: {pages.get('missing_page_docs',0)} · páginas 0: {pages.get('zero_page_docs',0)}")
        if stats.get("batch_warnings"):
            print(f"Batch warnings: {dict(stats['batch_warnings'])}")
    else:
        print_stats(args.tenant)


if __name__ == "__main__":
    main()
