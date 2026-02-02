"\"\"\"CLI wizard to showcase CERTIVA Engine onboarding/demo.\"\"\""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

from . import hitl_service, pipeline, utils, metrics
from .config import BASE_DIR


def _reset_state() -> None:
    with utils.get_connection() as conn:
        for table in ("docs", "review_queue", "audit", "dedupe"):
            conn.execute(f"DELETE FROM {table}")


def _process_batch(batch_dir: Path) -> None:
    files = sorted(p for p in batch_dir.glob("*.pdf") if p.is_file())
    for file_path in files:
        pipeline.process_file(file_path, force=True)


def _show_metrics(label: str, doc_type_prefix: str | None = None) -> None:
    data = metrics.gather_stats()
    print(f"\n== {label} ==")
    print(f"Documentos totales: {data['docs_total']} | Publicado: {data['posted']}")
    print(f"Auto-post: {data['auto_post_pct']:.1f}%")
    print(f"P50 total (min): {data['total_p50']:.2f} | P90 total (min): {data['total_p90']:.2f}")
    print(f"Duplicados: {data['duplicates']} | Reglas aprendidas: {data['rules_learned']}")
    if doc_type_prefix and doc_type_prefix.startswith("sales"):
        ar = data.get("ar_summary") or {}
        if ar.get("total"):
            print(
                f"Cartera ventas → Total: {ar['total']} | Cobradas: {ar['paid']}"
                f" ({ar['paid_pct']:.1f}%) | Pendientes: {ar['pending']}"
            )


def _select_docs(limit: int, doc_type_prefix: str | None = None) -> List[hitl_service.ReviewDoc]:
    queue = hitl_service.fetch_review_items(doc_type_prefix=doc_type_prefix)
    if not queue:
        return []
    prioritized = [doc for doc in queue if "NO_RULE" in doc.issues][:limit]
    if len(prioritized) < limit:
        prioritized.extend([doc for doc in queue if doc not in prioritized][: limit - len(prioritized)])
    return prioritized[:limit]


def run_wizard(batch_dir: Path, reset: bool, limit: int, doc_type_prefix: str | None) -> None:
    utils.configure_logging()
    if reset:
        print("→ Reseteando estado local ...")
        _reset_state()
    print(f"→ Procesando {batch_dir}")
    _process_batch(batch_dir)
    _show_metrics("Antes de HITL", doc_type_prefix=doc_type_prefix)
    targets = _select_docs(limit, doc_type_prefix=doc_type_prefix)
    if not targets:
        print("No hay documentos pendientes. Nada que revisar 🎉")
        return
    for doc in targets:
        detail = hitl_service.get_review_detail(doc.doc_id)
        supplier = detail["normalized"].get("supplier", {})
        invoice = detail["normalized"].get("invoice", {})
        print("\n---")
        print(f"Doc {doc.doc_id[:8]} | {supplier.get('name')} | Factura {invoice.get('number')} | Total {doc.totals.get('gross')}")
        print("Issues:", ", ".join(detail["issues_text"]))
        suggestion = detail.get("suggestion")
        if suggestion:
            print(
                f"Sugerencia: cuenta {suggestion.get('account')} / IVA {suggestion.get('iva_type')} "
                f"(conf {suggestion.get('confidence_llm', '-')})"
            )
        action = input("Acción [A=aceptar con regla, S=saltar, D=duplicado]: ").strip().lower()
        if action == "d":
            hitl_service.mark_duplicate(doc.doc_id, actor="wizard")
        elif action in ("a", ""):
            apply_bulk = input("¿Aplicar regla a pendientes del mismo NIF? [y/N]: ").strip().lower() == "y"
            hitl_service.accept_doc(
                doc.doc_id,
                actor="wizard",
                learn_rule="NO_RULE" in detail["issues"],
                apply_to_similar=apply_bulk,
                suggestion=suggestion,
            )
        else:
            print("Saltando documento.")
    print("\n→ Reprocesando lote tras acciones HITL ...")
    _process_batch(batch_dir)
    _show_metrics("Después de HITL", doc_type_prefix=doc_type_prefix)
    queue = hitl_service.fetch_review_items()
    print(f"Pendientes restantes: {len(queue)}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Wizard de onboarding/demo CERTIVA")
    parser.add_argument("--path", type=Path, default=BASE_DIR / "tests" / "golden", help="Carpeta de PDFs a procesar")
    parser.add_argument("--reset", action="store_true", help="Resetear base de datos antes de empezar")
    parser.add_argument("--limit", type=int, default=3, help="Número de documentos a revisar en el wizard")
    parser.add_argument(
        "--focus",
        choices=["all", "sales"],
        default="all",
        help="Permite centrar el wizard en facturas de venta (sales) o todo",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.path.exists():
        raise SystemExit(f"No existe la carpeta {args.path}")
    doc_type_prefix = "sales" if args.focus == "sales" else None
    run_wizard(args.path, reset=args.reset, limit=args.limit, doc_type_prefix=doc_type_prefix)


if __name__ == "__main__":
    main()
