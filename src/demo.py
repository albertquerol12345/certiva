"""Demo command to process the golden set and showcase the pipeline."""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

from . import metrics, pipeline, utils, hitl_cli

GOLDEN_DIR = Path(__file__).resolve().parents[1] / "tests" / "golden"


def _reset_state() -> None:
    with utils.get_connection() as conn:
        for table in ("docs", "review_queue", "audit", "dedupe"):
            conn.execute(f"DELETE FROM {table}")
    # Opcional: limpiar outputs antiguos para que la demo sea más clara
    for sub in ("json", "csv"):
        target = utils.BASE_DIR / "OUT" / sub
        for file in target.glob("*." + ("json" if sub == "json" else "csv")):
            file.unlink(missing_ok=True)


def _process_directory(directory: Path) -> None:
    files = sorted(p for p in directory.glob("*.pdf") if p.is_file())
    for file_path in files:
        pipeline.process_file(file_path, force=True)


def _print_stats() -> None:
    data = metrics.gather_stats()
    print("\n=== Métricas actuales ===")
    print("Documentos totales:", data["docs_total"])
    print("Publicado:", data["posted"])
    print(f"Auto-post: {data['auto_post_pct']:.1f}%")
    print(f"P50 total (min): {data['total_p50']:.2f} | P90 total (min): {data['total_p90']:.2f}")
    print(f"Duplicados detectados: {data['duplicates']}")
    print(f"Reglas aprendidas: {data['rules_learned']}")


def _print_queue(limit: int = 5) -> list:
    queue = utils.fetch_review_queue(limit)
    if not queue:
        print("\n✅ No hay documentos pendientes en HITL")
        return []
    print("\nDocumentos pendientes de revisión:")
    for row in queue:
        doc_id = row["doc_id"]
        normalized = utils.read_json(utils.BASE_DIR / "OUT" / "json" / f"{doc_id}.json")
        supplier = normalized.get("supplier", {})
        invoice = normalized.get("invoice", {})
        print(
            f" - {doc_id[:8]} | {supplier.get('name')} ({supplier.get('nif')}) | "
            f"Factura {invoice.get('number')} | Total {normalized.get('totals', {}).get('gross')}"
        )
    return queue


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Demo guiada del motor CERTIVA")
    parser.add_argument("--reset", action="store_true", help="Resetea la base de datos antes de empezar")
    parser.add_argument("--batch", type=Path, default=GOLDEN_DIR, help="Carpeta de PDFs a procesar")
    parser.add_argument(
        "--hitl",
        action="store_true",
        help="Abrir flujo interactivo HITL para el primer documento en cola",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.reset:
        print("→ Reseteando estado local ...")
        _reset_state()
    print(f"→ Procesando {args.batch}")
    _process_directory(args.batch)
    _print_stats()
    queue = _print_queue()
    if args.hitl and queue:
        target_doc = queue[0]["doc_id"]
        print("\nAbriendo revisión interactiva para", target_doc)
        hitl_cli.interactive(doc_id=target_doc)
        print("→ Reprocesando documento tras la revisión...")
        pipeline.reprocess_from_json(target_doc)
        _print_stats()
        _print_queue()
    elif not queue:
        print("Pipeline listo para demo: todos los documentos auto-posteados.")
    else:
        print("Puedes lanzar `python -m src.hitl_cli review` para revisar manualmente los pendientes.")


if __name__ == "__main__":
    main()
