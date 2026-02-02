import argparse
import getpass

from . import hitl_service, utils
from . import rules_engine


def list_queue(doc_type: str | None = None) -> None:
    items = hitl_service.fetch_review_items(doc_type_prefix=doc_type)
    if not items:
        print("No hay documentos pendientes de revisión.")
        return
    print(f"Pendientes: {len(items)}")
    for item in items:
        supplier = item.supplier
        invoice = item.invoice
        totals = item.totals
        print(
            f"- {item.doc_id[:8]} | {supplier.get('name')} | {invoice.get('number')} | "
            f"Total {totals.get('gross')} | Tipo {item.doc_type} | Conc {item.reconciled_pct*100:.1f}% | Issues: {', '.join(item.issues_text)} | "
            f"Conf OCR {item.confidences['ocr'] or '-'} / Entry {item.confidences['entry'] or '-'} / "
            f"Global {item.confidences['global'] or '-'}"
        )
        if item.suggestion:
            print(
                f"    Sugerencia → cuenta {item.suggestion.get('account')} / IVA {item.suggestion.get('iva_type')} "
                f"(conf {item.suggestion.get('confidence_llm', '-')})"
            )


def interactive(doc_id: str | None = None, doc_type: str | None = None) -> None:
    queue = hitl_service.fetch_review_items(doc_type_prefix=doc_type)
    if not queue:
        print("No hay documentos pendientes.")
        return
    for item in queue:
        if doc_id and item.doc_id != doc_id:
            continue
        detail = hitl_service.get_review_detail(item.doc_id)
        supplier = detail["normalized"].get("supplier", {})
        invoice = detail["normalized"].get("invoice", {})
        totals = detail["normalized"].get("totals", {})
        print("\n=== Documento", item.doc_id)
        print("Proveedor:", supplier.get("name"), supplier.get("nif"))
        print("Factura:", invoice.get("number"), "Fecha:", invoice.get("date"))
        print("Importe total:", totals.get("gross"))
        print("Tipo de documento:", detail.get("doc_type"))
        print("Issues:", ", ".join(detail["issues_text"]))
        recon = detail.get("reconciliation") or {}
        print(
            "Conciliación:",
            f"{recon.get('amount', 0):.2f} EUR ({(recon.get('pct') or 0)*100:.1f}%)"
        )
        confidences = detail["confidences"]
        print(
            f"Confianzas → OCR: {confidences['ocr'] or '-'}  Entry: {confidences['entry'] or '-'}  "
            f"Global: {confidences['global'] or '-'}"
        )
        suggestion = detail["suggestion"]
        if suggestion:
            print(
                f"Sugerencia LLM: cuenta {suggestion.get('account')} / IVA {suggestion.get('iva_type')} "
                f"(conf {suggestion.get('confidence_llm', '-')}, motivo: {suggestion.get('rationale', '')})"
            )
        action = input("[A]ceptar, [E]ditar, [D]uplicado, [R]eprocesar, [S]altar, [Q]uitar: ").strip().lower()
        actor = getpass.getuser()
        if action in ("a", ""):
            learn = "NO_RULE" in detail["issues"]
            apply_bulk = False
            if learn:
                apply_bulk = input("¿Aplicar regla a pendientes del mismo NIF? [y/N]: ").strip().lower() == "y"
            hitl_service.accept_doc(
                item.doc_id,
                actor=actor,
                learn_rule=learn,
                apply_to_similar=apply_bulk,
                suggestion=suggestion,
            )
        elif action == "e":
            account = input("Nueva cuenta (ej. 629000): ").strip() or detail["entry"]["lines"][0].get("account", "600000")
            iva_str = input("Nuevo IVA (porcentaje, ej. 21): ").strip()
            iva_rate = float(iva_str) if iva_str else detail["normalized"].get("lines", [{}])[0].get("vat_rate", 21)
            apply_bulk = input("¿Aplicar regla a pendientes del mismo NIF? [y/N]: ").strip().lower() == "y"
            hitl_service.edit_doc(item.doc_id, account, iva_rate, actor=actor, apply_to_similar=apply_bulk)
        elif action == "d":
            hitl_service.mark_duplicate(item.doc_id, actor=actor)
        elif action == "r":
            hitl_service.reprocess_doc(item.doc_id, actor=actor)
        elif action == "q":
            break
        if doc_id:
            break


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CLI de revisión humana CERTIVA")
    sub = parser.add_subparsers(dest="command")
    review = sub.add_parser("review", help="Revisión interactiva")
    review.add_argument("--doc", help="Doc_id específico", dest="doc")
    review.add_argument("--doc-type", help="Filtra por doc_type (prefijo)")
    sub.add_parser("list", help="Listado rápido de pendientes")
    parser.set_defaults(command="review")
    return parser.parse_args()


def main() -> None:
    utils.configure_logging()
    args = parse_args()
    if args.command == "list":
        list_queue(getattr(args, "doc_type", None))
    else:
        interactive(getattr(args, "doc", None), getattr(args, "doc_type", None))


if __name__ == "__main__":
    main()
