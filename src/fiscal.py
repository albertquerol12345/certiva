"""CLI utilities for fiscal exports (SII, Facturae, FACe, Veri*Factu)."""
from __future__ import annotations

import argparse

from . import efactura_payloads, facturae_export, sii_export
from .config import settings


def cmd_export_sii(args: argparse.Namespace) -> None:
    path = sii_export.write_sii_file(args.tenant, args.date_from, args.date_to)
    print(f"SII export generado en {path}")


def cmd_export_facturae(args: argparse.Namespace) -> None:
    path = facturae_export.write_facturae_file(args.doc_id)
    print(f"Facturae XML generado en {path}")


def cmd_export_face(args: argparse.Namespace) -> None:
    payload = efactura_payloads.build_face_payload(args.doc_id)
    path = efactura_payloads.write_payload(payload, f"face_{args.doc_id}.json")
    print(f"Payload FACe escrito en {path}")


def cmd_export_verifactu(args: argparse.Namespace) -> None:
    payload = efactura_payloads.build_verifactu_record(args.doc_id, args.action)
    path = efactura_payloads.write_payload(payload, f"verifactu_{args.doc_id}_{args.action}.json")
    print(f"Registro VeriFactu escrito en {path}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Herramientas fiscales offline")
    sub = parser.add_subparsers(dest="command", required=True)

    sii_cmd = sub.add_parser("export-sii", help="Generar payload SII para un periodo")
    sii_cmd.add_argument("--tenant")
    sii_cmd.add_argument("--date-from", required=True)
    sii_cmd.add_argument("--date-to", required=True)
    sii_cmd.set_defaults(func=cmd_export_sii)

    fact_cmd = sub.add_parser("export-facturae", help="Generar XML Facturae para un doc AR")
    fact_cmd.add_argument("--doc-id", required=True)
    fact_cmd.set_defaults(func=cmd_export_facturae)

    face_cmd = sub.add_parser("export-face-payload", help="Generar payload FACe/FACeB2B")
    face_cmd.add_argument("--doc-id", required=True)
    face_cmd.set_defaults(func=cmd_export_face)

    veri_cmd = sub.add_parser("export-verifactu", help="Registrar evento VeriFactu")
    veri_cmd.add_argument("--doc-id", required=True)
    veri_cmd.add_argument("--action", choices=["ALTA", "MODIF", "BAJA"], default="ALTA")
    veri_cmd.set_defaults(func=cmd_export_verifactu)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
