"""ERP adapter layer for exporting accounting entries."""
from __future__ import annotations

import argparse
import csv
import logging
from pathlib import Path
from typing import Dict, Protocol, Type

from . import utils, erp_validators
from .config import get_tenant_config, settings

logger = logging.getLogger(__name__)

A3_CSV_COLUMNS = [
    "Fecha",
    "Diario",
    "Documento",
    "Cuenta",
    "Debe",
    "Haber",
    "Concepto",
    "NIF",
]


class ERPAdapter(Protocol):
    name: str

    def export_entry(self, doc_id: str, entry: Dict) -> Path:
        ...


class BaseERPAdapter:
    name = "base"

    def __init__(self, tenant: str, config: Dict[str, str]):
        self.tenant = tenant
        self.config = config

    def export_entry(self, doc_id: str, entry: Dict) -> Path:  # pragma: no cover - interface
        raise NotImplementedError


class A3InnuvaAdapter(BaseERPAdapter):
    name = "a3innuva"

    def _prepare_lines(self, entry: Dict) -> list[Dict]:
        lines = entry.get("lines", [])
        supplier_account = self.config.get("supplier_account", "410000")
        prepared: list[Dict] = []
        for idx, line in enumerate(lines):
            current = dict(line)
            if idx == len(lines) - 1 and supplier_account:
                current.setdefault("account", supplier_account)
            prepared.append(current)
        return prepared

    def export_entry(self, doc_id: str, entry: Dict) -> Path:
        csv_dir = utils.BASE_DIR / "OUT" / "csv"
        csv_dir.mkdir(parents=True, exist_ok=True)
        csv_path = csv_dir / f"{doc_id}.csv"

        journal = entry.get("journal") or self.config.get("default_journal", "COMPRAS")
        entry["journal"] = journal
        lines = self._prepare_lines(entry)

        with csv_path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(A3_CSV_COLUMNS)
            for line in lines:
                writer.writerow(
                    [
                        entry.get("date"),
                        journal,
                        entry.get("invoice_number"),
                        line.get("account"),
                        f"{float(line.get('debit', 0.0)):.2f}",
                        f"{float(line.get('credit', 0.0)):.2f}",
                        line.get("concept") or entry.get("supplier", {}).get("name"),
                        line.get("nif") or entry.get("supplier", {}).get("nif"),
                    ]
                )
        return csv_path


class ContasolAdapter(BaseERPAdapter):
    name = "contasol"

    def export_entry(self, doc_id: str, entry: Dict) -> Path:  # pragma: no cover - placeholder
        raise NotImplementedError(
            "El adapter Contasol aún no está implementado. Configura 'erp': 'a3innuva' o crea una implementación propia."
        )


class HoldedAdapter(BaseERPAdapter):
    name = "holded"

    def export_entry(self, doc_id: str, entry: Dict) -> Path:
        holded_dir = utils.BASE_DIR / "OUT" / "holded"
        holded_dir.mkdir(parents=True, exist_ok=True)
        target = holded_dir / f"{doc_id}.json"
        contact = entry.get("customer") or entry.get("supplier") or {}
        lines_payload = []
        for line in entry.get("lines", []):
            lines_payload.append(
                {
                    "account": line.get("account"),
                    "description": line.get("concept") or entry.get("concept"),
                    "debit": float(line.get("debit", 0.0)),
                    "credit": float(line.get("credit", 0.0)),
                }
            )
        payload = {
            "date": entry.get("date"),
            "dueDate": entry.get("due") or entry.get("due_date"),
            "journal": entry.get("journal"),
            "documentNumber": entry.get("invoice_number"),
            "contact": {
                "name": contact.get("name"),
                "tax_id": contact.get("nif"),
            },
            "currency": entry.get("currency") or entry.get("invoice_currency") or "EUR",
            "lines": lines_payload,
            "totals": entry.get("totals"),
            "metadata": entry.get("metadata"),
        }
        utils.json_dump(payload, target)
        errors = erp_validators.validate_holded_payload(payload)
        if errors:
            errors_path = holded_dir / f"{doc_id}.errors.txt"
            errors_path.write_text("\n".join(f"{field}: {msg}" for field, msg in errors), encoding="utf-8")
            logger.warning("Holded payload con %d errores → %s", len(errors), errors_path)
        return target


ADAPTER_REGISTRY: Dict[str, Type[BaseERPAdapter]] = {
    A3InnuvaAdapter.name: A3InnuvaAdapter,
    ContasolAdapter.name: ContasolAdapter,
    HoldedAdapter.name: HoldedAdapter,
}


def get_adapter_for_tenant(tenant: str) -> ERPAdapter:
    config = get_tenant_config(tenant)
    adapter_name = config.get("erp", "a3innuva").lower()
    adapter_cls = ADAPTER_REGISTRY.get(adapter_name, A3InnuvaAdapter)
    return adapter_cls(tenant, config)


def export_entry(doc_id: str, entry: Dict, mark_posted: bool = True) -> Path:
    tenant = entry.get("tenant") or settings.default_tenant
    adapter = get_adapter_for_tenant(tenant)
    csv_path = adapter.export_entry(doc_id, entry)

    utils.add_audit(doc_id, "EXPORT_CSV", "system", None, {"path": str(csv_path), "adapter": adapter.name})
    if mark_posted:
        utils.record_stage_timestamp(doc_id, "posted")
        utils.update_doc_status(
            doc_id,
            "POSTED",
            entry_conf=entry.get("confidence_entry"),
            global_conf=entry.get("confidence_global"),
        )
    logger.info("Exported entry for tenant %s with adapter %s → %s", tenant, adapter.name, csv_path)
    return csv_path


def _main() -> None:
    parser = argparse.ArgumentParser(description="ERP export helpers")
    sub = parser.add_subparsers(dest="command")

    holded_cmd = sub.add_parser("holded-export", help="Generar JSON offline para Holded")
    holded_cmd.add_argument("--doc-id", required=True)

    args = parser.parse_args()
    if args.command == "holded-export":
        doc_id = args.doc_id
        bundle = utils.read_json(utils.BASE_DIR / "OUT" / "json" / f"{doc_id}.entry.json")
        if not bundle:
            raise SystemExit(f"No se encontró entry para {doc_id}")
        path = HoldedAdapter(settings.default_tenant, get_tenant_config(settings.default_tenant)).export_entry(doc_id, bundle)
        print(f"Holded export generado en {path}")
    else:
        parser.print_help()


if __name__ == "__main__":  # pragma: no cover - CLI manual
    import argparse

    _main()
