"""Offline builders for SII (Libro IVA) exports."""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional

from . import reports, utils
from .config import settings

SII_DIR = utils.BASE_DIR / "OUT" / "sii"
SII_DIR.mkdir(parents=True, exist_ok=True)


def _load_doc_bundle(doc_id: str) -> Dict[str, Any]:
    doc = utils.get_doc(doc_id)
    if not doc:
        raise ValueError(f"No existe el documento {doc_id}")
    normalized = utils.read_json(utils.BASE_DIR / "OUT" / "json" / f"{doc_id}.json")
    entry_path = utils.BASE_DIR / "OUT" / "json" / f"{doc_id}.entry.json"
    entry = utils.read_json(entry_path) if entry_path.exists() else {}
    return {"doc": doc, "normalized": normalized, "entry": entry}


def build_sii_invoice_payload(doc_id: str) -> Dict[str, Any]:
    bundle = _load_doc_bundle(doc_id)
    doc = bundle["doc"]
    normalized = bundle["normalized"]
    entry = bundle["entry"]
    metadata = normalized.get("metadata") or {}
    invoice = normalized.get("invoice") or {}
    totals = normalized.get("totals") or {}
    supplier = normalized.get("supplier") or {}
    flow = (metadata.get("flow") or ("AR" if (doc["doc_type"] or "").startswith("sales") else "AP")).upper()

    receptor = supplier if flow == "AR" else normalized.get("customer") or supplier

    payload = {
        "LibroRegistro": "Emitidas" if flow == "AR" else "Recibidas",
        "IDFactura": {
            "NumSerieFacturaEmisor": invoice.get("number") or doc["doc_id"][:16],
            "FechaExpedicionFacturaEmisor": invoice.get("date"),
        },
        "Emisor": {
            "NombreRazon": settings.sii_name,
            "NIF": settings.sii_tax_id,
        },
        "Receptor": {
            "NombreRazon": receptor.get("name"),
            "NIF": receptor.get("nif"),
        },
        "DatosFactura": {
            "BaseImponible": totals.get("base"),
            "CuotaIVA": totals.get("vat"),
            "TipoImpositivo": totals.get("vat") and totals.get("base") and round(
                float(totals["vat"]) / float(totals["base"]) * 100, 2
            ),
            "CuotaDeducible": totals.get("vat") if flow == "AP" else None,
        },
        "EstadoCuadre": entry.get("confidence_entry"),
        "Notas": {
            "Categoria": metadata.get("category"),
            "DocType": doc.get("doc_type"),
        },
    }
    return payload


def export_sii_period(tenant: Optional[str], date_from: str, date_to: str) -> Dict[str, Any]:
    registros = []
    for item in reports.iter_docs(tenant, date_from, date_to, statuses=["POSTED", "ENTRY_READY"]):
        doc_id = item["doc"]["doc_id"]
        try:
            registro = build_sii_invoice_payload(doc_id)
            registros.append(registro)
        except Exception as exc:  # pragma: no cover - defensivo
            registros.append(
                {
                    "LibroRegistro": "ERROR",
                    "IDFactura": {"NumSerieFacturaEmisor": doc_id},
                    "Error": str(exc),
                }
            )
    return {
        "Contribuyente": {"Nombre": settings.sii_name, "NIF": settings.sii_tax_id},
        "Periodo": {"Desde": date_from, "Hasta": date_to},
        "Registros": registros,
    }


def write_sii_file(
    tenant: Optional[str],
    date_from: str,
    date_to: str,
    path: Optional[Path] = None,
) -> Path:
    payload = export_sii_period(tenant, date_from, date_to)
    target = path or (SII_DIR / f"sii_{tenant or 'all'}_{date_from}_{date_to}.json")
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return target
