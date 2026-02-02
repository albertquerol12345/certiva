"""Offline payload builders for FACe/FACeB2B and Veri*Factu."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Literal

from . import utils
from .config import settings

EFACT_DIR = utils.BASE_DIR / "OUT" / "efactura"
EFACT_DIR.mkdir(parents=True, exist_ok=True)


def _load_doc(doc_id: str) -> Dict[str, Any]:
    doc = utils.get_doc(doc_id)
    if not doc:
        raise ValueError(f"No existe el documento {doc_id}")
    normalized = utils.read_json(utils.BASE_DIR / "OUT" / "json" / f"{doc_id}.json")
    entry_path = utils.BASE_DIR / "OUT" / "json" / f"{doc_id}.entry.json"
    entry = utils.read_json(entry_path) if entry_path.exists() else {}
    return {"doc": doc, "normalized": normalized, "entry": entry}


def build_face_payload(doc_id: str) -> Dict[str, Any]:
    bundle = _load_doc(doc_id)
    normalized = bundle["normalized"]
    invoice = normalized.get("invoice") or {}
    totals = normalized.get("totals") or {}
    supplier = normalized.get("supplier") or {}
    customer = normalized.get("customer") or {}

    return {
        "Cabecera": {
            "CodigoDir": customer.get("dir_code", "0000"),
            "Contrato": invoice.get("contract") or "",
            "Expediente": invoice.get("expediente") or "",
        },
        "Factura": {
            "Numero": invoice.get("number") or doc_id,
            "Fecha": invoice.get("date"),
            "ImporteTotal": totals.get("gross"),
            "Proveedor": {
                "Nombre": supplier.get("name"),
                "NIF": supplier.get("nif"),
            },
            "Cliente": {
                "Nombre": customer.get("name") or settings.facturae_name,
                "NIF": customer.get("nif") or settings.facturae_tax_id,
            },
        },
    }


def build_verifactu_record(doc_id: str, action: Literal["ALTA", "MODIF", "BAJA"]) -> Dict[str, Any]:
    bundle = _load_doc(doc_id)
    normalized = bundle["normalized"]
    invoice = normalized.get("invoice") or {}
    totals = normalized.get("totals") or {}
    return {
        "accion": action,
        "doc_id": doc_id,
        "numFactura": invoice.get("number") or doc_id,
        "fecha": invoice.get("date"),
        "hashAnterior": normalized.get("verifactu", {}).get("hash"),
        "importe": totals.get("gross"),
    }


def write_payload(payload: Dict[str, Any], name: str) -> Path:
    target = EFACT_DIR / name
    target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return target
