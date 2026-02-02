from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from dateutil import parser as date_parser
from pydantic import BaseModel, ValidationError
from pydantic import model_validator
from decimal import Decimal, InvalidOperation

from .config import get_ocr_provider, settings
from . import llm_suggest
from . import utils
from . import classifier
from .ocr_providers import OCRProvider, OCRResult

logger = logging.getLogger(__name__)


class Supplier(BaseModel):
    name: str
    nif: str
    vat: Optional[str] = None
    country: str = "ES"


class InvoiceInfo(BaseModel):
    number: str
    date: str
    due: Optional[str] = None
    currency: str = "EUR"

    @model_validator(mode="before")
    def normalize_dates(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(values, dict):
            return values
        for field in ("date", "due"):
            raw = values.get(field)
            if raw:
                values[field] = to_iso_date(raw)
        return values


class Totals(BaseModel):
    base: float
    vat: float
    gross: float


class InvoiceLine(BaseModel):
    desc: str
    qty: float
    unit_price: float
    vat_rate: float
    amount: float


class SourceMeta(BaseModel):
    channel: str
    filename: str


class NormalizedInvoice(BaseModel):
    doc_id: str
    tenant: str
    supplier: Supplier
    invoice: InvoiceInfo
    totals: Totals
    lines: List[InvoiceLine]
    confidence_ocr: float
    source: SourceMeta
    metadata: Optional[Dict[str, Any]] = None


def _parse_vat_breakdown(raw: str) -> List[Tuple[Decimal, Decimal]]:
    breakdown: List[Tuple[Decimal, Decimal]] = []
    if not raw:
        return breakdown
    for chunk in raw.replace(";", "|").split("|"):
        if not chunk:
            continue
        if ":" not in chunk:
            continue
        rate_str, base_str = chunk.split(":", 1)
        try:
            rate = Decimal(rate_str.strip())
            base = utils.quantize_amount(base_str.strip())
        except (InvalidOperation, ValueError):
            continue
        breakdown.append((rate, base))
    return breakdown


def to_iso_date(value: Any) -> str:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, datetime):
        return value.date().isoformat()
    try:
        return date_parser.parse(str(value)).date().isoformat()
    except (ValueError, TypeError):
        return date.today().isoformat()


def extract_invoice(
    doc_id: str,
    file_path: Path,
    tenant: str,
    metadata: Optional[Dict[str, Any]] = None,
    provider: Optional[OCRProvider] = None,
) -> NormalizedInvoice:
    provider_instance = provider or get_ocr_provider()
    result = provider_instance.analyze_document(file_path, tenant)
    suggestions = {}
    if (not result.invoice_number or not result.supplier_nif) and result.ocr_text:
        suggestions = llm_suggest.suggest_missing_fields(result.ocr_text)
        if not result.invoice_number and "invoice.number" in suggestions:
            result.invoice_number = suggestions["invoice.number"]["value"]
            result.confidence -= 0.05
        if not result.supplier_nif and "supplier.nif" in suggestions:
            result.supplier_nif = suggestions["supplier.nif"]["value"]
            result.confidence -= 0.05

    confidence = max(0.0, min(result.confidence, 0.99))
    if result.base and result.vat and not result.gross:
        result.gross = result.base + result.vat
    elif result.gross and not result.base:
        result.base = max(result.gross - result.vat, 0)

    base_amount = utils.quantize_amount(result.base or 0.0)
    vat_amount = utils.quantize_amount(result.vat or 0.0)
    gross_amount = utils.quantize_amount(result.gross or (base_amount + vat_amount))

    lines = result.items or [
        {
            "desc": "Concepto",
            "qty": 1.0,
            "unit_price": float(base_amount),
            "vat_rate": 21.0,
            "amount": float(base_amount),
        }
    ]
    normalized_lines = []
    for item in lines:
        normalized_lines.append(
            {
                "desc": item.get("desc", "Concepto"),
                "qty": float(utils.money(item.get("qty", 1.0))),
                "unit_price": utils.decimal_to_float(utils.quantize_amount(item.get("unit_price", item.get("amount", 0.0)))),
                "vat_rate": float(utils.money(item.get("vat_rate", 21.0))),
                "amount": utils.decimal_to_float(utils.quantize_amount(item.get("amount", 0.0))),
            }
        )

    totals_override = None
    if metadata and metadata.get("vat_breakdown"):
        breakdown = _parse_vat_breakdown(metadata["vat_breakdown"])
        if breakdown:
            normalized_lines = []
            base_amount = Decimal("0")
            vat_amount = Decimal("0")
            for idx, (rate, base_val) in enumerate(breakdown, 1):
                vat_val = utils.quantize_amount(base_val * rate / Decimal(100))
                base_amount += base_val
                vat_amount += vat_val
                normalized_lines.append(
                    {
                        "desc": f"Concepto {idx} ({float(rate):.2f}%)",
                        "qty": 1.0,
                        "unit_price": utils.decimal_to_float(base_val),
                        "vat_rate": float(rate),
                        "amount": utils.decimal_to_float(base_val),
                    }
                )
            gross_amount = base_amount + vat_amount
            totals_override = {
                "base": utils.decimal_to_float(base_amount),
                "vat": utils.decimal_to_float(vat_amount),
                "gross": utils.decimal_to_float(gross_amount),
            }

    invoice_date = utils.normalize_date(result.invoice_date) or utils.today_iso()
    due_date = utils.normalize_date(result.due_date)
    currency = utils.normalize_currency(result.currency)
    totals_dict = totals_override or {
        "base": utils.decimal_to_float(base_amount),
        "vat": utils.decimal_to_float(vat_amount),
        "gross": utils.decimal_to_float(gross_amount),
    }

    metadata_payload = dict(metadata or {})

    normalized_payload = {
        "doc_id": doc_id,
        "tenant": tenant,
        "supplier": {
            "name": result.supplier_name or "Proveedor",
            "nif": result.supplier_nif or "",
            "vat": result.supplier_vat or result.supplier_nif or "",
            "country": "ES",
        },
        "invoice": {
            "number": result.invoice_number or f"{doc_id[:8]}",
            "date": invoice_date,
            "due": due_date,
            "currency": currency,
        },
        "totals": totals_dict,
        "lines": normalized_lines,
        "confidence_ocr": round(confidence, 4),
        "source": {
            "channel": "inbox",
            "filename": file_path.name,
        },
        "metadata": metadata_payload,
    }

    doc_type = classifier.classify_document(normalized_payload)
    normalized_payload["metadata"]["doc_type"] = doc_type
    try:
        normalized = NormalizedInvoice(**normalized_payload)
    except ValidationError as exc:
        logger.error("Validation error normalizing invoice %s: %s", doc_id, exc)
        raise

    json_path = utils.BASE_DIR / "OUT" / "json" / f"{doc_id}.json"
    utils.json_dump(normalized.dict(), json_path)
    return normalized
