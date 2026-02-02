"""Simple heuristics to classify document types."""
from __future__ import annotations

from typing import Dict, Any


def classify_document(normalized: Dict[str, Any]) -> str:
    metadata = normalized.get("metadata") or {}
    category = (metadata.get("category") or "").lower()
    flow = (metadata.get("flow") or "").lower()
    totals = normalized.get("totals", {})
    gross = float(totals.get("gross", 0) or 0)
    supplier = normalized.get("supplier", {})
    nif = (supplier.get("nif") or "").upper()

    is_ar = flow == "ar" or category.startswith("ventas")
    if is_ar:
        if gross < 0 or category == "ventas_abono":
            return "sales_credit_note"
        if category == "ventas_intracom" or (nif.startswith("EU") and totals.get("vat", 0) in (0, "0", "0.00")):
            return "sales_intracom_invoice"
        return "sales_invoice"

    if category == "abono" or gross < 0:
        return "credit_note"
    if category == "intracomunitaria" or nif.startswith("EU"):
        return "intracom_invoice"
    if category in {"hosteleria", "viajes", "telefonia"} and abs(gross) < 250:
        return "expense_ticket"
    if category in {"marketing", "formacion"} and abs(gross) < 500:
        return "service_invoice"
    return "invoice"
