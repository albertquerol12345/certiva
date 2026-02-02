from __future__ import annotations

from decimal import Decimal
from typing import Dict, List, Tuple

from . import utils

HoldedError = Tuple[str, str]


def validate_holded_payload(payload: Dict) -> List[HoldedError]:
    """
    Validación mínima offline del JSON de Holded exportado:
    - contact.name y contact.tax_id requeridos y longitud razonable
    - al menos una línea con account/amount
    - totales coherentes si existen (suma líneas ~ totals.gross)
    """
    errors: List[HoldedError] = []
    contact = payload.get("contact") or {}
    lines = payload.get("lines") or []
    totals = payload.get("totals") or {}
    name = (contact.get("name") or "").strip()
    tax_id = (contact.get("tax_id") or "").strip()
    if not name:
        errors.append(("contact.name", "Requerido"))
    if not tax_id or len(tax_id) < 6 or len(tax_id) > 15:
        errors.append(("contact.tax_id", "NIF/CIF requerido (6-15 chars)"))
    if not lines:
        errors.append(("lines", "Debe contener al menos 1 línea"))
    else:
        for idx, line in enumerate(lines, start=1):
            if not line.get("account"):
                errors.append((f"lines[{idx}].account", "Requerido"))
            amount = utils.money(line.get("amount"))
            if amount == 0:
                errors.append((f"lines[{idx}].amount", "Importe 0"))
    if totals:
        try:
            gross = Decimal(str(totals.get("gross") or 0))
            line_sum = sum(utils.money(l.get("amount")) for l in lines)
            if gross and abs(gross - line_sum) > Decimal("0.02"):
                errors.append(("totals.gross", f"No cuadra con sum(lines): {line_sum}"))
        except Exception:
            errors.append(("totals.gross", "Formato inválido"))
    return errors
