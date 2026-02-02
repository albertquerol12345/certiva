import logging
import re
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from difflib import SequenceMatcher
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from . import llm_suggest, utils
from .config import settings, get_tenant_config

logger = logging.getLogger(__name__)
RULES_PATH = utils.BASE_DIR / "rules" / "vendor_map.csv"
AMOUNT_TOLERANCE = Decimal("0.02")

CATEGORY_ACCOUNT_MAP = {
    "suministros": "628000",
    "alquiler": "621000",
    "software": "629000",
    "it_support": "629000",
    "hosteleria": "629500",
    "intracomunitaria": "629000",
    "abono": "700000",
    "marketing": "627000",
    "telefonia": "628100",
    "seguros": "625000",
    "material_oficina": "602000",
    "mantenimiento": "629300",
    "viajes": "629200",
    "servicios_prof": "623000",
    "formacion": "649000",
}

SALES_CATEGORY_ACCOUNT_MAP = {
    "ventas_servicios": "705000",
    "ventas_productos": "700000",
    "ventas_intracom": "705500",
    "ventas_abono": "705000",
    "ventas_ticket": "705200",
}

ISSUE_MESSAGES = {
    "AMOUNT_SCALE_SUSPECT": "Importes sospechosos (posible coma/punto mal leído)",
    "AMOUNT_MISMATCH": "Base + IVA no cuadra con el total",
    "PAGECOUNT_ZERO": "El PDF parece vacío (0 páginas)",
    "RISK_PREMIUM": "Importe alto/categoría sensible: requiere revisión",
    "SECOND_OPINION_DISAGREE": "La revisión LLM secundario difiere de la propuesta",
    "INVALID_DATE": "Fecha de factura inválida",
    "FUTURE_DATE": "Fecha futura fuera de tolerancia",
    "NO_RULE": "No existe mapping proveedor→cuenta",
    "NIF_SUSPECT": "NIF/NIE/CIF no válido",
    "NIF_AMBIGUOUS": "NIF poco fiable",
    "NON_EUR_CURRENCY": "Moneda distinta de EUR",
    "MISSING_SUPPLIER_NIF": "Falta NIF del proveedor",
    "MISSING_INVOICE_NUMBER": "Falta número de factura",
    "DUP_NIF_NUMBER": "Posible duplicado por NIF+Número",
    "DUP_NIF_GROSS": "Posible duplicado por NIF+Importe",
    "CREDIT_NOTE": "Nota de crédito / abono",
    "INTRACOM_IVA0": "Operación intracomunitaria IVA 0%",
    "LINES_INCOMPLETE": "Detalle de líneas incompleto",
    "LLM_NOT_CONFIGURED": "LLM no configurado",
    "LLM_ERROR": "Error en proveedor LLM",
    "LLM_PARSE_ERROR": "No se pudo parsear respuesta LLM",
    "OCR_PROVIDER_FALLBACK": "OCR real no disponible; usando modo dummy",
    "LOW_CONFIDENCE": "Confianza global insuficiente",
    "PROVIDER_DEGRADED": "Proveedor degradado (circuit breaker)",
    "OCR_TEMP_ERROR": "OCR temporalmente no disponible",
    "LLM_TEMP_ERROR": "LLM temporalmente no disponible",
    "PROVIDER_UNAVAILABLE": "Proveedor no disponible temporalmente",
    "WITHHOLDING_PRESENT": "Factura con retención/IRPF",
    "WITHHOLDING_SALES_UNSUPPORTED": "Retención en ventas requiere revisión manual",
    "SUPPLIDO_PRESENT": "Factura con suplidos/partidas exentas",
}

REVIEW_ALWAYS = {
    "AMOUNT_MISMATCH",
    "PAGECOUNT_ZERO",
    "INVALID_DATE",
    "FUTURE_DATE",
    "NO_RULE",
    "NIF_SUSPECT",
    "NON_EUR_CURRENCY",
    "MISSING_SUPPLIER_NIF",
    "MISSING_INVOICE_NUMBER",
    "DUP_NIF_NUMBER",
    "DUP_NIF_GROSS",
    "OCR_PROVIDER_FALLBACK",
    "LOW_CONFIDENCE",
    "PROVIDER_DEGRADED",
    "OCR_TEMP_ERROR",
    "LLM_TEMP_ERROR",
    "PROVIDER_UNAVAILABLE",
    "AMOUNT_SCALE_SUSPECT",
    "RISK_PREMIUM",
    "WITHHOLDING_PRESENT",
    "WITHHOLDING_SALES_UNSUPPORTED",
    "SUPPLIDO_PRESENT",
}

HARD_ISSUES = {
    "AMOUNT_MISMATCH",
    "LINES_INCOMPLETE",
    "MISSING_SUPPLIER_NIF",
    "MISSING_INVOICE_NUMBER",
    "INVALID_DATE",
    "FUTURE_DATE",
    "NIF_SUSPECT",
    "LLM_NOT_CONFIGURED",
    "LLM_ERROR",
    "LLM_PARSE_ERROR",
    "OCR_PROVIDER_FALLBACK",
    "PROVIDER_DEGRADED",
    "OCR_TEMP_ERROR",
    "LLM_TEMP_ERROR",
    "AMOUNT_SCALE_SUSPECT",
    "PAGECOUNT_ZERO",
}

@dataclass
class RuleEvaluation:
    entry: Dict[str, Any]
    confidence_entry: float
    issues: List[str]
    review_payload: Optional[Dict[str, Any]]
    duplicate_flag: int
    llm_metadata: Optional[Dict[str, Any]] = None


def _normalize_name(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").strip().upper())


def _load_rules() -> List[Dict[str, Any]]:
    return utils.load_vendor_rules(RULES_PATH)


def _match_rule(invoice: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str]:
    rules = _load_rules()
    tenant = invoice.get("tenant", settings.default_tenant)
    supplier = invoice.get("supplier", {})
    supplier_nif = (supplier.get("nif") or "").upper()
    supplier_name = _normalize_name(supplier.get("name", ""))
    best_match = None
    best_ratio = 0.0
    for rule in rules:
        if str(rule.get("tenant", tenant)).lower() != tenant.lower():
            continue
        rule_nif = str(rule.get("nif", "")).upper()
        if supplier_nif and rule_nif and rule_nif == supplier_nif:
            return rule, "rule_nif"
        rule_name = _normalize_name(str(rule.get("supplier_name", "")))
        if supplier_name and rule_name:
            ratio = SequenceMatcher(None, supplier_name, rule_name).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_match = rule
    if best_match and best_ratio >= 0.82:
        return best_match, "rule_name"
    return None, ""


def _append_issue(issues: List[str], code: str) -> None:
    if code not in issues:
        issues.append(code)


def _parse_adjustment(metadata: Dict[str, Any], keys: List[str]) -> Decimal:
    for key in keys:
        if key in metadata:
            try:
                return abs(utils.quantize_amount(metadata.get(key, 0)))
            except Exception:
                return Decimal("0")
    return Decimal("0")


def _extract_adjustments(metadata: Dict[str, Any]) -> Tuple[Decimal, Decimal, List[str]]:
    """Extrae retención y suplidos declarados en metadata."""
    withholding = _parse_adjustment(
        metadata,
        ["withholding", "retention", "retencion", "irpf"],
    )
    suplidos = _parse_adjustment(
        metadata,
        ["supplidos", "suplidos", "suplido", "exempt_charges", "gastos_exentos"],
    )
    adjustment_issues: List[str] = []
    if withholding > 0:
        adjustment_issues.append("WITHHOLDING_PRESENT")
    if suplidos > 0:
        adjustment_issues.append("SUPPLIDO_PRESENT")
    return withholding, suplidos, adjustment_issues


def _validate_amounts(totals: Dict[str, Any], lines: List[Dict[str, Any]], metadata: Dict[str, Any]) -> Tuple[List[str], Decimal, Decimal]:
    issues: List[str] = []
    base = utils.quantize_amount(totals.get("base", 0))
    vat = utils.quantize_amount(totals.get("vat", 0))
    gross = utils.quantize_amount(totals.get("gross", 0))
    withholding, suplidos, adjustment_issues = _extract_adjustments(metadata)
    adjusted_total = base + vat + suplidos - withholding
    if abs(adjusted_total - gross) > AMOUNT_TOLERANCE:
        issues.append("AMOUNT_MISMATCH")
    if lines:
        line_total = sum(abs(utils.quantize_amount(line.get("amount", 0))) for line in lines)
        if abs(line_total + suplidos - withholding - gross) > AMOUNT_TOLERANCE:
            issues.append("AMOUNT_MISMATCH")
    issues.extend(adjustment_issues)
    return issues, withholding, suplidos


def _lines_incomplete(invoice: Dict[str, Any]) -> bool:
    lines = invoice.get("lines") or []
    totals = invoice.get("totals") or {}
    gross = utils.quantize_amount(totals.get("gross", 0))
    if gross <= Decimal("20"):
        return False
    meaningful_lines = [
        line
        for line in lines
        if abs(utils.quantize_amount(line.get("amount", 0))) >= Decimal("1")
    ]
    return len(meaningful_lines) == 0


def generate_entry(doc_id: str, invoice: Dict[str, Any]) -> RuleEvaluation:
    totals = invoice.get("totals", {})
    supplier = invoice.get("supplier", {})
    invoice_meta = invoice.get("invoice", {})
    tenant = invoice.get("tenant", settings.default_tenant)
    metadata_info = invoice.get("metadata") or {}
    category = (metadata_info.get("category") or "").strip().lower()
    doc_type = (metadata_info.get("doc_type") or "").lower()
    flow_hint = (metadata_info.get("flow") or "").upper()
    flow = flow_hint or ("AR" if doc_type.startswith("sales") else "AP")
    is_sales = flow == "AR"

    supplier_nif = (supplier.get("nif") or "").strip().upper()
    invoice_number = (invoice_meta.get("number") or "").strip()
    invoice_date = utils.normalize_date(invoice_meta.get("date"))
    due_date = utils.normalize_date(invoice_meta.get("due"))
    currency = utils.normalize_currency(invoice_meta.get("currency"))
    base_amount = utils.quantize_amount(totals.get("base", 0))
    vat_amount = utils.quantize_amount(totals.get("vat", 0))
    gross_amount = utils.quantize_amount(totals.get("gross", 0))
    issues: List[str] = []
    if gross_amount >= Decimal(str(settings.llm_premium_threshold_gross)):
        _append_issue(issues, "RISK_PREMIUM")
    sensitive_categories = {"abono", "ventas_abono", "intracomunitaria", "ventas_intracom", "ventas_ticket"}
    if category in sensitive_categories:
        _append_issue(issues, "RISK_PREMIUM")

    tenant_config = get_tenant_config(tenant)
    supplier_account = tenant_config.get("supplier_account", "410000")
    customer_account = tenant_config.get("customer_account", "430000")
    default_journal = tenant_config.get("default_journal", "COMPRAS")
    sales_journal = tenant_config.get("sales_journal", "VENTAS")
    forced_issues = metadata_info.get("forced_issues") or []
    for issue_code in forced_issues:
        _append_issue(issues, issue_code)
    if not supplier_nif:
        _append_issue(issues, "MISSING_SUPPLIER_NIF")
    if not invoice_number:
        _append_issue(issues, "MISSING_INVOICE_NUMBER")

    amount_issues, withholding_amount, suplidos_amount = _validate_amounts(totals, invoice.get("lines", []), metadata_info)
    for amount_issue in amount_issues:
        _append_issue(issues, amount_issue)
    if _lines_incomplete(invoice):
        _append_issue(issues, "LINES_INCOMPLETE")

    if not invoice_date:
        invoice_date = utils.today_iso()
        _append_issue(issues, "INVALID_DATE")
    else:
        parsed = date.fromisoformat(invoice_date)
        if parsed > date.today() + timedelta(days=3):
            _append_issue(issues, "FUTURE_DATE")

    if currency != "EUR":
        _append_issue(issues, "NON_EUR_CURRENCY")

    nif_status = utils.validate_spanish_nif(supplier_nif)
    nif_penalty = 0.0
    if nif_status == "invalid":
        _append_issue(issues, "NIF_SUSPECT")
    elif nif_status == "maybe":
        nif_penalty = 0.03

    duplicate_flag = 0
    duplicates = utils.find_duplicates(tenant, supplier_nif, invoice_number, totals.get("gross", 0))
    for dup in duplicates:
        if invoice_number and dup["inv_number"] == invoice_number:
            duplicate_flag = 1
            _append_issue(issues, "DUP_NIF_NUMBER")
            break
    if duplicate_flag == 0 and duplicates:
        duplicate_flag = 1
        _append_issue(issues, "DUP_NIF_GROSS")

    is_credit_note = False
    if category in {"abono", "ventas_abono"} or gross_amount < 0:
        is_credit_note = True
        _append_issue(issues, "CREDIT_NOTE")
        base_amount = abs(base_amount)
        vat_amount = abs(vat_amount)
        gross_amount = abs(gross_amount)

    if doc_type == "expense_ticket" and gross_amount > Decimal("500"):
        _append_issue(issues, "AMOUNT_SCALE_SUSPECT")

    is_intracom = supplier_nif.startswith("EU") or category == "intracomunitaria"
    if is_intracom and vat_amount == 0:
        _append_issue(issues, "INTRACOM_IVA0")

    if "SECOND_OPINION_DISAGREE" in (metadata_info.get("forced_issues") or []) or "SECOND_OPINION_DISAGREE" in issues:
        _append_issue(issues, "SECOND_OPINION_DISAGREE")

    default_account = "700000" if is_sales else "600000"
    rule, rule_source = _match_rule(invoice)
    review_payload = None
    llm_metadata: Optional[Dict[str, Any]] = None
    mapping_source = rule_source or "fallback"
    if rule:
        account = str(rule.get("account", default_account))
        iva_type = float(rule.get("iva_type", 21))
    else:
        mapping = llm_suggest.suggest_mapping(invoice)
        account = str(mapping.get("account", "")) if mapping else ""
        iva_type = float(mapping.get("iva_type", 21)) if mapping else 21.0
        review_payload = mapping
        llm_metadata = {
            "provider": mapping.get("provider"),
            "duration_ms": mapping.get("duration_ms"),
            "model_used": mapping.get("model_used"),
            "confidence_llm": mapping.get("confidence_llm"),
            "tokens_in": mapping.get("prompt_tokens"),
            "tokens_out": mapping.get("completion_tokens"),
            "cost_eur": mapping.get("cost_eur"),
        }
        mapping_source = "llm" if mapping else "fallback"
        for code in mapping.get("issue_codes", []):
            _append_issue(issues, code)
        _append_issue(issues, "NO_RULE")
        if not account:
            target_map = SALES_CATEGORY_ACCOUNT_MAP if is_sales else CATEGORY_ACCOUNT_MAP
            if category in target_map:
                account = target_map[category]
                mapping_source = "category"
                review_payload = {"account": account, "iva_type": iva_type, "source": "category"}
            else:
                account = "705000" if is_sales else "629000"
                mapping_source = "fallback"

    if mapping_source == "category" and "NO_RULE" in issues:
        issues.remove("NO_RULE")

    confidence = {
        "rule_nif": 0.95,
        "rule_name": 0.90,
        "llm": 0.80,
        "category": 0.85,
        "fallback": 0.60,
    }.get(mapping_source, 0.60)

    penalty_issues = [
        code
        for code in issues
        if code not in {"NO_RULE", "DUP_NIF_NUMBER", "DUP_NIF_GROSS"}
    ]
    confidence -= 0.05 * len(penalty_issues)
    confidence -= nif_penalty
    confidence = max(0.1, min(confidence, 0.99))

    vat_groups: Dict[Decimal, Dict[str, Decimal]] = defaultdict(lambda: {"base": Decimal("0"), "vat": Decimal("0")})
    for item in invoice.get("lines", []):
        try:
            base_val = abs(utils.quantize_amount(item.get("amount", 0)))
            rate = Decimal(str(item.get("vat_rate", iva_type or 21)))
        except (InvalidOperation, ValueError, TypeError):
            continue
        vat_val = utils.quantize_amount(base_val * rate / Decimal(100))
        vat_groups[rate]["base"] += base_val
        vat_groups[rate]["vat"] += vat_val

    if not vat_groups:
        default_rate = Decimal(str(iva_type or 21.0))
        vat_groups[default_rate]["base"] = base_amount
        vat_groups[default_rate]["vat"] = vat_amount

    entry_lines: List[Dict[str, Any]] = []
    vat_account = "477000" if is_sales else "472000"
    expense_or_revenue_account = account
    if is_sales and not account.startswith("7"):
        expense_or_revenue_account = SALES_CATEGORY_ACCOUNT_MAP.get(category, "705000")
    if not is_sales and not account.startswith("6"):
        expense_or_revenue_account = CATEGORY_ACCOUNT_MAP.get(category, "600000")

    for rate, sums in vat_groups.items():
        base_val = sums["base"]
        vat_val = sums["vat"]
        if base_val <= 0:
            continue
        entry_lines.append(
            {
                "account": expense_or_revenue_account,
                "debit": utils.decimal_to_float(base_val) if (is_credit_note if is_sales else not is_credit_note) else 0.0,
                "credit": utils.decimal_to_float(base_val) if (not is_credit_note if is_sales else is_credit_note) else 0.0,
                "concept": f"{invoice_number or supplier.get('name')} ({float(rate):.2f}%)",
                "vat_rate": float(rate),
            }
        )
        if vat_val > 0:
            entry_lines.append(
                {
                    "account": vat_account,
                    "debit": utils.decimal_to_float(vat_val) if (is_credit_note if is_sales else not is_credit_note) else 0.0,
                    "credit": utils.decimal_to_float(vat_val) if (not is_credit_note if is_sales else is_credit_note) else 0.0,
                    "concept": ("IVA REPERCUTIDO" if is_sales else "IVA SOPORTADO") + f" {float(rate):.2f}%",
                    "vat_rate": float(rate),
                }
            )

    if withholding_amount > 0:
        withholding_float = utils.decimal_to_float(withholding_amount)
        if is_sales:
            account_ret = "470800"
            entry_lines.append(
                {
                    "account": account_ret,
                    "debit": withholding_float if not is_credit_note else 0.0,
                    "credit": 0.0 if not is_credit_note else withholding_float,
                    "concept": "Retención ventas",
                }
            )
            _append_issue(issues, "WITHHOLDING_SALES_UNSUPPORTED")
        else:
            account_ret = "475100"
            entry_lines.append(
                {
                    "account": account_ret,
                    "debit": 0.0 if not is_credit_note else withholding_float,
                    "credit": withholding_float if not is_credit_note else 0.0,
                    "concept": "Retención IRPF",
                }
            )

    if suplidos_amount > 0:
        # Se fuerza revisión, pero se mantiene el asiento balanceado con el total.
        _append_issue(issues, "SUPPLIDO_PRESENT")

    if is_sales:
        entry_lines.append(
            {
                "account": customer_account,
                "debit": utils.decimal_to_float(gross_amount) if not is_credit_note else 0.0,
                "credit": utils.decimal_to_float(gross_amount) if is_credit_note else 0.0,
                "concept": supplier.get("name"),
                "nif": supplier_nif,
            }
        )
    else:
        entry_lines.append(
            {
                "account": supplier_account,
                "debit": 0.0 if not is_credit_note else utils.decimal_to_float(gross_amount),
                "credit": utils.decimal_to_float(gross_amount) if not is_credit_note else 0.0,
                "concept": supplier.get("name"),
                "nif": supplier_nif,
            }
        )

    entry = {
        "doc_id": doc_id,
        "tenant": tenant,
        "journal": sales_journal if is_sales else default_journal,
        "date": invoice_date,
        "due_date": due_date,
        "invoice_number": invoice_number,
        "currency": currency,
        "supplier": supplier,
        "lines": entry_lines,
        "confidence_entry": round(confidence, 4),
        "duplicate_flag": duplicate_flag,
        "mapping_source": mapping_source,
        "metadata": metadata_info,
        "flow": flow,
        "doc_type": doc_type or ("sales_invoice" if is_sales else "invoice"),
    }

    entry_path = utils.BASE_DIR / "OUT" / "json" / f"{doc_id}.entry.json"
    utils.json_dump(entry, entry_path)

    try:
        utils.upsert_dedupe(doc_id, tenant, supplier_nif, invoice_number, invoice_date, gross_amount)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("No se pudo actualizar dedupe para %s: %s", doc_id, exc)

    return RuleEvaluation(
        entry=entry,
        confidence_entry=entry["confidence_entry"],
        issues=issues,
        review_payload=review_payload,
        duplicate_flag=duplicate_flag,
        llm_metadata=llm_metadata,
    )


def issues_to_messages(codes: List[str]) -> List[str]:
    return [ISSUE_MESSAGES.get(code, code) for code in codes]
