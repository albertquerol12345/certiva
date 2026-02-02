import logging
import re
import time
from typing import Any, Dict, Optional

from .config import get_llm_provider, settings
from . import llm_debug, policies, provider_health

logger = logging.getLogger(__name__)


def suggest_missing_fields(ocr_text: str) -> Dict[str, Dict[str, Any]]:
    """Attempt to recover invoice.number or supplier.nif using regex + LLM assistance."""
    suggestions: Dict[str, Dict[str, Any]] = {}

    invoice_match = re.search(r"(?:Factura|Invoice|Fact\.? No\.?|Número)\s*[:#]?\s*([A-Z0-9\-/]{4,})", ocr_text, re.IGNORECASE)
    if invoice_match:
        suggestions["invoice.number"] = {
            "value": invoice_match.group(1).strip(),
            "confidence_llm": 0.65,
            "source": "regex",
        }

    nif_match = re.search(r"(?:NIF|VAT|CIF)\s*[:#]?\s*([A-Z0-9]{8,12})", ocr_text, re.IGNORECASE)
    if nif_match:
        suggestions["supplier.nif"] = {
            "value": nif_match.group(1).strip(),
            "confidence_llm": 0.60,
            "source": "regex",
        }

    missing = []
    if "invoice.number" not in suggestions:
        missing.append("invoice.number")
    if "supplier.nif" not in suggestions:
        missing.append("supplier.nif")

    # Placeholder: en futura versión se delegará al LLMProvider si es necesario.
    return suggestions


def _llm_issue_codes(exc: Exception) -> list[str]:
    message = str(exc).lower()
    codes = ["LLM_TEMP_ERROR"]
    if any(token in message for token in ("429", "timeout", "503", "unavailable")):
        codes.append("PROVIDER_UNAVAILABLE")
    return codes


def _degraded_mapping(provider_name: str) -> Dict[str, Any]:
    return {
        "account": "629000",
        "iva_type": 21.0,
        "confidence_llm": 0.0,
        "rationale": "Proveedor degradado",
        "issue_codes": ["PROVIDER_DEGRADED"],
        "provider": provider_name,
        "model_used": provider_name,
        "duration_ms": 0,
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "cost_eur": 0.0,
    }


def suggest_mapping(normalized_json: Dict[str, Any]) -> Dict[str, Any]:
    """Delegates mapping suggestion to the configured LLM provider."""
    provider = get_llm_provider()
    tenant = normalized_json.get("tenant", settings.default_tenant)
    policy = policies.get_tenant_policy(tenant)
    threshold_override_value: Optional[float] = None
    threshold_override = policy.get("llm_premium_threshold_gross")
    if threshold_override is not None:
        try:
            threshold_override_value = float(threshold_override)
            normalized_json["_llm_threshold_override"] = threshold_override_value
        except (TypeError, ValueError):
            threshold_override_value = None
    if provider_health.is_degraded("llm", provider.provider_name):
        normalized_json.pop("_llm_threshold_override", None)
        return _degraded_mapping(provider.provider_name)

    start = time.perf_counter()
    try:
        mapping = provider.propose_mapping(normalized_json) or {}
        provider_health.record_success("llm", provider.provider_name)
    except Exception as exc:
        logger.warning("LLM provider error para %s: %s", tenant, exc)
        duration_ms = int((time.perf_counter() - start) * 1000)
        normalized_json.pop("_llm_threshold_override", None)
        degraded = provider_health.record_failure("llm", provider.provider_name)
        issues = _llm_issue_codes(exc)
        if degraded and "PROVIDER_DEGRADED" not in issues:
            issues.append("PROVIDER_DEGRADED")
        return {
            "account": "629000",
            "iva_type": 21.0,
            "confidence_llm": 0.0,
            "rationale": str(exc),
            "issue_codes": issues,
            "provider": getattr(provider, "provider_name", "llm"),
            "model_used": getattr(provider, "provider_name", "llm"),
            "duration_ms": duration_ms,
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "cost_eur": 0.0,
        }
    duration_ms = int((time.perf_counter() - start) * 1000)
    normalized_json.pop("_llm_threshold_override", None)
    mapping.setdefault("account", "629000")
    mapping.setdefault("iva_type", 21.0)
    mapping.setdefault("confidence_llm", 0.5)
    mapping.setdefault("rationale", "LLM fallback")
    mapping.setdefault("issue_codes", [])
    mapping.setdefault("model_used", provider.provider_name)
    mapping.setdefault("provider", provider.provider_name)
    mapping.setdefault("duration_ms", duration_ms)
    mapping.setdefault("prompt_tokens", 0)
    mapping.setdefault("completion_tokens", 0)
    mapping.setdefault("cost_eur", 0.0)

    # Second opinion: si hay override premium/riesgo, ejecuta mini una segunda vez y compara
    second_opinion_enabled = bool(policy.get("second_opinion_enabled", True))
    if second_opinion_enabled:
        try:
            so_mapping = provider.propose_mapping(normalized_json) or {}
            acc_diff = so_mapping.get("account") != mapping.get("account")
            iva_diff = float(so_mapping.get("iva_type", 0)) != float(mapping.get("iva_type", 0))
            if acc_diff or iva_diff:
                mapping.setdefault("issue_codes", []).append("SECOND_OPINION_DISAGREE")
                mapping["rationale"] += " | Second opinion difiere"
        except Exception as exc:  # pragma: no cover - defensivo
            logger.info("Second opinion LLM falló: %s", exc)
    if llm_debug.is_enabled():
        debug_payload = provider.consume_debug_payload() or {}
        doc_id = normalized_json.get("doc_id") or normalized_json.get("sha256")
        tenant = normalized_json.get("tenant", settings.default_tenant)
        if doc_id and debug_payload:
            metadata = {
                "tenant": tenant,
                "duration_ms": mapping.get("duration_ms"),
                "provider": mapping.get("provider"),
                "model_used": mapping.get("model_used"),
                "strategy": getattr(settings, "llm_strategy", "unknown"),
                "threshold": threshold_override_value or getattr(provider, "threshold_gross", None),
            }
            debug_payload.setdefault("parsed_result", mapping)
            debug_payload["metadata"] = metadata
            llm_debug.record(str(doc_id), tenant, debug_payload)
    return mapping
