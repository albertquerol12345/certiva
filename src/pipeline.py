import json
import logging
import random
import time
from pathlib import Path
from typing import Any, Dict, Optional

from .config import get_ocr_provider, settings
from .ocr_providers import OCRProvider, OCRRetryableError
from . import exporter, metadata, ocr_normalizer, policies, provider_health, rules_engine, utils

logger = logging.getLogger(__name__)


def compute_doc_confidence(ocr_conf: Optional[float], rules_conf: Optional[float], llm_conf: Optional[float]) -> float:
    weights = []
    values = []
    def _add(value: Optional[float], weight: float) -> None:
        if value is None:
            return
        try:
            val = float(value)
        except (TypeError, ValueError):
            return
        weights.append(weight)
        values.append(val)

    _add(ocr_conf, 0.35)
    _add(rules_conf, 0.40)
    _add(llm_conf, 0.25)
    if not weights:
        return 0.0
    score = sum(v * w for v, w in zip(values, weights)) / sum(weights)
    return round(max(0.0, min(score, 1.0)), 4)


def _should_review(issues: list[str], duplicate_flag: int, confidence_global: float) -> bool:
    if duplicate_flag:
        return True
    if confidence_global < settings.min_conf_entry:
        return True
    if any(code in rules_engine.HARD_ISSUES for code in issues):
        return True
    return any(code in rules_engine.REVIEW_ALWAYS for code in issues)


def _append_issue(issues: list[str], code: str) -> None:
    if code not in issues:
        issues.append(code)


def _issue_codes_from_exception(stage: str, exc: Exception) -> list[str]:
    msg = str(exc).lower()
    issue = "OCR_TEMP_ERROR" if stage == "ocr" else "LLM_TEMP_ERROR"
    codes = [issue]
    transient_tokens = ("429", "timeout", "temporarily", "503", "unavailable")
    if any(token in msg for token in transient_tokens):
        codes.append("PROVIDER_UNAVAILABLE")
    return codes


def _apply_policy_overrides(tenant: str, metadata_payload: Dict[str, Any], issues: list[str]) -> bool:
    policy = policies.get_tenant_policy(tenant)
    forced_review = False
    category = (metadata_payload.get("category") or "").lower()
    safe_categories = {value.lower() for value in policy.get("autopost_categories_safe") or []}
    if not policy.get("autopost_enabled", True):
        forced_review = True
        _append_issue(issues, "POLICY_AUTOREVIEW")
    elif safe_categories and category not in safe_categories:
        forced_review = True
        _append_issue(issues, "CATEGORY_REVIEW")
    if not forced_review:
        canary_pct = float(policy.get("canary_sample_pct") or 0.0)
        if canary_pct > 0.0 and random.random() < canary_pct:
            forced_review = True
            _append_issue(issues, "CANARY_SAMPLE")
    return forced_review


def _enqueue_review(doc_id: str, issues: list[str], payload: Optional[Dict[str, Any]], tenant: Optional[str] = None) -> None:
    reason_list = rules_engine.issues_to_messages(issues) or ["Revisión manual"]
    reason = "; ".join(reason_list)
    utils.add_review_item(
        doc_id,
        reason,
        {"issues": issues, "suggestion": payload} if payload else {"issues": issues},
        tenant=tenant,
    )
    logger.info("Doc %s en cola de revisión: %s", doc_id, reason)


def process_normalized(
    doc_id: str,
    normalized: Dict[str, Any],
    ocr_conf: Optional[float] = None,
    skip_stage_update: bool = False,
) -> Optional[str]:
    rules_start = time.perf_counter()
    try:
        evaluation = rules_engine.generate_entry(doc_id, normalized)
        if not skip_stage_update:
            utils.record_stage_timestamp(doc_id, "validated")
    except Exception as exc:  # pragma: no cover
        logger.exception("Rules failed for %s", doc_id)
        utils.store_error(doc_id, f"Rules error: {exc}")
        return doc_id
    rules_time_ms = int((time.perf_counter() - rules_start) * 1000)

    entry = evaluation.entry
    entry_conf = evaluation.confidence_entry
    issues = evaluation.issues
    duplicate_flag = evaluation.duplicate_flag
    ocr_conf_value = ocr_conf if ocr_conf is not None else 1.0
    llm_meta = evaluation.llm_metadata or {}
    llm_conf = llm_meta.get("confidence_llm")
    confidence_global = compute_doc_confidence(ocr_conf_value, entry_conf, llm_conf)
    entry["confidence_global"] = confidence_global
    issues_json = json.dumps(issues, ensure_ascii=False)
    metadata_payload = normalized.get("metadata") or {}
    doc_type = metadata_payload.get("doc_type")

    low_confidence = confidence_global < settings.confidence_min_ok
    if low_confidence and "LOW_CONFIDENCE" not in issues:
        issues.append("LOW_CONFIDENCE")
        issues_json = json.dumps(issues, ensure_ascii=False)

    needs_review = _should_review(issues, duplicate_flag, confidence_global)

    tenant = normalized.get("tenant", settings.default_tenant)
    llm_meta = evaluation.llm_metadata or {}
    llm_provider = llm_meta.get("provider")
    llm_time_ms = llm_meta.get("duration_ms")
    llm_model_used = llm_meta.get("model_used")
    llm_tokens_in = llm_meta.get("tokens_in")
    llm_tokens_out = llm_meta.get("tokens_out")
    llm_cost_eur = llm_meta.get("cost_eur")

    policy_forced_review = _apply_policy_overrides(tenant, metadata_payload, issues)
    if policy_forced_review:
        needs_review = True
        issues_json = json.dumps(issues, ensure_ascii=False)

    if needs_review:
        _enqueue_review(doc_id, issues, evaluation.review_payload)
        utils.update_doc_status(
            doc_id,
            "REVIEW_PENDING",
            entry_conf=entry_conf,
            ocr_conf=ocr_conf_value,
            global_conf=confidence_global,
            doc_type=doc_type,
            duplicate_flag=duplicate_flag,
            issues=issues_json,
            llm_provider=llm_provider,
            llm_time_ms=llm_time_ms,
            rules_time_ms=rules_time_ms,
            llm_model_used=llm_model_used,
            llm_tokens_in=llm_tokens_in,
            llm_tokens_out=llm_tokens_out,
            llm_cost_eur=llm_cost_eur,
        )
        return doc_id

    utils.remove_review_item(doc_id)
    utils.update_doc_status(
        doc_id,
        "ENTRY_READY",
        entry_conf=entry_conf,
        ocr_conf=ocr_conf_value,
        global_conf=confidence_global,
        doc_type=doc_type,
        duplicate_flag=duplicate_flag,
        issues=issues_json,
        llm_provider=llm_provider,
        llm_time_ms=llm_time_ms,
        rules_time_ms=rules_time_ms,
        llm_model_used=llm_model_used,
        llm_tokens_in=llm_tokens_in,
        llm_tokens_out=llm_tokens_out,
        llm_cost_eur=llm_cost_eur,
    )
    utils.record_stage_timestamp(doc_id, "entry")

    try:
        exporter.export_entry(doc_id, entry)
    except Exception as exc:  # pragma: no cover
        logger.exception("Export failed for %s", doc_id)
        utils.store_error(doc_id, f"Export error: {exc}")
        return doc_id

    return doc_id


def process_file(
    file_path: Path,
    tenant: Optional[str] = None,
    force: bool = False,
    ocr_provider: Optional[OCRProvider] = None,
) -> Optional[str]:
    utils.configure_logging()
    total_start = time.perf_counter()
    tenant = tenant or settings.default_tenant
    if not file_path.exists():
        logger.warning("File %s disappeared before processing", file_path)
        return None
    if file_path.suffix.lower() not in utils.SUPPORTED_EXTENSIONS:
        logger.info("Skipping unsupported file %s", file_path)
        return None

    sha256 = utils.compute_sha256(file_path)
    doc_id = sha256
    existing = utils.get_doc(doc_id)
    if existing and not force and existing["status"] not in {"ERROR", "REVIEW_PENDING"}:
        logger.info("Doc %s already processed with status %s", doc_id, existing["status"])
        return doc_id

    file_meta = metadata.get_metadata_for_file(file_path.name)
    if file_meta and file_meta.get("tenant"):
        tenant = file_meta.get("tenant") or tenant

    utils.insert_or_get_doc(doc_id, sha256, file_path.name, tenant)
    page_count = utils.compute_pdf_page_count(file_path)
    if page_count is not None:
        utils.update_doc_metadata(doc_id, page_count=page_count)
        if page_count == 0:
            utils.persist_issues(doc_id, ["PAGECOUNT_ZERO"])
    utils.record_stage_timestamp(doc_id, "received")
    utils.update_doc_status(doc_id, "RECEIVED")

    provider_instance = ocr_provider or get_ocr_provider()
    fallback_issue = getattr(provider_instance, "fallback_issue_code", "")
    if provider_health.is_degraded("ocr", provider_instance.provider_name):
        issues = ["PROVIDER_DEGRADED"]
        utils.persist_issues(doc_id, issues)
        utils.store_error(doc_id, "OCR provider degradado (circuit breaker)")
        return doc_id
    try:
        ocr_start = time.perf_counter()
        normalized = ocr_normalizer.extract_invoice(
            doc_id,
            file_path,
            tenant,
            metadata=file_meta,
            provider=provider_instance,
        )
        if fallback_issue:
            metadata_payload = dict(normalized.metadata or {})
            forced = list(metadata_payload.get("forced_issues") or [])
            if fallback_issue not in forced:
                forced.append(fallback_issue)
            metadata_payload["forced_issues"] = forced
            normalized.metadata = metadata_payload
        ocr_time_ms = int((time.perf_counter() - ocr_start) * 1000)
        utils.record_stage_timestamp(doc_id, "ocr")
        provider_health.record_success("ocr", provider_instance.provider_name)
        utils.update_doc_status(
            doc_id,
            "OCR_OK",
            ocr_conf=normalized.confidence_ocr,
            ocr_provider=provider_instance.provider_name,
            ocr_time_ms=ocr_time_ms,
        )
    except OCRRetryableError as exc:
        logger.warning("OCR temporal para %s: %s", doc_id, exc)
        degraded = provider_health.record_failure("ocr", provider_instance.provider_name)
        issue_codes = _issue_codes_from_exception("ocr", exc)
        if degraded and "PROVIDER_DEGRADED" not in issue_codes:
            issue_codes.append("PROVIDER_DEGRADED")
        utils.persist_issues(doc_id, issue_codes)
        utils.store_error(doc_id, f"OCR temporal: {exc}")
        utils.enqueue_ocr_retry(doc_id, tenant, str(file_path), str(exc))
        return doc_id
    except Exception as exc:
        logger.exception("OCR failed for %s", doc_id)
        degraded = provider_health.record_failure("ocr", provider_instance.provider_name)
        issue_codes = _issue_codes_from_exception("ocr", exc)
        if degraded and "PROVIDER_DEGRADED" not in issue_codes:
            issue_codes.append("PROVIDER_DEGRADED")
        utils.persist_issues(doc_id, issue_codes)
        utils.store_error(doc_id, f"OCR error: {exc}")
        return doc_id

    result = process_normalized(doc_id, normalized.dict(), normalized.confidence_ocr)
    total_time_ms = int((time.perf_counter() - total_start) * 1000)
    if result:
        utils.update_doc_metadata(result, total_time_ms=total_time_ms)
    return result


def reprocess_from_json(doc_id: str) -> Optional[str]:
    json_path = utils.BASE_DIR / "OUT" / "json" / f"{doc_id}.json"
    if not json_path.exists():
        logger.warning("No normalized JSON for %s", doc_id)
        return None
    normalized = utils.read_json(json_path)
    doc = utils.get_doc(doc_id)
    ocr_conf = doc["ocr_conf"] if doc else None
    return process_normalized(doc_id, normalized, ocr_conf, skip_stage_update=False)
