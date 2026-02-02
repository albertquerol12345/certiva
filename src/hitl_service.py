"""Reusable helpers for HITL actions (shared by CLI and web)."""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import getpass

from . import pipeline, utils
from .config import settings
from . import rules_engine


@dataclass
class ReviewDoc:
    doc_id: str
    supplier: Dict[str, Any]
    invoice: Dict[str, Any]
    totals: Dict[str, Any]
    tenant: str
    doc_type: str
    reconciled_amount: float
    reconciled_pct: float
    issues: List[str]
    issues_text: List[str]
    confidences: Dict[str, Any]
    suggestion: Dict[str, Any]
    metadata: Dict[str, Any]


def _load_json(doc_id: str) -> Dict[str, Any]:
    return utils.read_json(utils.BASE_DIR / "OUT" / "json" / f"{doc_id}.json")


def _load_entry(doc_id: str) -> Dict[str, Any]:
    return utils.read_json(utils.BASE_DIR / "OUT" / "json" / f"{doc_id}.entry.json")


def _ensure_tenant(doc_id: str, tenant: Optional[str]) -> None:
    if not tenant:
        return
    doc = utils.get_doc(doc_id)
    doc_tenant = None
    if doc:
        try:
            doc_tenant = doc["tenant"]
        except Exception:
            doc_tenant = doc.get("tenant") if hasattr(doc, "get") else None
    if not doc or doc_tenant != tenant:
        raise ValueError("Documento fuera del tenant actual")


def _parse_queue_payload(row: Any) -> Dict[str, Any]:
    try:
        return json.loads(row["suggested"] or "{}")
    except (KeyError, json.JSONDecodeError, TypeError):
        return {}


def _issues_from_row(row: Any) -> List[str]:
    payload = _parse_queue_payload(row)
    if isinstance(payload, dict) and isinstance(payload.get("issues"), list):
        return payload["issues"]
    reason_val = None
    try:
        reason_val = row["reason"]  # sqlite3.Row soporta acceso por clave
    except Exception:
        if hasattr(row, "get"):
            reason_val = row.get("reason")
    if reason_val:
        return [part.strip() for part in str(reason_val).split(";") if part.strip()]
    return []


def _suggestion_from_row(row: Any) -> Dict[str, Any]:
    payload = _parse_queue_payload(row)
    suggestion = payload.get("suggestion") if isinstance(payload, dict) else None
    return suggestion if isinstance(suggestion, dict) else {}


def fetch_review_items(
    limit: Optional[int] = None,
    offset: int = 0,
    doc_type_prefix: Optional[str] = None,
    tenant: Optional[str] = None,
    issue_filter: Optional[str] = None,
    sort_by_issues: bool = True,
) -> List[ReviewDoc]:
    queue = utils.fetch_review_queue(limit=limit, offset=offset, tenant=tenant)
    docs: List[ReviewDoc] = []
    issue_filter_norm = (issue_filter or "").strip().upper()
    for row in queue:
        doc_id = row["doc_id"]
        normalized = _load_json(doc_id)
        issues = _issues_from_row(row)
        suggestion = _suggestion_from_row(row)
        doc_row = utils.get_doc(doc_id)
        metadata_payload = normalized.get("metadata", {})
        doc_type = (
            metadata_payload.get("doc_type")
            or (doc_row["doc_type"] if doc_row and doc_row["doc_type"] else None)
            or "invoice"
        )
        if doc_type_prefix and not doc_type.lower().startswith(doc_type_prefix.lower()):
            continue
        if issue_filter_norm:
            issue_upper = [code.upper() for code in issues]
            if issue_filter_norm not in issue_upper:
                continue
        confidences = {
            "ocr": doc_row["ocr_conf"] if doc_row else None,
            "entry": doc_row["entry_conf"] if doc_row else None,
            "global": doc_row["global_conf"] if doc_row else None,
        }
        if tenant and doc_row and doc_row["tenant"] != tenant:
            continue
        docs.append(
            ReviewDoc(
                doc_id=doc_id,
                supplier=normalized.get("supplier", {}),
                invoice=normalized.get("invoice", {}),
                totals=normalized.get("totals", {}),
                tenant=normalized.get("tenant", settings.default_tenant),
                doc_type=doc_type,
                reconciled_amount=doc_row["reconciled_amount"] if doc_row else 0.0,
                reconciled_pct=doc_row["reconciled_pct"] if doc_row else 0.0,
                issues=issues,
                issues_text=rules_engine.issues_to_messages(issues),
                confidences=confidences,
                suggestion=suggestion,
                metadata=metadata_payload,
            )
        )
    if sort_by_issues:
        docs.sort(key=lambda d: (len(d.issues) if isinstance(d.issues, list) else 0, -(d.confidences.get("global") or 0)), reverse=True)
    return docs


def summarize_review_queue(limit_per_issue: int = 5, tenant: Optional[str] = None) -> Dict[str, Any]:
    """Agrupa la cola HITL por código de issue y devuelve una muestra de doc_ids."""
    queue = utils.fetch_review_queue(tenant=tenant)
    counter: Dict[str, int] = {}
    samples: Dict[str, List[str]] = {}
    for row in queue:
        issues = _issues_from_row(row)
        doc_id = row["doc_id"]
        for code in issues or ["NO_ISSUE"]:
            counter[code] = counter.get(code, 0) + 1
            bucket = samples.setdefault(code, [])
            if len(bucket) < limit_per_issue:
                bucket.append(doc_id)
    summary = sorted(counter.items(), key=lambda kv: kv[1], reverse=True)
    return {
        "counts": summary,
        "samples": samples,
        "total": len(queue),
    }


def get_review_detail(doc_id: str, tenant: Optional[str] = None) -> Dict[str, Any]:
    _ensure_tenant(doc_id, tenant)
    normalized = _load_json(doc_id)
    entry = _load_entry(doc_id)
    doc_row = utils.get_doc(doc_id)
    if tenant and doc_row and doc_row["tenant"] != tenant:
        raise ValueError("Documento fuera del tenant actual")
    queue_rows = [row for row in utils.fetch_review_queue(tenant=tenant) if row["doc_id"] == doc_id]
    issues = _issues_from_row(queue_rows[0]) if queue_rows else []
    suggestion = _suggestion_from_row(queue_rows[0]) if queue_rows else {}
    metadata_payload = normalized.get("metadata", {})
    doc_type = metadata_payload.get("doc_type") or (doc_row["doc_type"] if doc_row else None) or "invoice"
    matches = utils.fetch_matches_for_doc(doc_id)
    reconciliation = {
        "amount": doc_row["reconciled_amount"] if doc_row else 0.0,
        "pct": doc_row["reconciled_pct"] if doc_row else 0.0,
        "matches": matches,
    }

    return {
        "doc_id": doc_id,
        "normalized": normalized,
        "entry": entry,
        "issues": issues,
        "issues_text": rules_engine.issues_to_messages(issues),
        "suggestion": suggestion,
        "doc_type": doc_type,
        "confidences": {
            "ocr": doc_row["ocr_conf"] if doc_row else None,
            "entry": doc_row["entry_conf"] if doc_row else None,
            "global": doc_row["global_conf"] if doc_row else None,
        },
        "reconciliation": reconciliation,
    }


def _append_rule(normalized: Dict[str, Any], account: str, iva_type: float, actor: str, notes: str) -> None:
    supplier = normalized.get("supplier", {})
    row = {
        "tenant": normalized.get("tenant", settings.default_tenant),
        "supplier_name": supplier.get("name", ""),
        "nif": supplier.get("nif", ""),
        "account": account,
        "iva_type": iva_type,
        "notes": notes,
    }
    utils.append_vendor_rule(utils.BASE_DIR / "rules" / "vendor_map.csv", row)
    utils.add_audit(normalized.get("doc_id"), "LEARN_RULE", actor, None, row)


def _apply_rule_to_similar(nif: str, exclude_doc: str, actor: str) -> None:
    if not nif:
        return
    queue = utils.fetch_review_queue()
    for row in queue:
        doc_id = row["doc_id"]
        if doc_id == exclude_doc:
            continue
        normalized = _load_json(doc_id)
        other_nif = (normalized.get("supplier", {}).get("nif") or "").upper()
        if other_nif and other_nif == nif.upper():
            pipeline.reprocess_from_json(doc_id)
            utils.add_audit(doc_id, "HITL_AUTO_REPROCESS", actor, None, {"reason": "rule_applied"})


def accept_doc(
    doc_id: str,
    actor: Optional[str] = None,
    learn_rule: bool = False,
    apply_to_similar: bool = False,
    suggestion: Optional[Dict[str, Any]] = None,
    tenant: Optional[str] = None,
) -> None:
    actor = actor or getpass.getuser()
    _ensure_tenant(doc_id, tenant)
    normalized = _load_json(doc_id)
    entry = _load_entry(doc_id)
    issues = get_review_detail(doc_id, tenant=tenant)["issues"]
    if learn_rule and "NO_RULE" in issues:
        default_account = suggestion.get("account") if suggestion else None
        default_iva = suggestion.get("iva_type") if suggestion else None
        first_line = entry.get("lines", [{}])[0]
        account = default_account or first_line.get("account", "600000")
        iva_rate = float(default_iva or normalized.get("lines", [{}])[0].get("vat_rate", 21))
        _append_rule(normalized, account, iva_rate, actor, "aprendido HITL")
        if apply_to_similar:
            supplier_nif = (normalized.get("supplier", {}).get("nif") or "").upper()
            _apply_rule_to_similar(supplier_nif, doc_id, actor)
    pipeline.reprocess_from_json(doc_id)
    utils.add_audit(doc_id, "HITL_ACCEPT", actor, None, {"issues": issues})


def edit_doc(
    doc_id: str,
    account: str,
    iva_rate: float,
    actor: Optional[str] = None,
    apply_to_similar: bool = False,
    tenant: Optional[str] = None,
) -> None:
    actor = actor or getpass.getuser()
    _ensure_tenant(doc_id, tenant)
    entry = _load_entry(doc_id)
    normalized = _load_json(doc_id)
    if not entry.get("lines"):
        raise ValueError("No hay líneas para editar")
    entry["lines"][0]["account"] = account
    normalized["lines"][0]["vat_rate"] = iva_rate
    utils.json_dump(entry, utils.BASE_DIR / "OUT" / "json" / f"{doc_id}.entry.json")
    utils.json_dump(normalized, utils.BASE_DIR / "OUT" / "json" / f"{doc_id}.json")
    _append_rule(normalized, account, iva_rate, actor, "editado HITL")
    supplier_nif = (normalized.get("supplier", {}).get("nif") or "").upper()
    if apply_to_similar:
        _apply_rule_to_similar(supplier_nif, doc_id, actor)
    pipeline.reprocess_from_json(doc_id)
    utils.add_audit(doc_id, "HITL_EDIT", actor, None, {"account": account, "iva": iva_rate})


def mark_duplicate(doc_id: str, actor: Optional[str] = None, tenant: Optional[str] = None) -> None:
    actor = actor or getpass.getuser()
    _ensure_tenant(doc_id, tenant)
    utils.update_doc_status(doc_id, "ERROR", duplicate_flag=1)
    utils.remove_review_item(doc_id)
    utils.add_audit(doc_id, "HITL_DUPLICATE", actor, None, {"duplicate": True})


def reprocess_doc(doc_id: str, actor: Optional[str] = None, tenant: Optional[str] = None) -> None:
    actor = actor or getpass.getuser()
    _ensure_tenant(doc_id, tenant)
    pipeline.reprocess_from_json(doc_id)
    utils.add_audit(doc_id, "HITL_REPROCESS", actor, None, None)


def clear_reconciliation(
    doc_id: str,
    actor: Optional[str] = None,
    include_manual: bool = False,
    tenant: Optional[str] = None,
) -> None:
    actor = actor or getpass.getuser()
    _ensure_tenant(doc_id, tenant)
    utils.clear_matches(doc_id, include_manual=include_manual)
    utils.add_audit(doc_id, "HITL_CLEAR_RECON", actor, None, {"include_manual": include_manual})
