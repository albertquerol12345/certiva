from __future__ import annotations

from src import pipeline, utils, rules_engine
from src.rules_engine import RuleEvaluation
from src.config import settings


def _make_evaluation(doc_id: str, entry_conf: float, issues=None, llm_conf: float = 0.8) -> RuleEvaluation:
    entry = {
        "doc_id": doc_id,
        "tenant": "demo",
        "journal": "COMPRAS",
        "lines": [],
        "confidence_entry": entry_conf,
        "mapping_source": "llm",
    }
    return RuleEvaluation(
        entry=entry,
        confidence_entry=entry_conf,
        issues=issues or [],
        review_payload=None,
        duplicate_flag=0,
        llm_metadata={"confidence_llm": llm_conf},
    )


def test_confidence_gating_sends_low_confidence_to_review(temp_certiva_env, monkeypatch):
    utils.insert_or_get_doc("doc-high", "doc-high", "doc.pdf", "demo")

    def fake_eval_high(*args, **kwargs):
        return _make_evaluation("doc-high", 0.95, [], 0.9)

    monkeypatch.setattr(rules_engine, "generate_entry", fake_eval_high)
    normalized = {
        "doc_id": "doc-high",
        "tenant": "demo",
        "supplier": {"name": "Proveedor", "nif": "B12345678"},
        "invoice": {"number": "INV1", "date": "2025-01-01", "due": "2025-01-30"},
        "totals": {"base": 100.0, "vat": 21.0, "gross": 121.0},
        "lines": [{"desc": "Servicio", "amount": 100.0, "vat_rate": 21.0}],
        "metadata": {},
    }
    pipeline.process_normalized("doc-high", normalized, ocr_conf=0.95)
    row = utils.get_doc("doc-high")
    assert row["status"] in {"ENTRY_READY", "POSTED"}

    utils.insert_or_get_doc("doc-low", "doc-low", "doc.pdf", "demo")

    def fake_eval_low(*args, **kwargs):
        return _make_evaluation("doc-low", 0.6, [], 0.4)

    monkeypatch.setattr(rules_engine, "generate_entry", fake_eval_low)
    normalized["doc_id"] = "doc-low"
    pipeline.process_normalized("doc-low", normalized, ocr_conf=0.5)
    row_low = utils.get_doc("doc-low")
    assert row_low["status"] == "REVIEW_PENDING"
    issues = row_low["issues"] or ""
    assert "LOW_CONFIDENCE" in issues
