from __future__ import annotations

import json
from pathlib import Path

import pytest

from src import pipeline, utils, config, policies
from src.llm_providers import LLMProvider
from src import llm_suggest


class BlankLLM(LLMProvider):
    provider_name = "blank"

    def propose_mapping(self, invoice):  # noqa: D401
        return {
            "account": "",
            "iva_type": 21.0,
            "confidence_llm": 0.8,
            "issue_codes": [],
            "provider": self.provider_name,
            "model_used": "mini",
        }


class CaptureLLM(LLMProvider):
    provider_name = "capture"

    def __init__(self) -> None:
        super().__init__()
        self.last_threshold = None

    def propose_mapping(self, invoice):
        self.last_threshold = invoice.get("_llm_threshold_override")
        return {
            "account": "600000",
            "iva_type": 21.0,
            "confidence_llm": 0.9,
            "issue_codes": [],
            "provider": self.provider_name,
            "model_used": "mini",
        }


def _write_policy(base: Path, content: str) -> None:
    policy_path = base / "config" / "tenants" / "demo" / "policies.yaml"
    policy_path.parent.mkdir(parents=True, exist_ok=True)
    policy_path.write_text(content, encoding="utf-8")
    policies.clear_policy_cache()


def _normalized_payload(doc_id: str) -> dict:
    return {
        "doc_id": doc_id,
        "tenant": "demo",
        "supplier": {"name": "Proveedor", "nif": "B12345678", "vat": "", "country": "ES"},
        "invoice": {"number": "INV-1", "date": "2024-01-01", "due": "2024-01-10", "currency": "EUR"},
        "totals": {"base": 100.0, "vat": 21.0, "gross": 121.0},
        "lines": [
            {"desc": "Servicio", "qty": 1.0, "unit_price": 100.0, "vat_rate": 21.0, "amount": 100.0},
        ],
        "confidence_ocr": 0.98,
        "source": {"channel": "tests", "filename": "demo.pdf"},
        "metadata": {"category": "suministros"},
    }


def test_policy_autopost_disabled_forces_review(temp_certiva_env):
    base = temp_certiva_env["base"]
    _write_policy(base, "autopost_enabled: false\n")
    doc_id = "policy-doc"
    utils.insert_or_get_doc(doc_id, "deadbeef" * 8, "demo.pdf", "demo")

    config.set_llm_provider_override(BlankLLM())
    try:
        pipeline.process_normalized(doc_id, _normalized_payload(doc_id), 0.98)
    finally:
        config.set_llm_provider_override(None)
    row = utils.get_doc(doc_id)
    assert row["status"] == "REVIEW_PENDING"
    issues = json.loads(row["issues"]) if row["issues"] else []
    assert "POLICY_AUTOREVIEW" in issues


def test_policy_canary_sample_adds_issue(temp_certiva_env, monkeypatch):
    base = temp_certiva_env["base"]
    _write_policy(base, "autopost_enabled: true\ncanary_sample_pct: 1.0\n")
    monkeypatch.setattr("src.pipeline.random.random", lambda: 0.0)
    doc_id = "canary-doc"
    utils.insert_or_get_doc(doc_id, "cafebabe" * 8, "demo2.pdf", "demo")
    config.set_llm_provider_override(BlankLLM())
    try:
        pipeline.process_normalized(doc_id, _normalized_payload(doc_id), 0.98)
    finally:
        config.set_llm_provider_override(None)
    row = utils.get_doc(doc_id)
    assert row["status"] == "REVIEW_PENDING"
    issues = json.loads(row["issues"]) if row["issues"] else []
    assert "CANARY_SAMPLE" in issues


def test_policy_threshold_override_passed_to_llm(temp_certiva_env):
    base = temp_certiva_env["base"]
    _write_policy(base, "llm_premium_threshold_gross: 1234\n")
    capture = CaptureLLM()
    config.set_llm_provider_override(capture)
    payload = _normalized_payload("threshold-doc")
    result = llm_suggest.suggest_mapping(payload)
    config.set_llm_provider_override(None)
    assert capture.last_threshold == pytest.approx(1234.0)
    assert result["account"] == "600000"
