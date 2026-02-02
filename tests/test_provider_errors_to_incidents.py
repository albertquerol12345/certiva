from __future__ import annotations

import json
from pathlib import Path

import pytest
from reportlab.pdfgen import canvas

from src import config, pipeline, provider_health, utils
from src.llm_providers import LLMProvider
from src.ocr_providers import OCRProvider, OCRResult


class FlakyOCR(OCRProvider):
    provider_name = "flaky_ocr"

    def __init__(self):
        self.calls = 0

    def analyze_document(self, file_path: Path, tenant: str) -> OCRResult:  # noqa: ARG002
        self.calls += 1
        raise RuntimeError("429 Too Many Requests")


class FlakyLLM(LLMProvider):
    provider_name = "flaky_llm"

    def __init__(self):
        super().__init__()
        self.calls = 0

    def propose_mapping(self, invoice):  # noqa: D401
        self.calls += 1
        raise RuntimeError("503 Service Unavailable")


@pytest.mark.usefixtures("temp_certiva_env")
def test_ocr_circuit_breaker_marks_provider_degraded(monkeypatch, tmp_path):
    monkeypatch.setattr(config.settings, "ocr_breaker_threshold", 1, raising=False)
    provider_health.reset_all()
    pdf = tmp_path / "ocr.pdf"
    c = canvas.Canvas(str(pdf))
    c.drawString(100, 750, "Proveedor: Fail OCR")
    c.drawString(100, 730, "NIF: B11111111")
    c.drawString(100, 710, "Factura: FAIL-1")
    c.drawString(100, 690, "Base imponible: 10")
    c.drawString(100, 670, "IVA: 2.1")
    c.drawString(100, 650, "Total: 12.1")
    c.save()

    flaky = FlakyOCR()
    config.set_ocr_provider_override(flaky)
    config.set_llm_provider_override(None)
    try:
        doc_id = pipeline.process_file(pdf, tenant="demo", force=True)
        row = utils.get_doc(doc_id)
        issues = json.loads(row["issues"]) if row["issues"] else []
        assert "OCR_TEMP_ERROR" in issues
        assert "PROVIDER_DEGRADED" in issues
        provider_health_state = provider_health.snapshot()
        assert provider_health_state
        # Next document should be short-circuited without calling the provider
        doc_id2 = pipeline.process_file(pdf, tenant="demo", force=True)
        row2 = utils.get_doc(doc_id2)
        issues2 = json.loads(row2["issues"]) if row2["issues"] else []
        assert issues2 == ["PROVIDER_DEGRADED"]
    finally:
        config.set_ocr_provider_override(None)


@pytest.mark.usefixtures("temp_certiva_env")
def test_llm_circuit_breaker(monkeypatch, tmp_path):
    monkeypatch.setattr(config.settings, "llm_breaker_threshold", 1, raising=False)
    provider_health.reset_all()
    pdf = tmp_path / "llm.pdf"
    c = canvas.Canvas(str(pdf))
    c.drawString(100, 750, "Proveedor: Fail LLM")
    c.drawString(100, 730, "NIF: B99999999")
    c.drawString(100, 710, "Factura: LLM-1")
    c.drawString(100, 690, "Base imponible: 20")
    c.drawString(100, 670, "IVA: 4.2")
    c.drawString(100, 650, "Total: 24.2")
    c.save()

    config.set_ocr_provider_override(None)
    config.set_llm_provider_override(FlakyLLM())
    try:
        doc_id = pipeline.process_file(pdf, tenant="demo", force=True)
        row = utils.get_doc(doc_id)
        issues = json.loads(row["issues"]) if row["issues"] else []
        assert "LLM_TEMP_ERROR" in issues
        assert "PROVIDER_UNAVAILABLE" in issues
        # Next attempt should short-circuit
        doc_id2 = pipeline.process_file(pdf, tenant="demo", force=True)
        row2 = utils.get_doc(doc_id2)
        issues2 = json.loads(row2["issues"]) if row2["issues"] else []
        assert "PROVIDER_DEGRADED" in issues2
    finally:
        config.set_llm_provider_override(None)
