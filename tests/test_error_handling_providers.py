from __future__ import annotations

import json
from pathlib import Path

import pytest
from reportlab.pdfgen import canvas

from src import pipeline, utils, config
from src.llm_providers import LLMProvider, DummyLLMProvider
from src.ocr_providers import OCRProvider, DummyOCRProvider, OCRResult


class BoomOCR(OCRProvider):
    provider_name = "boom_ocr"

    def analyze_document(self, file_path: Path, tenant: str) -> OCRResult:  # noqa: ARG002
        raise RuntimeError("429 Too Many Requests")


class BoomLLM(LLMProvider):
    provider_name = "boom_llm"

    def propose_mapping(self, invoice):  # noqa: D401
        raise RuntimeError("503 Service Unavailable")


@pytest.mark.usefixtures("temp_certiva_env")
def test_ocr_error_registers_issue(tmp_path):
    pdf_path = tmp_path / "fail.pdf"
    c = canvas.Canvas(str(pdf_path))
    c.drawString(100, 750, "Proveedor: Boom OCR")
    c.drawString(100, 730, "NIF: B00000001")
    c.drawString(100, 710, "Factura: BOOM-1")
    c.drawString(100, 690, "Base imponible: 10")
    c.drawString(100, 670, "IVA: 2.1")
    c.drawString(100, 650, "Total: 12.1")
    c.save()

    config.set_ocr_provider_override(BoomOCR())
    config.set_llm_provider_override(DummyLLMProvider())
    try:
        doc_id = pipeline.process_file(pdf_path, tenant="demo", force=True)
    finally:
        config.set_ocr_provider_override(None)
        config.set_llm_provider_override(None)
    assert doc_id
    row = utils.get_doc(doc_id)
    assert row is not None
    assert row["status"] == "ERROR"
    issues = json.loads(row["issues"]) if row["issues"] else []
    assert "OCR_TEMP_ERROR" in issues
    assert "PROVIDER_UNAVAILABLE" in issues
    assert row["error"] and "OCR error" in row["error"]


@pytest.mark.usefixtures("temp_certiva_env")
def test_llm_error_creates_incidence(tmp_path):
    pdf_path = tmp_path / "ok.pdf"
    # Creating a minimal PDF compatible with DummyOCRProvider
    c = canvas.Canvas(str(pdf_path))
    c.drawString(100, 750, "Proveedor: Error SL")
    c.drawString(100, 730, "NIF: B12345678")
    c.drawString(100, 710, "Factura: ERR-1")
    c.drawString(100, 690, "Base imponible: 10")
    c.drawString(100, 670, "IVA: 2.1")
    c.drawString(100, 650, "Total: 12.1")
    c.save()

    config.set_ocr_provider_override(DummyOCRProvider())
    config.set_llm_provider_override(BoomLLM())
    try:
        doc_id = pipeline.process_file(pdf_path, tenant="demo", force=True)
    finally:
        config.set_ocr_provider_override(None)
        config.set_llm_provider_override(None)
    assert doc_id
    row = utils.get_doc(doc_id)
    assert row is not None
    assert row["status"] in {"REVIEW_PENDING", "ERROR"}
    issues = json.loads(row["issues"]) if row["issues"] else []
    assert "LLM_TEMP_ERROR" in issues
    assert "PROVIDER_UNAVAILABLE" in issues
