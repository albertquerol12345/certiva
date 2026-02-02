from pathlib import Path
import csv

import pytest
from reportlab.pdfgen import canvas

from src import pipeline, utils, config
from src.batch_writer import build_batch_outputs
from src.ocr_providers import DummyOCRProvider
from src.llm_providers import DummyLLMProvider
from src.exporter import A3_CSV_COLUMNS


def _reset_overrides():
    config.set_ocr_provider_override(None)
    config.set_llm_provider_override(None)


def test_dummy_providers_process_single_doc(temp_certiva_env, tmp_path):
    pdf_path = tmp_path / "sample.pdf"
    c = canvas.Canvas(str(pdf_path))
    c.drawString(100, 750, "Proveedor: Demo Supplier S.L.")
    c.drawString(100, 730, "NIF: B12345678")
    c.drawString(100, 710, "Factura: INV-001")
    c.drawString(100, 690, "Base imponible: 100.00")
    c.drawString(100, 670, "IVA: 21.00")
    c.drawString(100, 650, "Total: 121.00")
    c.save()

    config.set_ocr_provider_override(DummyOCRProvider())
    config.set_llm_provider_override(DummyLLMProvider())
    try:
        doc_id = pipeline.process_file(pdf_path, tenant="demo", force=True)
    finally:
        _reset_overrides()
    assert doc_id
    batch_dir = build_batch_outputs([doc_id], "demo", "lote_test")
    assert (batch_dir / "a3_asientos.csv").exists()
    assert (batch_dir / "incidencias.csv").exists()
    assert (batch_dir / "RESUMEN.txt").exists()


def test_dummy_pipeline_respects_a3_contract(temp_certiva_env, tmp_path):
    pdf_path = tmp_path / "sample2.pdf"
    c = canvas.Canvas(str(pdf_path))
    c.drawString(80, 760, "Proveedor: Consultoría Demo SL")
    c.drawString(80, 740, "NIF: B11223344")
    c.drawString(80, 720, "Factura: CD-001")
    c.drawString(80, 700, "Base imponible: 200.00")
    c.drawString(80, 680, "IVA: 42.00")
    c.drawString(80, 660, "Total: 242.00")
    c.save()

    config.set_ocr_provider_override(DummyOCRProvider())
    config.set_llm_provider_override(DummyLLMProvider())
    try:
        doc_id = pipeline.process_file(pdf_path, tenant="demo", force=True)
    finally:
        _reset_overrides()
    assert doc_id
    batch_dir = build_batch_outputs([doc_id], "demo", "lote_contract")
    csv_path = batch_dir / "a3_asientos.csv"
    assert csv_path.exists()
    with csv_path.open("r", encoding="utf-8") as fh:
        reader = list(csv.reader(fh))
    assert reader, "CSV vacío"
    assert reader[0] == A3_CSV_COLUMNS
    if len(reader) > 1:
        for row in reader[1:]:
            assert len(row) == len(A3_CSV_COLUMNS)


def test_provider_factories_respect_settings(monkeypatch):
    _reset_overrides()
    config._build_ocr_provider.cache_clear()
    config._build_llm_provider.cache_clear()
    monkeypatch.setattr(config.settings, "ocr_provider_type", "dummy")
    monkeypatch.setattr(config.settings, "llm_provider_type", "dummy")
    assert isinstance(config.get_ocr_provider(), DummyOCRProvider)
    assert isinstance(config.get_llm_provider(), DummyLLMProvider)


def test_openai_provider_requires_credentials(monkeypatch):
    config._build_llm_provider.cache_clear()
    monkeypatch.setattr(config.settings, "llm_provider_type", "openai")
    monkeypatch.setattr(config.settings, "openai_api_key", None)
    provider = config.get_llm_provider()
    assert isinstance(provider, DummyLLMProvider)
    config._build_llm_provider.cache_clear()


def test_azure_provider_requires_config(monkeypatch):
    config._build_ocr_provider.cache_clear()
    monkeypatch.setattr(config.settings, "ocr_provider_type", "azure")
    monkeypatch.setattr(config.settings, "azure_formrec_endpoint", None)
    monkeypatch.setattr(config.settings, "azure_formrec_key", None)
    provider = config.get_ocr_provider()
    assert isinstance(provider, DummyOCRProvider)
    assert getattr(provider, "fallback_issue_code", "") == "OCR_PROVIDER_FALLBACK"
    config._build_ocr_provider.cache_clear()
