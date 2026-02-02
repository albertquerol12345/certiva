from __future__ import annotations

from src import config
from src.ocr_providers import DummyOCRProvider


def test_get_ocr_provider_returns_dummy_when_misconfigured(monkeypatch):
    config._build_ocr_provider.cache_clear()
    monkeypatch.setattr(config.settings, "ocr_provider_type", "azure")
    monkeypatch.setattr(config.settings, "azure_formrec_endpoint", None)
    monkeypatch.setattr(config.settings, "azure_formrec_key", None)
    monkeypatch.setattr(config.settings, "azure_formrec_model_id", None)
    provider = config.get_ocr_provider()
    assert isinstance(provider, DummyOCRProvider)
    assert getattr(provider, "fallback_issue_code", "") == "OCR_PROVIDER_FALLBACK"
    config._build_ocr_provider.cache_clear()


def test_get_ocr_provider_respects_dummy(monkeypatch):
    config._build_ocr_provider.cache_clear()
    monkeypatch.setattr(config.settings, "ocr_provider_type", "dummy")
    provider = config.get_ocr_provider()
    assert isinstance(provider, DummyOCRProvider)
    assert not getattr(provider, "fallback_issue_code", "")
    config._build_ocr_provider.cache_clear()
