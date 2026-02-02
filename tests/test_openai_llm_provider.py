import pytest

from src.config import settings
from src.llm_providers import OpenAILLMProvider


@pytest.mark.openai
def test_openai_provider_mapping(monkeypatch):
    if not settings.openai_api_key:
        pytest.skip("OPENAI_API_KEY no configurada")
    provider = OpenAILLMProvider(settings.openai_api_key, settings.openai_model, settings.openai_api_base)
    invoice = {
        "supplier": {"name": "Proveedor Demo", "nif": "B12345678"},
        "totals": {"base": 100.0, "vat": 21.0, "gross": 121.0},
        "invoice": {"number": "TEST-1", "date": "2025-03-01", "currency": "EUR"},
        "lines": [{"desc": "Servicios profesionales", "amount": 100.0, "vat_rate": 21}],
        "metadata": {"doc_type": "invoice", "category": "servicios_prof"},
    }
    result = provider.propose_mapping(invoice)
    assert "account" in result
    assert "iva_type" in result
    assert "issue_codes" in result
