import pytest
from pathlib import Path

from src.config import settings
from src.ocr_providers import AzureOCRProvider


@pytest.mark.azure
def test_azure_provider_optional(monkeypatch):
    if not (settings.azure_formrec_endpoint and settings.azure_formrec_key):
        pytest.skip("Azure Document Intelligence no configurado.")
    provider = AzureOCRProvider(
        settings.azure_formrec_endpoint,
        settings.azure_formrec_key,
        settings.azure_formrec_model_id,
    )
    sample_pdf = Path("tests/golden/golden_01_suministro_iberdrola.pdf")
    result = provider.analyze_document(sample_pdf, settings.default_tenant)
    assert result.supplier_name
    assert result.invoice_number
