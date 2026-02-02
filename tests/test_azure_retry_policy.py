from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict

import pytest

from azure.core.exceptions import HttpResponseError  # type: ignore

from src.config import settings
from src.ocr_providers import AzureOCRProvider, OCRResult
from src import azure_ocr_monitor


class FakePoller:
    def __init__(self, payload: Dict[str, Any]):
        self.payload = payload

    def result(self, timeout: int = 0):
        document = SimpleNamespace(fields=self.payload, confidence=0.92)
        return SimpleNamespace(documents=[document], content="mock-content")


class FakeClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def begin_analyze_document(self, model_id, document, polling_interval):
        self.calls += 1
        answer = self.responses.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return FakePoller(answer)


def _dummy_result() -> Dict[str, Any]:
    return {
        "VendorName": SimpleNamespace(value="Proveedor"),
        "VendorTaxId": SimpleNamespace(value="B12345678"),
        "CustomerTaxId": SimpleNamespace(value=""),
        "InvoiceId": SimpleNamespace(value="INV-1"),
        "InvoiceDate": SimpleNamespace(value="2025-01-01"),
        "DueDate": SimpleNamespace(value="2025-01-10"),
        "InvoiceTotal": SimpleNamespace(value=121.0, amount=121.0, currency="EUR"),
        "SubTotal": SimpleNamespace(value=100.0, amount=100.0),
        "TotalTaxAmount": SimpleNamespace(value=21.0, amount=21.0),
        "Items": [],
    }


class DummyResponse:
    def __init__(self, status: int, retry_after: str):
        self.status_code = status
        self.headers = {"Retry-After": retry_after}
        self.reason = "throttle"


def _http_error(status: int, retry_after: str) -> HttpResponseError:
    response = DummyResponse(status, retry_after)
    return HttpResponseError(message="mock", response=response)


def _sample_result() -> OCRResult:
    return OCRResult(
        supplier_name="Proveedor",
        supplier_nif="B12345678",
        supplier_vat="",
        invoice_number="INV-1",
        invoice_date="2025-01-01",
        due_date="2025-01-10",
        currency="EUR",
        base=100.0,
        vat=21.0,
        gross=121.0,
        items=[{"desc": "Servicio", "qty": 1.0, "unit_price": 100.0, "vat_rate": 21.0, "amount": 100.0}],
        confidence=0.9,
        ocr_text="",
    )


@pytest.mark.usefixtures("temp_certiva_env")
def test_azure_retry_respects_retry_after(monkeypatch, tmp_path):
    sleep_calls = []

    def fake_sleep(value):
        sleep_calls.append(value)

    monkeypatch.setattr("time.sleep", fake_sleep)
    provider = AzureOCRProvider(
        settings.azure_formrec_endpoint,
        settings.azure_formrec_key,
        settings.azure_formrec_model_id or "prebuilt-invoice",
        client=FakeClient(
            [
                _http_error(429, "0.1"),
                _dummy_result(),
                _dummy_result(),
            ]
        ),
        cache_dir=tmp_path / "cache",
        enable_cache=False,
        max_attempts=2,
    )
    azure_ocr_monitor.snapshot(reset=True)
    pdf = tmp_path / "doc.pdf"
    pdf.write_bytes(b"%PDF-1.4 mock")
    provider.analyze_document(pdf, "demo")
    assert sleep_calls, "Debió esperar Retry-After"
    assert any(call >= 0.1 for call in sleep_calls)
    stats = azure_ocr_monitor.snapshot(reset=True)
    assert stats["status_counts"].get(429) == 1


@pytest.mark.usefixtures("temp_certiva_env")
def test_azure_cache_hit_short_circuits(monkeypatch, tmp_path):
    client = FakeClient([_dummy_result(), _dummy_result()])
    provider = AzureOCRProvider(
        settings.azure_formrec_endpoint,
        settings.azure_formrec_key,
        settings.azure_formrec_model_id or "prebuilt-invoice",
        client=client,
        cache_dir=tmp_path / "cache",
        enable_cache=True,
        max_attempts=2,
    )
    pdf = tmp_path / "doc.pdf"
    pdf.write_bytes(b"%PDF-1.4 mock")
    provider.analyze_document(pdf, "demo")
    result = provider.analyze_document(pdf, "demo")
    assert isinstance(result, OCRResult)
    assert client.calls == 1
