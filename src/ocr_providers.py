"""OCR provider abstractions for CERTIVA."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
import hashlib
import logging
import os
import random
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from . import azure_ocr_monitor, utils, ocr_cache

logger = logging.getLogger(__name__)


@dataclass
class OCRResult:
    supplier_name: str
    supplier_nif: str
    supplier_vat: Optional[str]
    invoice_number: str
    invoice_date: str
    due_date: Optional[str]
    currency: str
    base: float
    vat: float
    gross: float
    items: List[Dict[str, Any]]
    confidence: float
    ocr_text: str


class OCRProvider(ABC):
    provider_name = "undefined"

    @abstractmethod
    def analyze_document(self, file_path: Path, tenant: str) -> OCRResult:
        """Analiza un documento y devuelve un OCRResult estructurado."""
        raise NotImplementedError


class DummyOCRProvider(OCRProvider):
    """Provider offline que reutiliza PDFs con texto y JSON ya generados."""

    provider_name = "dummy"

    def _load_existing_json(self, file_path: Path) -> Optional[OCRResult]:
        from . import utils  # local import to avoid circular deps

        doc_id = utils.compute_sha256(file_path)
        json_path = utils.BASE_DIR / "OUT" / "json" / f"{doc_id}.json"
        if not json_path.exists():
            return None
        data = utils.read_json(json_path)
        supplier = data.get("supplier", {})
        invoice = data.get("invoice", {})
        totals = data.get("totals", {})
        lines = data.get("lines") or []
        return OCRResult(
            supplier_name=supplier.get("name", ""),
            supplier_nif=supplier.get("nif", ""),
            supplier_vat=supplier.get("vat"),
            invoice_number=invoice.get("number", ""),
            invoice_date=invoice.get("date", utils.today_iso()),
            due_date=invoice.get("due"),
            currency=invoice.get("currency", "EUR"),
            base=float(totals.get("base") or 0.0),
            vat=float(totals.get("vat") or 0.0),
            gross=float(totals.get("gross") or 0.0),
            items=[dict(line) for line in lines],
            confidence=float(data.get("confidence_ocr", 0.9)),
            ocr_text="",
        )

    def analyze_document(self, file_path: Path, tenant: str) -> OCRResult:  # noqa: ARG002
        from . import utils

        existing = self._load_existing_json(file_path)
        if existing:
            return existing
        try:
            from pdfminer.high_level import extract_text  # type: ignore
        except ImportError as exc:  # pragma: no cover - optional dep
            raise RuntimeError("pdfminer.six no está instalado para DummyOCRProvider") from exc
        if file_path.suffix.lower() != ".pdf":
            raise RuntimeError("DummyOCRProvider solo soporta PDFs con texto")
        text = extract_text(str(file_path))
        supplier_name = _match_regex(text, r"Proveedor[:\s]+(.+)") or _match_regex(text, r"Supplier[:\s]+(.+)")
        supplier_nif = _match_regex(text, r"(?:NIF|VAT|CIF)[:\s]+([A-Z0-9]{8,13})") or ""
        invoice_number = _match_regex(text, r"(?:Factura|Invoice)[:#\s]+([\w\-/]+)") or ""
        currency = _match_regex(text, r"Moneda[:\s]+([A-Z]{3})") or "EUR"
        base = _match_amount(text, r"Base imponible[:\s]+([0-9.,]+)")
        vat = _match_amount(text, r"IVA[:\s]+([0-9.,]+)")
        gross = _match_amount(text, r"Total[:\s]+([0-9.,]+)")
        invoice_date = _match_regex(text, r"Fecha[:\s]+([0-9\-/]+)") or utils.today_iso()
        due_date = _match_regex(text, r"Vencimiento[:\s]+([0-9\-/]+)")
        items = [
            {
                "desc": _match_regex(text, r"Concepto[:\s]+(.+)") or "Servicio",
                "qty": 1.0,
                "unit_price": base or 0.0,
                "vat_rate": 21.0 if base else 0.0,
                "amount": base or gross or 0.0,
            }
        ]
        confidence = 0.93 if supplier_name and supplier_nif and invoice_number else 0.6
        return OCRResult(
            supplier_name=supplier_name or "Proveedor",
            supplier_nif=supplier_nif,
            supplier_vat=None,
            invoice_number=invoice_number,
            invoice_date=invoice_date,
            due_date=due_date,
            currency=currency,
            base=base or (gross - vat if gross and vat else gross) or 0.0,
            vat=vat or (gross - base if gross and base else 0.0),
            gross=gross or (base + vat if base and vat else base) or 0.0,
            items=items,
            confidence=confidence,
            ocr_text=text,
        )


class OCRRetryableError(RuntimeError):
    """Señala que el OCR falló de forma temporal y puede reintentarse más tarde."""


class _TokenBucket:
    def __init__(self, rate: float) -> None:
        self.rate = max(0.01, rate)
        self.capacity = max(1.0, self.rate * 2)
        self.tokens = self.capacity
        self.timestamp = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self) -> float:
        if self.rate <= 0:
            return 0.0
        total_sleep = 0.0
        while True:
            with self.lock:
                now = time.monotonic()
                delta = now - self.timestamp
                self.timestamp = now
                self.tokens = min(self.capacity, self.tokens + delta * self.rate)
                if self.tokens >= 1:
                    self.tokens -= 1
                    return total_sleep
                needed = max(0.0, (1 - self.tokens) / self.rate)
            sleep_for = needed + random.uniform(0.0, 0.12)
            time.sleep(sleep_for)
            total_sleep += sleep_for


BASE_DIR = Path(__file__).resolve().parents[1]
_DEFAULT_RPS = float(
    os.getenv("AZURE_MAX_RPS")
    or os.getenv("AZURE_OCR_MAX_RPS", "0.8")
    or "0.8"
)
_DEFAULT_CONCURRENCY = int(os.getenv("AZURE_MAX_INFLIGHT") or os.getenv("AZURE_OCR_MAX_CONCURRENCY", "1") or "1")
_DEFAULT_MAX_ATTEMPTS = int(os.getenv("AZURE_OCR_RETRY_TOTAL", "4") or "4")
_DEFAULT_BACKOFF_FACTOR = float(os.getenv("AZURE_OCR_RETRY_BACKOFF", "1.0") or "1.0")
_DEFAULT_MAX_SLEEP = float(os.getenv("AZURE_OCR_RETRY_MAX_SLEEP", "45") or "45")
_DEFAULT_TIMEOUT = int(os.getenv("AZURE_OCR_READ_TIMEOUT_SEC", "120") or "120")
_DEFAULT_CACHE_DIR = Path(os.getenv("AZURE_OCR_CACHE_DIR", str(BASE_DIR / "OUT" / "ocr_cache")))
_DEFAULT_CACHE_ENABLED = os.getenv("AZURE_OCR_ENABLE_CACHE", "1") not in {"0", "false", "False"}
_DEFAULT_BACKOFF_SCHEDULE = [0.8, 2.1, 5.0, 11.0]


class AzureOCRProvider(OCRProvider):
    """Cliente robusto para Azure Document Intelligence (prebuilt invoice)."""

    provider_name = "azure"
    RETRYABLE_STATUS = {408, 429, 500, 502, 503, 504}

    def __init__(
        self,
        endpoint: Optional[str],
        key: Optional[str],
        model_id: Optional[str] = None,
        *,
        client=None,
        cache_dir: Optional[Path] = None,
        enable_cache: Optional[bool] = None,
        token_bucket: Optional[_TokenBucket] = None,
        semaphore: Optional[threading.Semaphore] = None,
        max_attempts: Optional[int] = None,
        max_rps: Optional[float] = None,
        max_concurrency: Optional[int] = None,
        backoff_factor: Optional[float] = None,
        backoff_schedule: Optional[List[float]] = None,
        max_sleep: Optional[float] = None,
        read_timeout: Optional[int] = None,
        polling_interval: Optional[float] = None,
    ) -> None:
        if not endpoint or not key or not model_id:
            raise RuntimeError("Azure OCR misconfigured: endpoint/key/model missing")
        self.endpoint = endpoint
        self.key = key
        self.model_id = model_id
        try:
            from azure.ai.formrecognizer import DocumentAnalysisClient  # type: ignore
            from azure.core.credentials import AzureKeyCredential  # type: ignore
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("Falta azure-ai-formrecognizer para AzureOCRProvider") from exc
        self._client = client or DocumentAnalysisClient(endpoint=self.endpoint, credential=AzureKeyCredential(self.key))
        rate = max(0.05, max_rps or _DEFAULT_RPS)
        concurrency = max(1, max_concurrency or _DEFAULT_CONCURRENCY)
        self._token_bucket = token_bucket or _TokenBucket(rate)
        self._semaphore = semaphore or threading.Semaphore(concurrency)
        self._cache_dir = Path(cache_dir) if cache_dir else _DEFAULT_CACHE_DIR
        self._cache_enabled = _DEFAULT_CACHE_ENABLED if enable_cache is None else enable_cache
        ocr_cache.configure(self._cache_dir, self._cache_enabled)
        self._max_attempts = max(1, max_attempts or _DEFAULT_MAX_ATTEMPTS)
        self._max_sleep = max(1.0, max_sleep or _DEFAULT_MAX_SLEEP)
        factor = backoff_factor if backoff_factor is not None else _DEFAULT_BACKOFF_FACTOR
        schedule = backoff_schedule or [min(self._max_sleep, base * factor) for base in _DEFAULT_BACKOFF_SCHEDULE]
        if not schedule:
            schedule = list(_DEFAULT_BACKOFF_SCHEDULE)
        self._backoff_schedule = schedule
        self._read_timeout = max(30, read_timeout or _DEFAULT_TIMEOUT)
        self._polling_interval = polling_interval or 2.5

    def analyze_document(self, file_path: Path, tenant: str) -> OCRResult:
        from azure.core.exceptions import HttpResponseError, ServiceRequestError  # type: ignore

        data = file_path.read_bytes()
        doc_hash = hashlib.sha256(data).hexdigest()
        cached = ocr_cache.get_cached(doc_hash) if self._cache_enabled else None
        if cached:
            azure_ocr_monitor.record_call(
                status_code=304,
                latency_ms=0.0,
                cache_hit=True,
                throttling_delay_ms=0.0,
                retried=False,
            )
            return cached

        last_error: Optional[Exception] = None
        last_status = 0
        for attempt in range(1, self._max_attempts + 1):
            throttle_delay = self._token_bucket.acquire()
            with self._semaphore:
                start = time.perf_counter()
                try:
                    poller = self._client.begin_analyze_document(
                        self.model_id,
                        document=data,
                        polling_interval=self._polling_interval,
                    )
                    result = poller.result(timeout=self._read_timeout)
                    mapped = self._map_result(result)
                    elapsed = (time.perf_counter() - start) * 1000
                    azure_ocr_monitor.record_call(
                        status_code=200,
                        latency_ms=elapsed,
                        cache_hit=False,
                        throttling_delay_ms=throttle_delay * 1000,
                        retried=attempt > 1,
                    )
                    ocr_cache.put_cached(doc_hash, mapped)
                    return mapped
                except HttpResponseError as exc:
                    last_error = exc
                    last_status = getattr(exc, "status_code", 0) or 0
                    retry_after = self._parse_retry_after(exc)
                    elapsed = (time.perf_counter() - start) * 1000
                    azure_ocr_monitor.record_call(
                        status_code=last_status,
                        latency_ms=elapsed,
                        retry_after_s=retry_after,
                        cache_hit=False,
                        throttling_delay_ms=throttle_delay * 1000,
                        retried=True,
                    )
                    if not self._should_retry(last_status) or attempt == self._max_attempts:
                        break
                    self._sleep_with_jitter(retry_after, attempt)
                    continue
                except ServiceRequestError as exc:
                    last_error = exc
                    last_status = 0
                    elapsed = (time.perf_counter() - start) * 1000
                    azure_ocr_monitor.record_call(
                        status_code=0,
                        latency_ms=elapsed,
                        cache_hit=False,
                        throttling_delay_ms=throttle_delay * 1000,
                        retried=True,
                    )
                    if attempt == self._max_attempts:
                        break
                    self._sleep_with_jitter(0.0, attempt)
                    continue
                except Exception as exc:  # pragma: no cover - defensive
                    last_error = exc
                    last_status = getattr(exc, "status_code", 0) or 0
                    elapsed = (time.perf_counter() - start) * 1000
                    azure_ocr_monitor.record_call(
                        status_code=last_status,
                        latency_ms=elapsed,
                        cache_hit=False,
                        throttling_delay_ms=throttle_delay * 1000,
                        retried=True,
                    )
                    if attempt == self._max_attempts or not self._should_retry(last_status):
                        break
                    self._sleep_with_jitter(0.0, attempt)
                    continue

        if last_status in self.RETRYABLE_STATUS or isinstance(last_error, ServiceRequestError):
            raise OCRRetryableError(str(last_error) if last_error else "Azure OCR temporal")
        raise RuntimeError(f"AzureOCRProvider falló para {file_path.name}: {last_error}") from last_error

    def _parse_retry_after(self, exc: Exception) -> float:
        header_value: Optional[str] = None
        response = getattr(exc, "response", None)
        if response is not None:
            headers = getattr(response, "headers", {}) or {}
            header_value = headers.get("Retry-After") or headers.get("retry-after")
        if not header_value:
            return 0.0
        try:
            return min(float(header_value), self._max_sleep)
        except ValueError:
            return 0.0

    def _sleep_with_jitter(self, retry_after: float, attempt: int) -> None:
        if retry_after:
            target = min(retry_after, self._max_sleep)
        else:
            schedule_idx = min(attempt - 1, len(self._backoff_schedule) - 1)
            target = min(self._backoff_schedule[schedule_idx], self._max_sleep)
        jitter = random.uniform(0.0, max(0.05, target * 0.2))
        time.sleep(min(self._max_sleep, target + jitter))

    def _should_retry(self, status_code: int) -> bool:
        return status_code == 0 or status_code in self.RETRYABLE_STATUS

    @staticmethod
    def _value_to_float(value: Any) -> float:
        if value is None:
            return 0.0
        amount = getattr(value, "amount", None)
        if amount is not None:
            try:
                return float(amount or 0.0)
            except (TypeError, ValueError):
                return 0.0
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _value_to_str(value: Any) -> str:
        if value is None:
            return ""
        currency_code = getattr(value, "currency", None)
        if currency_code:
            return str(currency_code)
        try:
            return str(value)
        except Exception:  # pragma: no cover
            return ""

    def _map_result(self, result) -> OCRResult:
        if not result.documents:
            raise RuntimeError("Azure OCR no devolvió documentos")
        invoice_doc = result.documents[0]
        fields = invoice_doc.fields or {}

        def _field_value(name: str) -> Any:
            field = fields.get(name) or fields.get(name.lower()) or fields.get(name.upper())
            return getattr(field, "value", None)

        items: List[Dict[str, Any]] = []
        raw_items = _field_value("Items") or []
        for item in raw_items:
            try:
                item_value = item.value or {}
                desc = getattr(item_value.get("Description"), "value", None)
                qty = getattr(item_value.get("Quantity"), "value", None)
                unit_price = getattr(item_value.get("UnitPrice"), "value", None)
                amount = getattr(item_value.get("Amount"), "value", None)
                vat_rate = getattr(item_value.get("Tax"), "value", None)
            except AttributeError:
                continue
            items.append(
                {
                    "desc": desc or "Concepto",
                    "qty": self._value_to_float(qty or 1.0),
                    "unit_price": self._value_to_float(unit_price or amount or 0.0),
                    "vat_rate": self._value_to_float(vat_rate or 21.0),
                    "amount": self._value_to_float(amount or 0.0),
                }
            )
        currency_field = _field_value("Currency") or _field_value("CurrencyCode")
        currency = self._value_to_str(currency_field) or "EUR"
        ocr_conf = getattr(invoice_doc, "confidence", None) or 0.85
        ocr_text = getattr(result, "content", "") or ""
        return OCRResult(
            supplier_name=(str(_field_value("VendorName") or "")).strip(),
            supplier_nif=(str(_field_value("VendorTaxId") or "")).strip(),
            supplier_vat=(str(_field_value("VendorTaxId") or "")).strip() or None,
            invoice_number=(str(_field_value("InvoiceId") or "")).strip(),
            invoice_date=str(_field_value("InvoiceDate") or ""),
            due_date=str(_field_value("DueDate") or ""),
            currency=currency,
            base=self._value_to_float(_field_value("SubTotal")),
            vat=self._value_to_float(_field_value("TotalTaxAmount")),
            gross=self._value_to_float(_field_value("InvoiceTotal")),
            items=items,
            confidence=float(ocr_conf),
            ocr_text=ocr_text,
        )


def _match_regex(text: str, pattern: str) -> Optional[str]:
    import re

    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None


def _match_amount(text: str, pattern: str) -> Optional[float]:
    raw = _match_regex(text, pattern)
    if not raw:
        return None
    raw = raw.replace(".", "").replace(",", ".")
    try:
        return float(raw)
    except ValueError:
        return None
