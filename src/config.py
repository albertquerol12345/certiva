from __future__ import annotations

from functools import lru_cache
import json
import logging
import os
from pathlib import Path
from typing import Dict, Literal, Optional

from dotenv import load_dotenv
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Para evitar ciclos, importamos proveedores dentro de los builders.
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - solo para type checkers
    from .ocr_providers import OCRProvider  # noqa: F401
    from .llm_providers import LLMProvider  # noqa: F401

BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / ".env"
TENANTS_CONFIG_PATH = BASE_DIR / "tenants.json"
load_dotenv(ENV_PATH)
ENV_PROFILE = os.getenv("APP_ENV") or os.getenv("ENV")
if ENV_PROFILE:
    overlay = BASE_DIR / f".env.{ENV_PROFILE}"
    if overlay.exists():
        load_dotenv(overlay, override=True)
ENV_PROFILE = os.getenv("APP_ENV") or os.getenv("ENV")
if ENV_PROFILE:
    overlay = BASE_DIR / f".env.{ENV_PROFILE}"
    if overlay.exists():
        load_dotenv(overlay, override=True)
logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    app_env: Literal["dev", "test", "prod"] = Field(default="dev", alias="APP_ENV")
    # Mantiene compatibilidad con OCR_PROVIDER_TYPE y permite OCR_PROVIDER como fallback
    ocr_provider_type: Literal["azure", "dummy"] = Field(default="dummy", alias="OCR_PROVIDER_TYPE")
    default_tenant: str = Field(default="demo", alias="DEFAULT_TENANT")

    azure_formrec_endpoint: Optional[str] = Field(alias="AZURE_FORMREC_ENDPOINT", default=None)
    azure_formrec_key: Optional[str] = Field(alias="AZURE_FORMREC_KEY", default=None)
    azure_formrec_model_id: Optional[str] = Field(alias="AZURE_FORMREC_MODEL_ID", default=None)
    azure_ocr_max_rps: float = Field(default=0.8, alias="AZURE_OCR_MAX_RPS")
    azure_ocr_max_concurrency: int = Field(default=1, alias="AZURE_OCR_MAX_CONCURRENCY")
    azure_ocr_retry_total: int = Field(default=8, alias="AZURE_OCR_RETRY_TOTAL")
    azure_ocr_retry_backoff: float = Field(default=1.0, alias="AZURE_OCR_RETRY_BACKOFF")
    azure_ocr_retry_max_sleep: float = Field(default=45.0, alias="AZURE_OCR_RETRY_MAX_SLEEP")
    azure_ocr_read_timeout_sec: int = Field(default=120, alias="AZURE_OCR_READ_TIMEOUT_SEC")
    azure_ocr_cache_dir: str = Field(default=str(BASE_DIR / "OUT" / "ocr_cache"), alias="AZURE_OCR_CACHE_DIR")
    azure_ocr_enable_cache: bool = Field(default=True, alias="AZURE_OCR_ENABLE_CACHE")

    gcp_credentials: Optional[str] = Field(alias="GOOGLE_APPLICATION_CREDENTIALS", default=None)
    gcp_project_id: Optional[str] = Field(alias="GCP_PROJECT_ID", default=None)
    gcp_location: Optional[str] = Field(alias="GCP_LOCATION", default=None)
    gcp_processor_id: Optional[str] = Field(alias="GCP_PROCESSOR_ID", default=None)

    min_conf_ocr: float = Field(default=0.90, alias="MIN_CONF_OCR")
    min_conf_entry: float = Field(default=0.85, alias="MIN_CONF_ENTRY")

    openai_api_key: Optional[str] = Field(alias="OPENAI_API_KEY", default=None)
    openai_api_base: str = Field(alias="OPENAI_API_BASE", default="https://api.openai.com/v1")
    openai_model: str = Field(alias="OPENAI_MODEL", default="gpt-5.1-codex-mini")
    openai_model_mini: Optional[str] = Field(alias="OPENAI_MODEL_MINI", default=None)
    openai_model_premium: Optional[str] = Field(alias="OPENAI_MODEL_PREMIUM", default=None)
    llm_provider_type: Literal["dummy", "openai"] = Field(default="dummy", alias="LLM_PROVIDER_TYPE")
    llm_strategy: Literal["mini_only", "dual_cascade"] = Field(default="mini_only", alias="LLM_STRATEGY")
    llm_premium_threshold_gross: float = Field(default=1000.0, alias="LLM_PREMIUM_THRESHOLD_GROSS")
    debug_llm: bool = Field(default=False, alias="DEBUG_LLM")
    llm_debug_redact_pii: bool = Field(default=True, alias="LLM_DEBUG_REDACT_PII")
    confidence_min_ok: float = Field(default=0.8, alias="CONFIDENCE_MIN_OK")
    watch_batch_size: int = Field(default=50, alias="WATCH_BATCH_SIZE")
    watch_batch_timeout: int = Field(default=300, alias="WATCH_BATCH_TIMEOUT")
    watch_glob: str = Field(default="*.pdf", alias="WATCH_GLOB")
    pipeline_concurrency: int = Field(default=1, alias="PIPELINE_CONCURRENCY")
    ocr_breaker_threshold: int = Field(default=3, alias="OCR_BREAKER_THRESHOLD")
    llm_breaker_threshold: int = Field(default=3, alias="LLM_BREAKER_THRESHOLD")
    openai_mini_in_per_mtok: float = Field(default=0.20, alias="OPENAI_MINI_IN_PER_MTOK")
    openai_mini_out_per_mtok: float = Field(default=0.80, alias="OPENAI_MINI_OUT_PER_MTOK")
    openai_premium_in_per_mtok: float = Field(default=1.00, alias="OPENAI_PREMIUM_IN_PER_MTOK")
    openai_premium_out_per_mtok: float = Field(default=4.00, alias="OPENAI_PREMIUM_OUT_PER_MTOK")

    manifest_path: Optional[str] = Field(alias="GOLDEN_MANIFEST_PATH", default=None)
    web_session_secret: str = Field(alias="WEB_SESSION_SECRET", default="change-this")
    session_cookie_secure: bool = Field(default=True, alias="WEB_SESSION_SECURE_COOKIES")
    session_cookie_same_site: str = Field(default="lax", alias="WEB_SESSION_SAMESITE")
    session_cookie_max_age: int = Field(default=60 * 60 * 8, alias="WEB_SESSION_MAX_AGE")
    web_allowed_origin: str = Field(default="http://localhost:8000", alias="WEB_ALLOWED_ORIGIN")
    auth_max_fails: int = Field(default=5, alias="AUTH_MAX_FAILS")
    auth_lock_minutes: int = Field(default=15, alias="AUTH_LOCK_MINUTES")
    hitl_page_size: int = Field(default=20, alias="HITL_PAGE_SIZE")
    llm_rag_provider: str = Field(default="dummy", alias="LLM_RAG_PROVIDER")
    llm_rag_model: str = Field(default="gpt-4o-mini", alias="LLM_RAG_MODEL")
    llm_explain_provider: str = Field(default="dummy", alias="LLM_EXPLAIN_PROVIDER")
    llm_explain_model: str = Field(default="grok-4-fast", alias="LLM_EXPLAIN_MODEL")
    llm_suggest_provider: str = Field(default="dummy", alias="LLM_SUGGEST_PROVIDER")
    llm_suggest_model: str = Field(default="gpt-4o-mini", alias="LLM_SUGGEST_MODEL")
    llm_timeout_seconds: int = Field(default=20, alias="LLM_TIMEOUT_SECONDS")
    llm_cost_alert_daily_eur: float = Field(default=50.0, alias="LLM_COST_ALERT_DAILY_EUR")
    llm_enable_pii: bool = Field(default=False, alias="LLM_ENABLE_PII")
    llm_pii_scrub_strict: bool = Field(default=False, alias="LLM_PII_SCRUB_STRICT")
    llm_max_calls_tenant_daily: int = Field(default=10000, alias="LLM_MAX_CALLS_TENANT_DAILY")
    llm_max_calls_user_daily: int = Field(default=2000, alias="LLM_MAX_CALLS_USER_DAILY")
    sii_tax_id: str = Field(default="ES00000000A", alias="SII_TAX_ID")
    sii_name: str = Field(default="CERTIVA DEMO SL", alias="SII_NAME")
    facturae_tax_id: str = Field(default="ES00000000A", alias="FACTURAE_TAX_ID")
    facturae_name: str = Field(default="CERTIVA DEMO SL", alias="FACTURAE_NAME")
    facturae_address: str = Field(default="C/ Principal 123, Madrid", alias="FACTURAE_ADDRESS")
    facturae_postal_code: str = Field(default="28001", alias="FACTURAE_POSTAL_CODE")
    facturae_country_code: str = Field(default="ESP", alias="FACTURAE_COUNTRY_CODE")
    imap_host: Optional[str] = Field(default=None, alias="IMAP_HOST")
    imap_user: Optional[str] = Field(default=None, alias="IMAP_USER")
    imap_password: Optional[str] = Field(default=None, alias="IMAP_PASSWORD")
    imap_mailbox: str = Field(default="INBOX", alias="IMAP_MAILBOX")
    sftp_host: Optional[str] = Field(default=None, alias="SFTP_HOST")
    sftp_user: Optional[str] = Field(default=None, alias="SFTP_USER")
    sftp_password: Optional[str] = Field(default=None, alias="SFTP_PASSWORD")
    sftp_remote_path: str = Field(default="/", alias="SFTP_REMOTE_PATH")
    # Alerting
    alert_webhook_url: Optional[str] = Field(default=None, alias="ALERT_WEBHOOK_URL")
    alert_review_queue_threshold: int = Field(default=50, alias="ALERT_REVIEW_QUEUE_THRESHOLD")
    alert_batch_warning: bool = Field(default=True, alias="ALERT_BATCH_WARNING")
    alert_zero_page_threshold: int = Field(default=1, alias="ALERT_ZERO_PAGE_THRESHOLD")
    alert_webhook_format: str = Field(default="slack", alias="ALERT_WEBHOOK_FORMAT")
    prometheus_target: str = Field(default="http://localhost:8000/metrics", alias="PROMETHEUS_TARGET")
    out_retention_days: int = Field(default=30, alias="OUT_RETENTION_DAYS")

    @model_validator(mode="after")
    def apply_profile_defaults(self) -> "Settings":
        env = (self.app_env or "dev").lower()
        fields_set = getattr(self, "model_fields_set", set())
        # Compatibilidad con OCR_PROVIDER (legacy)
        legacy_ocr = os.getenv("OCR_PROVIDER")
        if legacy_ocr and "ocr_provider_type" not in fields_set:
            self.ocr_provider_type = legacy_ocr.lower()  # type: ignore[assignment]
        if env == "prod":
            if "debug_llm" not in fields_set:
                self.debug_llm = False
            if "llm_debug_redact_pii" not in fields_set:
                self.llm_debug_redact_pii = True
            if "pipeline_concurrency" not in fields_set:
                self.pipeline_concurrency = 1
            if "llm_max_calls_tenant_daily" not in fields_set:
                self.llm_max_calls_tenant_daily = min(self.llm_max_calls_tenant_daily, 2000)
            if "llm_max_calls_user_daily" not in fields_set:
                self.llm_max_calls_user_daily = min(self.llm_max_calls_user_daily, 500)
            if self.web_session_secret in {"change-this", "changeme", ""}:
                raise ValueError("WEB_SESSION_SECRET debe establecerse con un valor fuerte en prod")
            if self.session_cookie_secure is False:
                raise ValueError("WEB_SESSION_SECURE_COOKIES debe estar activo en prod")
            if self.llm_enable_pii and not self.llm_pii_scrub_strict:
                raise ValueError("En prod no se permite LLM_ENABLE_PII sin LLM_PII_SCRUB_STRICT=1")
        elif env == "test":
            if "debug_llm" not in fields_set:
                self.debug_llm = False
        return self

    model_config = SettingsConfigDict(
        populate_by_name=True,
        env_file=str(ENV_PATH),
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache()
def get_settings() -> Settings:
    return Settings()


def _load_tenants_config() -> Dict[str, Dict[str, str]]:
    if TENANTS_CONFIG_PATH.exists():
        try:
            with TENANTS_CONFIG_PATH.open("r", encoding="utf-8") as fh:
                data = json.load(fh)
                if isinstance(data, dict):
                    return data
        except json.JSONDecodeError:
            pass
    return {
        "default": {
            "default_journal": "COMPRAS",
            "supplier_account": "410000",
        }
    }


tenant_configs = _load_tenants_config()


def _with_defaults(config: Dict[str, str]) -> Dict[str, str]:
    enriched = dict(config)
    enriched.setdefault("erp", "a3innuva")
    enriched.setdefault("default_journal", "COMPRAS")
    enriched.setdefault("supplier_account", "410000")
    enriched.setdefault("sales_journal", "VENTAS")
    enriched.setdefault("customer_account", "430000")
    return enriched


def get_tenant_config(tenant: str) -> Dict[str, str]:
    tenant_norm = tenant.lower()
    for key, value in tenant_configs.items():
        if key.lower() == tenant_norm:
            return _with_defaults(value)
    default_cfg = tenant_configs.get("default", {"default_journal": "COMPRAS", "supplier_account": "410000"})
    return _with_defaults(default_cfg)


def list_tenants(include_defaults: bool = False) -> Dict[str, Dict[str, str]]:
    """Return raw tenant definitions or enriched copies with defaults."""
    if include_defaults:
        return {name: _with_defaults(cfg) for name, cfg in tenant_configs.items()}
    return {name: dict(cfg) for name, cfg in tenant_configs.items()}


def _write_tenants_file(data: Dict[str, Dict[str, str]]) -> None:
    TENANTS_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with TENANTS_CONFIG_PATH.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)


def save_tenant_config(tenant: str, definition: Dict[str, str]) -> None:
    """Persist a tenant definition (raw, without auto defaults)."""
    clean = {k: v for k, v in definition.items() if v not in (None, "")}
    global tenant_configs
    tenant_configs = dict(tenant_configs)
    tenant_configs[tenant] = clean
    _write_tenants_file(tenant_configs)


def reload_tenants_config() -> Dict[str, Dict[str, str]]:
    """Reload tenants configuration from disk."""
    global tenant_configs
    tenant_configs = _load_tenants_config()
    return tenant_configs


settings = get_settings()

_provider_overrides: Dict[str, Optional[object]] = {"ocr": None, "llm": None}


@lru_cache()
def _build_ocr_provider() -> OCRProvider:
    from .ocr_providers import AzureOCRProvider, DummyOCRProvider, OCRProvider  # local import para evitar ciclos

    if settings.ocr_provider_type == "azure":
        try:
            provider = AzureOCRProvider(
                settings.azure_formrec_endpoint,
                settings.azure_formrec_key,
                settings.azure_formrec_model_id or "prebuilt-invoice",
                cache_dir=Path(settings.azure_ocr_cache_dir),
                enable_cache=settings.azure_ocr_enable_cache,
                max_attempts=settings.azure_ocr_retry_total,
                max_rps=settings.azure_ocr_max_rps,
                max_concurrency=settings.azure_ocr_max_concurrency,
                backoff_factor=settings.azure_ocr_retry_backoff,
                max_sleep=settings.azure_ocr_retry_max_sleep,
                read_timeout=settings.azure_ocr_read_timeout_sec,
            )
            return provider
        except Exception as exc:
            logger.warning("Falling back to DummyOCRProvider: Azure OCR unavailable (%s)", exc)
            fallback = DummyOCRProvider()
            setattr(fallback, "fallback_issue_code", "OCR_PROVIDER_FALLBACK")
            return fallback
    return DummyOCRProvider()


@lru_cache()
def _build_llm_provider() -> LLMProvider:
    from .llm_providers import DummyLLMProvider, LLMProvider, OpenAILLMProvider, DualOpenAILLMProvider  # local import to avoid cycle

    if settings.llm_provider_type == "openai":
        mini_model = settings.openai_model_mini or settings.openai_model
        premium_model = settings.openai_model_premium or settings.openai_model
        pricing = {
            "mini": {
                "in": settings.openai_mini_in_per_mtok,
                "out": settings.openai_mini_out_per_mtok,
            },
            "premium": {
                "in": settings.openai_premium_in_per_mtok,
                "out": settings.openai_premium_out_per_mtok,
            },
        }
        try:
            if settings.llm_strategy == "dual_cascade":
                return DualOpenAILLMProvider(
                    api_key=settings.openai_api_key,
                    api_base=settings.openai_api_base,
                    model_mini=mini_model,
                    model_premium=premium_model,
                    threshold_gross=settings.llm_premium_threshold_gross,
                    pricing=pricing,
                )
            return OpenAILLMProvider(
                settings.openai_api_key,
                mini_model,
                settings.openai_api_base,
                pricing={"mini": pricing["mini"]},
            )
        except RuntimeError as exc:
            logger.warning("Falling back to DummyLLMProvider: %s", exc)
    return DummyLLMProvider()


def get_ocr_provider() -> OCRProvider:
    override = _provider_overrides.get("ocr")
    if override:
        return override  # type: ignore[return-value]
    return _build_ocr_provider()


def get_llm_provider() -> LLMProvider:
    override = _provider_overrides.get("llm")
    if override:
        return override  # type: ignore[return-value]
    return _build_llm_provider()


def set_ocr_provider_override(provider: Optional[OCRProvider]) -> None:
    _provider_overrides["ocr"] = provider


def set_llm_provider_override(provider: Optional[LLMProvider]) -> None:
    _provider_overrides["llm"] = provider
