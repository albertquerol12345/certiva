"""Router centralizado para llamadas a modelos LLM."""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from enum import Enum
import time
from typing import Dict, Optional

from .config import settings
from . import utils
from .pii_scrub import scrub_pii

logger = logging.getLogger(__name__)

MAX_CONTEXT_CHARS = 4000


class LLMTask(str, Enum):
    RAG_NORMATIVO = "rag_normativo"
    EXPLICAR_PNL = "explicar_pnl"
    EXPLICAR_IVA = "explicar_iva"
    EXPLICAR_CASHFLOW = "explicar_cashflow"
    SUGERIR_MAPPING = "sugerir_mapping"


@dataclass
class LLMConfig:
    provider: str
    model: str
    max_tokens: int = 1024
    temperature: float = 0.2
    cost_hint: float = 0.0


def _build_task_config() -> Dict[LLMTask, LLMConfig]:
    return {
        LLMTask.RAG_NORMATIVO: LLMConfig(
            provider=settings.llm_rag_provider,
            model=settings.llm_rag_model,
            max_tokens=900,
            temperature=0.1,
        ),
        LLMTask.EXPLICAR_PNL: LLMConfig(
            provider=settings.llm_explain_provider,
            model=settings.llm_explain_model,
            max_tokens=700,
            temperature=0.25,
        ),
        LLMTask.EXPLICAR_IVA: LLMConfig(
            provider=settings.llm_explain_provider,
            model=settings.llm_explain_model,
            max_tokens=600,
            temperature=0.2,
        ),
        LLMTask.EXPLICAR_CASHFLOW: LLMConfig(
            provider=settings.llm_explain_provider,
            model=settings.llm_explain_model,
            max_tokens=700,
            temperature=0.3,
        ),
        LLMTask.SUGERIR_MAPPING: LLMConfig(
            provider=settings.llm_suggest_provider,
            model=settings.llm_suggest_model,
            max_tokens=400,
            temperature=0.2,
        ),
    }


TASK_CONFIG = _build_task_config()


def _scrub_text(text: str) -> str:
    return scrub_pii(
        text or "",
        strict=settings.llm_pii_scrub_strict,
        enabled=not settings.llm_enable_pii,
    )


def _truncate(text: Optional[str]) -> Optional[str]:
    if not text:
        return text
    if len(text) > MAX_CONTEXT_CHARS:
        return text[:MAX_CONTEXT_CHARS] + "\n...[contenido truncado]..."
    return text


def _resolve_provider(provider: str) -> str:
    normalized = (provider or "dummy").lower()
    if normalized == "dummy":
        return "dummy"
    if normalized == "openai":
        if settings.openai_api_key:
            return normalized
        logger.warning("OPENAI_API_KEY no configurado. Se usará proveedor 'dummy'.")
        return "dummy"
    env_key = f"{normalized.upper()}_API_KEY"
    if os.getenv(env_key):
        return normalized
    logger.warning("Proveedor %s sin credenciales (%s). Se usará 'dummy'.", normalized, env_key)
    return "dummy"


def call_llm(
    task: LLMTask,
    system_prompt: str,
    user_prompt: str,
    context: Optional[str] = None,
    tenant: Optional[str] = None,
    user: Optional[str] = None,
) -> str:
    """Llama al modelo configurado para una tarea y devuelve la respuesta textual."""
    cfg = TASK_CONFIG.get(task)
    if not cfg:
        logger.warning("No existe configuración LLM para la tarea %s. Se usará proveedor 'dummy'.", task)
        cfg = LLMConfig(provider="dummy", model="placeholder")

    safe_user_prompt = _scrub_text(user_prompt)
    safe_context = _truncate(_scrub_text(context or ""))
    combined_prompt = safe_user_prompt
    if safe_context:
        combined_prompt = f"{safe_user_prompt}\n\nContexto autorizado:\n{safe_context}"

    quota_error = utils.check_llm_quota(tenant, user)
    if quota_error:
        logger.warning("LLM quota exceeded for tenant=%s user=%s", tenant, user)
        simulated = f"[Simulación {task.value}] {quota_error}"
        utils.log_llm_call(
            task.value,
            "quota_guard",
            "quota_guard",
            0,
            0,
            0.0,
            quota_error,
            tenant=tenant,
            username=user,
        )
        return simulated

    provider = _resolve_provider(cfg.provider)
    logger.info("LLM call task=%s provider=%s model=%s", task.value, provider, cfg.model)

    start = time.monotonic()
    response = ""
    error = None
    try:
        if provider == "openai":
            response = _call_openai(cfg, system_prompt, combined_prompt)
        elif provider in {"groq", "xai"}:
            logger.warning("Proveedor %s aún no implementado. Se devolverá respuesta simulada.", provider)
            response = _simulate_response(task, combined_prompt)
        else:
            response = _simulate_response(task, combined_prompt)
        return response
    except Exception as exc:  # pragma: no cover - defensive
        error = str(exc)
        logger.error("LLM call failed: %s", exc)
        return _simulate_response(task, combined_prompt)
    finally:
        latency_ms = (time.monotonic() - start) * 1000
        prompt_tokens = _estimate_tokens(system_prompt) + _estimate_tokens(combined_prompt)
        completion_tokens = _estimate_tokens(response)
        utils.log_llm_call(
            task.value,
            provider,
            cfg.model,
            prompt_tokens,
            completion_tokens,
            latency_ms,
            error,
            tenant=tenant,
            username=user,
        )


def _call_openai(cfg: LLMConfig, system_prompt: str, combined_prompt: str) -> str:
    try:
        import openai
    except ImportError:  # pragma: no cover - dependencia opcional
        logger.error("openai no está instalado. Devuelvo respuesta simulada.")
        return _simulate_response(LLMTask.RAG_NORMATIVO, combined_prompt)

    api_key = settings.openai_api_key
    if not api_key:
        logger.warning("OPENAI_API_KEY no configurado. Devuelvo respuesta simulada.")
        return _simulate_response(LLMTask.RAG_NORMATIVO, combined_prompt)

    openai.api_key = api_key
    openai.api_base = settings.openai_api_base
    try:
        completion = openai.ChatCompletion.create(
            model=cfg.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": combined_prompt},
            ],
            max_tokens=cfg.max_tokens,
            temperature=cfg.temperature,
            timeout=settings.llm_timeout_seconds,
        )
        return completion["choices"][0]["message"]["content"].strip()
    except Exception as exc:  # pragma: no cover - errores externos
        logger.error("Error llamando a OpenAI: %s", exc)
        return _simulate_response(LLMTask.RAG_NORMATIVO, combined_prompt)


def _simulate_response(task: LLMTask, combined_prompt: str) -> str:
    preview = combined_prompt.strip().splitlines()[0][:120] if combined_prompt else ""
    return f"[Simulación {task.value}] Respuesta generada localmente. Prompt: {preview!r}"
@dataclass
class LLMCallLog:
    task: LLMTask
    provider: str
    model: str
    prompt_tokens: int
    completion_tokens: int
    latency_ms: float
    error: Optional[str] = None

def _estimate_tokens(text: str) -> int:
    if not text:
        return 0
    return max(1, len(text.split()))
