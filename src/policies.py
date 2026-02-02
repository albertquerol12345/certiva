from __future__ import annotations

import logging
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List

import yaml

from . import utils
from .config import settings

logger = logging.getLogger(__name__)


def _current_root() -> Path:
    return utils.BASE_DIR / "config" / "tenants"


POLICIES_ROOT = _current_root()


def _base_policy() -> Dict[str, Any]:
    return {
        "autopost_enabled": settings.app_env != "prod",
        "autopost_categories_safe": [],
        "llm_premium_threshold_gross": settings.llm_premium_threshold_gross,
        "canary_sample_pct": 0.0,
    }


def _normalize_policy(raw: Dict[str, Any]) -> Dict[str, Any]:
    policy = _base_policy()
    for key, value in raw.items():
        if value is None:
            continue
        if key == "autopost_categories_safe" and isinstance(value, list):
            policy[key] = [str(item).strip() for item in value if str(item).strip()]
        elif key == "autopost_enabled":
            policy[key] = bool(value)
        elif key == "canary_sample_pct":
            try:
                policy[key] = max(0.0, min(float(value), 1.0))
            except (TypeError, ValueError):
                logger.warning("Policy valor inválido para canary_sample_pct: %s", value)
        elif key == "llm_premium_threshold_gross":
            try:
                policy[key] = float(value)
            except (TypeError, ValueError):
                logger.warning("Policy valor inválido para threshold gross: %s", value)
        else:
            policy[key] = value
    return policy


@lru_cache()
def get_tenant_policy(tenant: str) -> Dict[str, Any]:
    tenant_key = (tenant or settings.default_tenant).lower()
    policy_root = _current_root()
    policy_path = policy_root / tenant_key / "policies.yaml"
    if not policy_path.exists():
        return _base_policy()
    try:
        data = yaml.safe_load(policy_path.read_text(encoding="utf-8")) or {}
        if not isinstance(data, dict):
            logger.warning("Policy YAML inválido para %s, usando defaults", tenant_key)
            return _base_policy()
        return _normalize_policy(data)
    except Exception as exc:  # pragma: no cover - YAML corrupto
        logger.warning("No se pudo cargar policies.yaml para %s: %s", tenant_key, exc)
        return _base_policy()


def clear_policy_cache() -> None:
    global POLICIES_ROOT
    POLICIES_ROOT = _current_root()
    get_tenant_policy.cache_clear()


__all__ = ["get_tenant_policy", "clear_policy_cache", "POLICIES_ROOT"]
