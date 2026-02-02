"""Utilities for recording LLM prompts/responses in a PII-safe way."""
from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any, Dict

from .config import BASE_DIR, settings
from .pii_scrub import scrub_pii

_DEBUG_ROOT = BASE_DIR / "OUT" / "debug"


def _sanitize(obj: Any) -> Any:
    if isinstance(obj, str):
        return scrub_pii(
            obj,
            strict=settings.llm_pii_scrub_strict,
            enabled=getattr(settings, "llm_debug_redact_pii", True),
        )
    if isinstance(obj, dict):
        return {key: _sanitize(value) for key, value in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(item) for item in obj]
    return obj


def is_enabled() -> bool:
    return bool(getattr(settings, "debug_llm", False))


def record(doc_id: str, tenant: str, payload: Dict[str, Any]) -> None:
    if not is_enabled():
        return
    target = _DEBUG_ROOT / doc_id
    target.mkdir(parents=True, exist_ok=True)
    for name, content in payload.items():
        sanitized = _sanitize(content)
        path = target / f"{name}.json"
        path.write_text(json.dumps(sanitized, ensure_ascii=False, indent=2), encoding="utf-8")


def copy_into_batch(doc_id: str, batch_dir: Path) -> None:
    source = _DEBUG_ROOT / doc_id
    if not source.exists():
        return
    dest = batch_dir / doc_id / "debug"
    if dest.exists():
        shutil.rmtree(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source, dest)
