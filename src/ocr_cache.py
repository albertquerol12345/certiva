from __future__ import annotations

import json
import os
from dataclasses import asdict
from pathlib import Path
from typing import Optional, TYPE_CHECKING

BASE_DIR = Path(__file__).resolve().parents[1]

if TYPE_CHECKING:  # pragma: no cover
    from .ocr_providers import OCRResult

_DEFAULT_DIR = Path(os.getenv("AZURE_OCR_CACHE_DIR", str(BASE_DIR / "OUT" / "ocr_cache")))
_DEFAULT_ENABLED = os.getenv("AZURE_OCR_ENABLE_CACHE", "1") not in {"0", "false", "False"}

_cache_dir: Path = _DEFAULT_DIR
_cache_enabled: bool = _DEFAULT_ENABLED


def configure(directory: Optional[Path] = None, enabled: Optional[bool] = None) -> None:
    """Allow runtime customization of the OCR cache settings."""
    global _cache_dir, _cache_enabled
    if directory is not None:
        _cache_dir = Path(directory)
    if enabled is not None:
        _cache_enabled = bool(enabled)
    if _cache_enabled:
        _cache_dir.mkdir(parents=True, exist_ok=True)


def _path_for(doc_hash: str) -> Path:
    return _cache_dir / f"{doc_hash}.json"


def get_cached(doc_hash: str) -> Optional["OCRResult"]:
    """Return a cached OCRResult if available."""
    if not _cache_enabled:
        return None
    path = _path_for(doc_hash)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    from .ocr_providers import OCRResult  # local import to avoid circular dependency

    try:
        return OCRResult(**payload)
    except Exception:
        return None


def put_cached(doc_hash: str, result: "OCRResult") -> None:
    """Persist the OCRResult for future runs."""
    if not _cache_enabled:
        return
    _cache_dir.mkdir(parents=True, exist_ok=True)
    path = _path_for(doc_hash)
    payload = asdict(result)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
