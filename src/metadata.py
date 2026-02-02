"""Helpers to read optional manifest data (category, tenant, etc.) for sample docs."""
from __future__ import annotations

import csv
from functools import lru_cache
from pathlib import Path
from typing import Dict, Optional

from .config import BASE_DIR, settings

DEFAULT_MANIFEST = (
    Path(settings.manifest_path) if settings.manifest_path else BASE_DIR / "tests" / "golden_manifest.csv"
)


@lru_cache()
def _load_manifest(path: Path = DEFAULT_MANIFEST) -> Dict[str, Dict[str, str]]:
    if not path.exists():
        return {}
    data: Dict[str, Dict[str, str]] = {}
    with path.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            filename = row.get("filename")
            if not filename:
                continue
            data[filename] = {k: v for k, v in row.items() if v not in (None, "")}
    return data


def _normalize_name(filename: str) -> str:
    if filename.endswith("-dirty.pdf"):
        return filename.replace("-dirty.pdf", ".pdf")
    return filename


def get_metadata_for_file(filename: str) -> Optional[Dict[str, str]]:
    manifest = _load_manifest()
    if not manifest:
        return None
    filename = filename.strip()
    if filename in manifest:
        return dict(manifest[filename])
    normalized = _normalize_name(filename)
    if normalized in manifest:
        return dict(manifest[normalized])
    return None
