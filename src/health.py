"""Readiness utilities for FastAPI endpoints."""
from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Dict

from . import utils

CHECK_DIRS = [
    ("inbox", Path(utils.BASE_DIR / "IN")),
    ("out_json", Path(utils.BASE_DIR / "OUT" / "json")),
    ("out_csv", Path(utils.BASE_DIR / "OUT" / "csv")),
    ("out_logs", Path(utils.BASE_DIR / "OUT" / "logs")),
]


class ReadinessError(Exception):
    def __init__(self, details: Dict[str, str]):
        super().__init__("Readiness checks failed")
        self.details = details


def _check_db(details: Dict[str, str]) -> None:
    try:
        with utils.get_connection() as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS readiness_probe(ts TEXT)")
            conn.execute("DELETE FROM readiness_probe")
            conn.execute("INSERT INTO readiness_probe(ts) VALUES (?)", (utils.iso_now(),))
        details["db"] = "ok"
    except Exception as exc:
        details["db"] = f"error: {exc}"
        raise ReadinessError(details)


def _check_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path, delete=True) as tmp:
        tmp.write(b"ready")
        tmp.flush()


def check_readiness() -> Dict[str, str]:
    details: Dict[str, str] = {}
    _check_db(details)
    for label, path in CHECK_DIRS:
        try:
            _check_directory(path)
            details[label] = "ok"
        except Exception as exc:
            details[label] = f"error: {exc}"
            raise ReadinessError(details)
    return details
