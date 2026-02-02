from __future__ import annotations

from pathlib import Path

import pytest

from tools import azure_probe
from src import azure_ocr_monitor


class DummyProvider:
    def __init__(self, *args, **kwargs):
        self.calls = 0

    def analyze_document(self, file_path: Path, tenant: str):
        self.calls += 1
        return None


def test_azure_probe_offline(monkeypatch, tmp_path):
    folder = tmp_path / "pdfs"
    folder.mkdir()
    for idx in range(3):
        (folder / f"{idx}.pdf").write_bytes(b"%PDF-1.4 dummy")
    azure_ocr_monitor.snapshot(reset=True)
    monkeypatch.setattr(azure_probe, "AzureOCRProvider", DummyProvider)
    report, ok = azure_probe.probe(folder, count=2, seed=42)
    assert "Documentos procesados: 2" in report
    assert ok
    assert (azure_probe.BASE_DIR / "OUT").exists()
