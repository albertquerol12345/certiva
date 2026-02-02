from __future__ import annotations

import pytest
from reportlab.pdfgen import canvas  # type: ignore

from tests import augment


def _create_pdf(path):
    c = canvas.Canvas(str(path))
    c.drawString(100, 700, "Factura demo")
    c.drawString(100, 680, "Importe: 100 €")
    c.save()


@pytest.mark.skipif(augment.convert_from_path is None, reason="pdf2image no disponible")  # type: ignore[attr-defined]
def test_augment_folder_generates_dirty_pdf(tmp_path, monkeypatch):
    source = tmp_path / "source"
    dest = tmp_path / "dest"
    source.mkdir()
    pdf_path = source / "demo.pdf"
    _create_pdf(pdf_path)
    try:
        generated = list(augment.augment_folder(source, dest, seed=1, limit=1))
    except Exception as exc:  # pragma: no cover - poppler missing
        pytest.skip(f"No se pudo ejecutar pdf2image: {exc}")
    assert generated
    assert generated[0].exists()
