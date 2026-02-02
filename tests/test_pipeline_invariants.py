from __future__ import annotations

import csv

from src import pipeline, utils
from src.batch_writer import build_batch_outputs
from src.ocr_normalizer import (
    InvoiceInfo,
    InvoiceLine,
    NormalizedInvoice,
    SourceMeta,
    Supplier,
    Totals,
)


def _normalized_invoice(
    doc_id: str,
    *,
    supplier_nif: str,
    base: float,
    vat: float,
    gross: float,
    forced_issues: list[str] | None = None,
) -> NormalizedInvoice:
    return NormalizedInvoice(
        doc_id=doc_id,
        tenant="demo",
        supplier=Supplier(name="Proveedor Test", nif=supplier_nif),
        invoice=InvoiceInfo(number=f"INV-{doc_id}", date="2025-01-15", due="2025-02-15", currency="EUR"),
        totals=Totals(base=base, vat=vat, gross=gross),
        lines=[
            InvoiceLine(desc="Servicio", qty=1.0, unit_price=base, vat_rate=21.0, amount=base),
        ],
        confidence_ocr=0.95,
        source=SourceMeta(channel="test", filename=f"{doc_id}.pdf"),
        metadata={"doc_type": "invoice", "category": "servicios_prof", "flow": "AP", "forced_issues": forced_issues or []},
    )


def _process_and_batch(normalized: NormalizedInvoice):
    utils.insert_or_get_doc(normalized.doc_id, normalized.doc_id, normalized.source.filename, normalized.tenant)
    pipeline.process_normalized(normalized.doc_id, normalized.dict(), normalized.confidence_ocr)
    batch_dir = build_batch_outputs([normalized.doc_id], normalized.tenant, f"batch_{normalized.doc_id}")
    return batch_dir


def _incidence_rows(batch_dir):
    inc_path = batch_dir / "incidencias.csv"
    assert inc_path.exists()
    with inc_path.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return list(reader)


def _a3_rows(batch_dir):
    a3_path = batch_dir / "a3_asientos.csv"
    assert a3_path.exists()
    with a3_path.open("r", encoding="utf-8") as fh:
        reader = list(csv.reader(fh))
    return reader


def test_amount_mismatch_always_incidence(temp_certiva_env):
    normalized = _normalized_invoice("doc_mismatch", supplier_nif="B12345678", base=100.0, vat=21.0, gross=160.0)
    batch_dir = _process_and_batch(normalized)
    rows = _incidence_rows(batch_dir)
    assert any(row["doc_id"] == normalized.doc_id for row in rows)
    assert any("AMOUNT_MISMATCH" in (row["issues"] or "") for row in rows)
    a3_rows = _a3_rows(batch_dir)
    # Sólo cabecera porque ningún documento fue OK
    assert len(a3_rows) == 1


def test_missing_nif_cannot_post(temp_certiva_env):
    normalized = _normalized_invoice("doc_missing_nif", supplier_nif="", base=100.0, vat=21.0, gross=121.0)
    batch_dir = _process_and_batch(normalized)
    rows = _incidence_rows(batch_dir)
    assert any(row["doc_id"] == normalized.doc_id for row in rows)
    assert any("MISSING_SUPPLIER_NIF" in (row["issues"] or "") for row in rows)
    a3_rows = _a3_rows(batch_dir)
    assert len(a3_rows) == 1


def test_forced_issue_triggers_incidence(temp_certiva_env):
    normalized = _normalized_invoice(
        "doc_forced_issue",
        supplier_nif="B99887766",
        base=50.0,
        vat=10.5,
        gross=60.5,
        forced_issues=["OCR_PROVIDER_FALLBACK"],
    )
    batch_dir = _process_and_batch(normalized)
    rows = _incidence_rows(batch_dir)
    assert any("OCR_PROVIDER_FALLBACK" in (row["issues"] or "") for row in rows)
    a3_rows = _a3_rows(batch_dir)
    assert len(a3_rows) == 1
