from src import exporter
from src.config import get_tenant_config, settings


def _sample_entry():
    return {
        "date": "2025-01-01",
        "invoice_number": "INV-1",
        "journal": "COMPRAS",
        "lines": [
            {"account": "600000", "debit": "100.00", "credit": "0.00", "concept": "Compra", "nif": "B12345678"},
            {"account": "410000", "debit": "0.00", "credit": "100.00", "concept": "Proveedor", "nif": "B12345678"},
        ],
        "supplier": {"name": "Proveedor", "nif": "B12345678"},
    }


def test_a3_csv_contains_expected_header(tmp_path, monkeypatch):
    monkeypatch.setattr(exporter.utils, "BASE_DIR", tmp_path, raising=False)
    entry = _sample_entry()
    adapter = exporter.A3InnuvaAdapter(settings.default_tenant, get_tenant_config(settings.default_tenant))
    csv_path = adapter.export_entry("doc123", entry)
    content = csv_path.read_text(encoding="utf-8").splitlines()
    assert content[0].split(",") == exporter.A3_CSV_COLUMNS
    assert "INV-1" in content[1]
