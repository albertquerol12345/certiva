from __future__ import annotations

from pathlib import Path

from src import a3_validator
from src.exporter import A3_CSV_COLUMNS


def _write_csv(path: Path, rows):
    path.write_text("\n".join(",".join(map(str, row)) for row in rows), encoding="utf-8")


def test_validate_a3_csv_detects_bad_header(tmp_path):
    csv_path = tmp_path / "a3.csv"
    csv_path.write_text("A,B,C\n1,2,3", encoding="utf-8")
    errors = a3_validator.validate_a3_csv(csv_path)
    assert errors and errors[0][1] == "Cabecera"


def test_validate_a3_csv_detects_row_errors(tmp_path):
    csv_path = tmp_path / "a3_valid.csv"
    header = A3_CSV_COLUMNS
    rows = [header, ["2025-01-01", "COMPRAS", "INV1", "600000", "100.00", "0.00", "Concepto", "B12345678"]]
    _write_csv(csv_path, rows)
    assert a3_validator.validate_a3_csv(csv_path) == []
    bad_csv = tmp_path / "a3_bad.csv"
    rows = [
        header,
        ["01-01-2025", "", "", "ABC", "foo", "bar", "", "123"],
    ]
    _write_csv(bad_csv, rows)
    errors = a3_validator.validate_a3_csv(bad_csv)
    fields = {err[1] for err in errors}
    assert {"Fecha", "Diario", "Documento", "Cuenta", "Debe", "Haber", "Concepto", "NIF"}.issubset(fields)
