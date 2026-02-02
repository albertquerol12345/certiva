from tests.test_reports import _seed_reporting_docs

from src import sii_export


def test_export_sii_period_contains_records(temp_certiva_env):
    reports_module = temp_certiva_env["reports"]
    _seed_reporting_docs(temp_certiva_env)
    payload = sii_export.export_sii_period("demo", "2025-01-01", "2025-03-31")
    assert payload["Registros"]
    libros = {registro["LibroRegistro"] for registro in payload["Registros"]}
    assert libros  # contiene emitidas o recibidas
