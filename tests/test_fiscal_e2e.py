import importlib
from types import SimpleNamespace

from tests.test_reports import _seed_reporting_docs

from src import efactura_payloads, facturae_export, fiscal, sii_export, utils


def test_fiscal_cli_exports_files(temp_certiva_env):
    base = temp_certiva_env["base"]
    _seed_reporting_docs(temp_certiva_env)
    importlib.reload(sii_export)
    importlib.reload(facturae_export)
    importlib.reload(efactura_payloads)
    fiscal_module = importlib.reload(fiscal)

    fiscal_module.cmd_export_sii(SimpleNamespace(tenant="demo", date_from="2025-01-01", date_to="2025-02-28"))
    sii_dir = base / "OUT" / "sii"
    assert any(path.suffix == ".json" for path in sii_dir.glob("*.json"))

    fiscal_module.cmd_export_facturae(SimpleNamespace(doc_id="ar-001"))
    facturae_dir = base / "OUT" / "facturae"
    assert (facturae_dir / "ar-001.xml").exists()

    fiscal_module.cmd_export_verifactu(SimpleNamespace(doc_id="ar-001", action="ALTA"))
    ef_dir = base / "OUT" / "efactura"
    assert any("verifactu" in path.name for path in ef_dir.glob("*.json"))
