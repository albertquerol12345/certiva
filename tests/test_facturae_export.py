import importlib
import json

import pytest

from tests.test_reports import _seed_reporting_docs

from src import facturae_export, utils


def test_facturae_xml_contains_basic_tags(temp_certiva_env):
    reports_module = temp_certiva_env["reports"]
    _seed_reporting_docs(temp_certiva_env)
    module = importlib.reload(facturae_export)
    # use AR doc from seed
    doc_id = "ar-001"
    xml = module.build_facturae_xml(doc_id)
    assert "<Facturae" in xml
    assert "<InvoiceNumber>" in xml
    assert "AR-001" in xml or "ar-001" in xml


def test_write_facturae_validates_and_writes(temp_certiva_env):
    _seed_reporting_docs(temp_certiva_env)
    module = importlib.reload(facturae_export)
    path = module.write_facturae_file("ar-001")
    assert path.exists()


def test_write_facturae_detects_incoherent_totals(temp_certiva_env):
    base = temp_certiva_env["base"]
    _seed_reporting_docs(temp_certiva_env)
    module = importlib.reload(facturae_export)
    json_path = base / "OUT" / "json" / "ar-001.json"
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    payload["totals"]["gross"] = 999.99
    json_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(ValueError):
        module.write_facturae_file("ar-001")
