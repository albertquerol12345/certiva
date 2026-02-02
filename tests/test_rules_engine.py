def _base_invoice():
    return {
        "supplier": {"name": "Iberdrola Comercialización", "nif": "A12345678"},
        "invoice": {"number": "IB-2025-001", "date": "2025-01-15", "due": "2025-02-15", "currency": "EUR"},
        "totals": {"base": 120.0, "vat": 25.2, "gross": 145.2},
        "metadata": {"category": "suministros", "doc_type": "invoice", "flow": "AP"},
        "tenant": "demo",
    }


def test_generate_entry_ap_invoice(temp_certiva_env):
    rules_engine = temp_certiva_env["rules_engine"]
    evaluation = rules_engine.generate_entry("doc-ap", _base_invoice())
    lines = evaluation.entry["lines"]
    accounts = {line["account"] for line in lines}
    assert "628000" in accounts  # gasto
    assert any(line["account"].startswith("472") for line in lines)
    assert "410000" in accounts
    assert evaluation.confidence_entry >= 0.7
    assert evaluation.entry["journal"] == "COMPRAS"


def test_generate_entry_ar_invoice(temp_certiva_env):
    rules_engine = temp_certiva_env["rules_engine"]
    invoice = _base_invoice()
    invoice["metadata"] = {"category": "ventas_servicios", "doc_type": "sales_invoice", "flow": "AR"}
    invoice["totals"] = {"base": 100.0, "vat": 21.0, "gross": 121.0}
    evaluation = rules_engine.generate_entry("doc-ar", invoice)
    lines = evaluation.entry["lines"]
    accounts = {line["account"] for line in lines}
    assert any(acc.startswith("70") for acc in accounts)
    assert any(acc.startswith("477") for acc in accounts)
    assert "430000" in accounts
    assert evaluation.entry["journal"] == "VENTAS"
