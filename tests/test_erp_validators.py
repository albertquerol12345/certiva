from __future__ import annotations

from src import erp_validators


def test_validate_holded_payload_detects_missing_fields():
    payload = {"contact": {"name": "", "tax_id": "B1"}, "lines": [], "totals": {"gross": 10}}
    errors = erp_validators.validate_holded_payload(payload)
    assert any("contact.name" in e[0] for e in errors)
    assert any("tax_id" in e[0] for e in errors)
    assert any(e[0] == "lines" for e in errors)


def test_validate_holded_payload_ok_sum():
    payload = {
        "contact": {"name": "Cliente Demo", "tax_id": "B12345678"},
        "lines": [
            {"account": "700000", "amount": 5},
            {"account": "700100", "amount": 5},
        ],
        "totals": {"gross": 10},
    }
    errors = erp_validators.validate_holded_payload(payload)
    assert errors == []
