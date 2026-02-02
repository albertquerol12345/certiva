import json

from src import utils


def _create_doc(
    base,
    doc_id: str,
    *,
    doc_type: str,
    tenant: str,
    totals,
    metadata,
    invoice,
    entry_lines,
    issues=None,
    reconciled_pct: float = 0.0,
):
    normalized = {
        "supplier": {"name": metadata.get("counterparty_name", "Proveedor Demo"), "nif": metadata.get("counterparty_nif", "A00000000")},
        "invoice": invoice,
        "totals": totals,
        "metadata": metadata,
        "tenant": tenant,
    }
    utils.json_dump(normalized, base / "OUT" / "json" / f"{doc_id}.json")
    utils.json_dump({"lines": entry_lines}, base / "OUT" / "json" / f"{doc_id}.entry.json")
    with utils.get_connection() as conn:
        conn.execute(
            """
            INSERT INTO docs(doc_id, filename, tenant, status, doc_type, issues, ocr_conf, entry_conf, global_conf, reconciled_amount, reconciled_pct)
            VALUES(?, ?, ?, 'POSTED', ?, ?, 0.99, 0.95, 0.95, ?, ?)
            """,
            (
                doc_id,
                f"{doc_id}.pdf",
                tenant,
                doc_type,
                json.dumps(issues or []),
                float(totals.get("gross", 0) * reconciled_pct),
                reconciled_pct,
            ),
        )


def _seed_reporting_docs(env):
    base = env["base"]
    _create_doc(
        base,
        "ap-001",
        doc_type="invoice",
        tenant="demo",
        totals={"base": 100.0, "vat": 21.0, "gross": 121.0},
        metadata={"category": "suministros", "doc_type": "invoice", "flow": "AP", "vat_rate": 21, "counterparty_name": "Proveedor Uno", "counterparty_nif": "A11111111"},
        invoice={"number": "AP-001", "date": "2025-01-05", "due": "2025-01-20"},
        entry_lines=[
            {"account": "628000", "debit": 100.0, "credit": 0.0},
            {"account": "472000", "debit": 21.0, "credit": 0.0},
            {"account": "410000", "debit": 0.0, "credit": 121.0},
        ],
        issues=["NO_RULE"],
    )
    _create_doc(
        base,
        "ar-001",
        doc_type="sales_invoice",
        tenant="demo",
        totals={"base": 150.0, "vat": 31.5, "gross": 181.5},
        metadata={"category": "ventas_servicios", "doc_type": "sales_invoice", "flow": "AR", "vat_rate": 21, "counterparty_name": "Cliente Demo", "counterparty_nif": "B22222222"},
        invoice={"number": "AR-001", "date": "2025-01-10", "due": "2025-02-15"},
        entry_lines=[
            {"account": "700000", "debit": 0.0, "credit": 150.0},
            {"account": "477000", "debit": 0.0, "credit": 31.5},
            {"account": "430000", "debit": 181.5, "credit": 0.0},
        ],
        reconciled_pct=0.0,
    )


def test_reporting_builders(temp_certiva_env):
    reports = temp_certiva_env["reports"]
    _seed_reporting_docs(temp_certiva_env)

    pnl = reports.build_pnl("demo", "2025-01-01", "2025-02-28")
    assert pnl["total_income"] > 0
    assert pnl["total_expense"] > 0
    assert "INGRESOS" in pnl["groups"]

    vat = reports.build_vat_report("demo", "2025-01-01", "2025-02-28")
    assert vat["soportado"]["21"]["vat"] > 0
    assert vat["repercutido"]["21"]["vat"] > 0

    aging = reports.build_aging("demo", "2025-03-01", "AR")
    total_aging = sum(bucket["importe"] for bucket in aging["buckets"].values())
    assert round(total_aging, 2) == 181.5

    cashflow = reports.build_cashflow_forecast("demo", "2025-02-01", 3)
    assert any(bucket["in"] > 0 for bucket in cashflow["buckets"])
