from src import utils


def _write_json(base_path, filename, data):
    path = base_path / "OUT" / "json" / filename
    utils.json_dump(data, path)


def test_match_transactions_with_partials(temp_certiva_env):
    bank_matcher = temp_certiva_env["bank_matcher"]
    base = temp_certiva_env["base"]
    doc_id = "sales-test-doc"
    normalized = {
        "supplier": {"name": "Cliente Demo", "nif": "B99887766"},
        "invoice": {"number": "VENT-001", "date": "2025-01-10", "due": "2025-01-25"},
        "totals": {"base": 82.64, "vat": 17.36, "gross": 100.0},
        "metadata": {"category": "ventas_servicios", "doc_type": "sales_invoice", "flow": "AR"},
        "tenant": "demo",
    }
    _write_json(base, f"{doc_id}.json", normalized)
    with utils.get_connection() as conn:
        conn.execute(
            """
            INSERT INTO docs(doc_id, filename, tenant, status, doc_type, reconciled_amount, reconciled_pct, paid_flag)
            VALUES(?, ?, ?, 'POSTED', ?, 0, 0, 0)
            """,
            (doc_id, "dummy.pdf", "demo", "sales_invoice"),
        )
        for idx, amount in enumerate((60.0, 40.0), start=1):
            conn.execute(
                """
                INSERT INTO bank_tx(tx_id, tenant, date, amount, currency, description, account_id, direction, raw, matched_doc_id)
                VALUES(?, ?, ?, ?, 'EUR', ?, ?, ?, '{}', NULL)
                """,
                (
                    f"tx{idx}",
                    "demo",
                    "2025-01-20",
                    amount,
                    f"Cobro VENT-001 ({idx})",
                    "ES11",
                    "CREDIT",
                ),
            )
    matched = bank_matcher.match_transactions("demo", amount_tolerance=0.05, date_window_days=10, score_threshold=0.1)
    assert matched == 1
    with utils.get_connection() as conn:
        matches = conn.execute("SELECT matched_amount FROM matches WHERE doc_id = ?", (doc_id,)).fetchall()
        assert sorted(round(row["matched_amount"], 2) for row in matches) == [40.0, 60.0]
        doc_row = conn.execute("SELECT reconciled_amount, reconciled_pct FROM docs WHERE doc_id = ?", (doc_id,)).fetchone()
        assert round(doc_row["reconciled_amount"], 2) == 100.0
        assert doc_row["reconciled_pct"] >= 0.99


def test_override_and_clear_match(temp_certiva_env):
    bank_matcher = temp_certiva_env["bank_matcher"]
    base = temp_certiva_env["base"]
    doc_id = "ap-override"
    tx_id = "tx-override"
    _write_json(
        base,
        f"{doc_id}.json",
        {
            "totals": {"gross": 50},
            "metadata": {"doc_type": "invoice", "flow": "AP", "date": "2025-01-10"},
        },
    )
    with utils.get_connection() as conn:
        conn.execute(
            "INSERT INTO docs(doc_id, filename, tenant, status, doc_type) VALUES(?,?,?,?,?)",
            (doc_id, "dummy.pdf", "demo", "POSTED", "invoice"),
        )
        conn.execute(
            """
            INSERT INTO bank_tx(tx_id, tenant, date, amount, currency, description, account_id, direction, raw, matched_doc_id)
            VALUES(?, 'demo', '2025-01-12', 50.0, 'EUR', 'Pago proveedor', 'ES11', 'DEBIT', '{}', NULL)
            """,
            (tx_id,),
        )
    # fuerza match
    bank_matcher.override_match(tx_id=tx_id, doc_id=doc_id, amount=50.0, tenant="demo")
    with utils.get_connection() as conn:
        match_row = conn.execute("SELECT matched_amount FROM matches WHERE tx_id = ?", (tx_id,)).fetchone()
        doc_row = conn.execute("SELECT reconciled_pct FROM docs WHERE doc_id = ?", (doc_id,)).fetchone()
        assert round(match_row["matched_amount"], 2) == 50.0
        assert doc_row["reconciled_pct"] >= 0.99
    # limpia match
    bank_matcher.clear_match(tx_id)
    with utils.get_connection() as conn:
        remaining = conn.execute("SELECT COUNT(*) FROM matches WHERE tx_id = ?", (tx_id,)).fetchone()[0]
        doc_row = conn.execute("SELECT reconciled_pct FROM docs WHERE doc_id = ?", (doc_id,)).fetchone()
        assert remaining == 0
        assert (doc_row["reconciled_pct"] or 0) == 0


def test_gather_bank_stats_rows(temp_certiva_env):
    bank_matcher = temp_certiva_env["bank_matcher"]
    base = temp_certiva_env["base"]
    utils = temp_certiva_env["utils"]
    doc_id = "ap-row-test"
    matched_tx = "tx-matched"
    unmatched_tx = "tx-unmatched"
    _write_json(
        base,
        f"{doc_id}.json",
        {"totals": {"gross": 25}, "metadata": {"doc_type": "invoice", "flow": "AP", "date": "2025-01-02"}},
    )
    with utils.get_connection() as conn:
        conn.execute(
            "INSERT INTO docs(doc_id, filename, tenant, status, doc_type) VALUES(?,?,?,?,?)",
            (doc_id, "dummy.pdf", "demo", "POSTED", "invoice"),
        )
        conn.execute(
            """
            INSERT INTO bank_tx(tx_id, tenant, date, amount, currency, description, account_id, direction, raw, matched_doc_id, tx_hash)
            VALUES(?, 'demo', '2025-01-05', 25.0, 'EUR', 'Pago 1', 'ES11', 'DEBIT', '{}', NULL, 'h1')
            """,
            (matched_tx,),
        )
        conn.execute(
            """
            INSERT INTO bank_tx(tx_id, tenant, date, amount, currency, description, account_id, direction, raw, matched_doc_id, tx_hash)
            VALUES(?, 'demo', '2025-01-06', 13.0, 'EUR', 'Pago 2', 'ES11', 'DEBIT', '{}', NULL, 'h2')
            """,
            (unmatched_tx,),
        )
    bank_matcher.override_match(tx_id=matched_tx, doc_id=doc_id, amount=25.0, tenant="demo")
    stats = bank_matcher.gather_bank_stats(tenant="demo", include_rows=True, limit=10)
    assert stats["tx_total"] == 2
    assert stats["tx_matched"] == 1
    assert len(stats.get("matches") or []) == 1
    assert len(stats.get("unmatched") or []) == 1
