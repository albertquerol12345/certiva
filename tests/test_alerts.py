from __future__ import annotations

import json

from src import alerts, utils, provider_health, config


def test_evaluate_alerts_flags_review_and_batch(monkeypatch, temp_certiva_env):
    # Umbrales bajos para activar alertas
    monkeypatch.setattr(config.settings, "alert_review_queue_threshold", 0, raising=False)
    monkeypatch.setattr(config.settings, "alert_batch_warning", True, raising=False)
    monkeypatch.setattr(config.settings, "alert_zero_page_threshold", 0, raising=False)
    monkeypatch.setattr(config.settings, "ocr_breaker_threshold", 1, raising=False)
    utils.add_review_item("doc-alert", reason="TEST", suggested=None, tenant="demo")
    with utils.get_connection() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO docs(doc_id, filename, tenant, status, doc_type, issues)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            ("doc-alert", "dummy.pdf", "demo", "REVIEW_PENDING", "invoice", json.dumps(["BATCH_MISSING_PAGES"])),
        )
    provider_health.record_failure("ocr", "azure")  # marca degradado
    alerts_list = alerts.evaluate_alerts(tenant="demo")
    assert any("HITL pendiente" in msg for msg in alerts_list)
    assert any("Batch warnings" in msg for msg in alerts_list)
    assert any("degradados" in msg for msg in alerts_list)


def test_send_alerts_logs_and_webhook(monkeypatch):
    sent = {"called": False}

    def fake_post(url, json=None, timeout=5.0):
        sent["called"] = True
        assert "text" in (json or {})
        return None

    monkeypatch.setattr(config.settings, "alert_webhook_url", "http://example.com", raising=False)
    monkeypatch.setattr("src.alerts.httpx.post", fake_post)
    ok = alerts.send_alerts(["Alerta demo"], tenant="demo")
    assert ok is True
    assert sent["called"] is True
