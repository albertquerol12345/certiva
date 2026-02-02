import importlib

from fastapi.testclient import TestClient

from tests.test_reports import _seed_reporting_docs

from src import config, utils


def test_metrics_endpoint_returns_prometheus_text(temp_certiva_env, monkeypatch):
    config.settings.web_session_secret = "test-secret"
    config.settings.session_cookie_secure = False
    config.settings.web_allowed_origin = "http://testserver"
    _seed_reporting_docs(temp_certiva_env)
    with utils.get_connection() as conn:
        conn.execute("DELETE FROM llm_calls")
    utils.log_llm_call("rag_normativo", "dummy", "dummy", 10, 5, 12.0, None, tenant="demo", username="tester")

    import src.webapp as webapp_module

    app = importlib.reload(webapp_module).app
    client = TestClient(app)
    resp = client.get("/metrics")
    assert resp.status_code == 200
    body = resp.text
    assert "certiva_docs_total" in body
    assert "certiva_llm_calls_total" in body
