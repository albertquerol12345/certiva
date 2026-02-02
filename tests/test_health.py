import importlib

from fastapi.testclient import TestClient

from src import config, health, utils


def _reload_app():
    import src.webapp as webapp_module

    return importlib.reload(webapp_module).app


def test_readyz_ok(temp_certiva_env):
    config.settings.web_session_secret = "test-secret"
    config.settings.session_cookie_secure = False
    config.settings.web_allowed_origin = "http://testserver"
    app = _reload_app()
    client = TestClient(app)
    resp = client.get("/readyz")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ready"


def test_readyz_db_failure(temp_certiva_env, monkeypatch):
    config.settings.web_session_secret = "test-secret"
    config.settings.session_cookie_secure = False
    config.settings.web_allowed_origin = "http://testserver"
    monkeypatch.setattr(health, "utils", utils, raising=False)
    original_get_connection = utils.get_connection

    def broken_get_connection():
        raise RuntimeError("db offline")

    monkeypatch.setattr(health.utils, "get_connection", broken_get_connection)
    app = _reload_app()
    client = TestClient(app)
    resp = client.get("/readyz")
    assert resp.status_code == 503
    assert "db" in resp.json()["detail"]
