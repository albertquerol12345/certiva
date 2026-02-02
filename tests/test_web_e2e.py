import importlib
import re

import pytest
from fastapi.testclient import TestClient

from src import auth, config

CSRF_RE = re.compile(r'name="_csrf_token"\s+value="([^"]+)"')


def _extract_csrf(response_text: str) -> str:
    match = CSRF_RE.search(response_text)
    assert match, "CSRF token no encontrado"
    return match.group(1)


@pytest.fixture
def web_client(temp_certiva_env, monkeypatch):
    config.settings.web_session_secret = "test-secret"
    config.settings.session_cookie_secure = False
    config.settings.web_allowed_origin = "http://testserver"
    monkeypatch.setattr(auth, "hash_password", lambda pwd: pwd)
    monkeypatch.setattr(auth, "verify_password", lambda plain, hashed: plain == hashed)
    import src.webapp as webapp_module

    webapp = importlib.reload(webapp_module)
    client = TestClient(webapp.app)
    utils = temp_certiva_env["utils"]
    utils.create_user("admin", auth.hash_password("secret"), role="admin", is_active=True)
    yield client, webapp


def _login(client: TestClient):
    resp = client.get("/login")
    token = _extract_csrf(resp.text)
    resp = client.post(
        "/login",
        data={"_csrf_token": token, "username": "admin", "password": "secret"},
        headers={"origin": "http://testserver"},
        follow_redirects=False,
    )
    assert resp.status_code == 303


def test_login_and_dashboard(web_client):
    client, _ = web_client
    _login(client)
    resp = client.get("/", follow_redirects=True)
    assert resp.status_code == 200
    assert "Dashboard" in resp.text


def test_review_requires_login_then_allows_access(web_client):
    client, _ = web_client
    resp = client.get("/review", follow_redirects=False)
    assert resp.status_code == 303
    assert resp.headers["location"] == "/login"
    _login(client)
    resp = client.get("/review")
    assert resp.status_code == 200
    assert "Revisión HITL" in resp.text


def test_assistant_flow_uses_llm_stub(web_client, monkeypatch):
    client, webapp = web_client
    _login(client)
    resp = client.get("/assistant")
    token = _extract_csrf(resp.text)

    def fake_answer(question: str, tenant: str | None = None, user: str | None = None):
        return "Respuesta simulada"

    monkeypatch.setattr(webapp.rag_normativo, "answer_normative_question", fake_answer)
    resp = client.post(
        "/assistant",
        data={"_csrf_token": token, "question": "¿Qué es el IVA?", "doc_id": "", "doc_action": "issues"},
        headers={"origin": "http://testserver"},
    )
    assert resp.status_code == 200
    assert "Respuesta simulada" in resp.text


def test_demo_upload_page_requires_login(web_client):
    client, _ = web_client
    resp = client.get("/demo-upload", follow_redirects=False)
    assert resp.status_code == 303
    _login(client)
    resp = client.get("/demo-upload")
    assert resp.status_code == 200
