import re

import pytest
from fastapi.testclient import TestClient

from src import auth


def _extract_csrf(html: str) -> str:
    match = re.search(r'name="_csrf_token" value="([^"]+)"', html)
    assert match, "CSRF token not found in response"
    return match.group(1)


def _login(client: TestClient, username: str, password: str) -> None:
    # Primer GET para generar sesión y token
    resp = client.get("/login")
    assert resp.status_code == 200
    csrf = _extract_csrf(resp.text)
    resp = client.post(
        "/login",
        data={"username": username, "password": password, "_csrf_token": csrf},
        headers={"origin": "http://testserver"},
        follow_redirects=False,
    )
    assert resp.status_code in (302, 303)


@pytest.fixture
def web_client(temp_certiva_env, monkeypatch):
    """
    Crea un TestClient con DB temporal y origen permitido.
    """
    import importlib

    # Ajusta origen permitido para que pase verify_origin
    from src import config, utils

    monkeypatch.setattr(config.settings, "web_allowed_origin", "http://testserver", raising=False)
    monkeypatch.setattr(config.settings, "session_cookie_secure", False, raising=False)
    # Recarga webapp para usar BASE_DIR temporal del fixture
    import src.webapp as webapp_module

    webapp = importlib.reload(webapp_module)
    app = webapp.app
    client = TestClient(app)
    return client


def test_conciliacion_force_and_clear_requires_csrf(temp_certiva_env, web_client):
    utils = temp_certiva_env["utils"]
    from src import auth as auth_module

    username = "admin"
    password = "dummy"
    utils.create_user(username, auth_module.hash_password(password), role="admin", is_active=True)

    # Inserta doc y tx
    doc_id = "doc-web"
    tx_id = "tx-web"
    with utils.get_connection() as conn:
        conn.execute(
            "INSERT INTO docs(doc_id, filename, tenant, status, doc_type) VALUES(?,?,?,?,?)",
            (doc_id, "dummy.pdf", "demo", "POSTED", "invoice"),
        )
        conn.execute(
            """
            INSERT INTO bank_tx(tx_id, tenant, date, amount, currency, description, account_id, direction, raw, matched_doc_id, tx_hash)
            VALUES(?, 'demo', '2025-01-10', 20.0, 'EUR', 'Pago web', 'ES11', 'DEBIT', '{}', NULL, 'hash-web')
            """,
            (tx_id,),
        )

    # Login y obtén csrf de la página de conciliación
    _login(web_client, username, password)
    resp = web_client.get("/conciliacion")
    assert resp.status_code == 200
    csrf = _extract_csrf(resp.text)

    # CSRF incorrecto → 400
    bad = web_client.post(
        "/conciliacion/force",
        data={"tx_id": tx_id, "doc_id": doc_id, "_csrf_token": "bad"},
        headers={"origin": "http://testserver"},
        follow_redirects=False,
    )
    assert bad.status_code == 400

    # CSRF correcto → 303 y match creado
    ok = web_client.post(
        "/conciliacion/force",
        data={"tx_id": tx_id, "doc_id": doc_id, "amount": 20.0, "_csrf_token": csrf},
        headers={"origin": "http://testserver"},
        follow_redirects=False,
    )
    assert ok.status_code in (302, 303)
    with utils.get_connection() as conn:
        row = conn.execute("SELECT matched_doc_id FROM bank_tx WHERE tx_id = ?", (tx_id,)).fetchone()
        assert row["matched_doc_id"] == doc_id

    # Clear con csrf correcto
    resp = web_client.post(
        "/conciliacion/clear",
        data={"tx_id": tx_id, "_csrf_token": csrf},
        headers={"origin": "http://testserver"},
        follow_redirects=False,
    )
    assert resp.status_code in (302, 303)
    with utils.get_connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM matches WHERE tx_id = ?", (tx_id,)).fetchone()[0]
        assert count == 0
        doc_row = conn.execute("SELECT reconciled_pct FROM docs WHERE doc_id = ?", (doc_id,)).fetchone()
        assert (doc_row["reconciled_pct"] or 0) == 0
