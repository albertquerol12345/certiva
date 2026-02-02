import re

import pytest
from fastapi.testclient import TestClient

from src import auth


def _extract_csrf(html: str) -> str:
    match = re.search(r'name="_csrf_token" value="([^"]+)"', html)
    assert match, "CSRF token not found"
    return match.group(1)


def _login(client: TestClient, username: str, password: str) -> None:
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
    import importlib

    from src import config

    monkeypatch.setattr(config.settings, "web_allowed_origin", "http://testserver", raising=False)
    monkeypatch.setattr(config.settings, "session_cookie_secure", False, raising=False)
    import src.webapp as webapp_module

    app = importlib.reload(webapp_module).app
    return TestClient(app)


def test_review_bulk_duplicate_respects_csrf(temp_certiva_env, web_client):
    utils = temp_certiva_env["utils"]

    # Usuario admin
    utils.create_user("admin", auth.hash_password("secret"), role="admin", is_active=True)

    # Doc y review_queue
    doc_id = "doc-bulk"
    utils.json_dump(
        {
            "supplier": {"name": "Proveedor", "nif": "B123"},
            "invoice": {"number": "F001", "date": "2025-01-10"},
            "totals": {"gross": 10},
            "metadata": {"doc_type": "invoice", "flow": "AP"},
        },
        temp_certiva_env["base"] / "OUT" / "json" / f"{doc_id}.json",
    )
    with utils.get_connection() as conn:
        conn.execute(
            "INSERT INTO docs(doc_id, filename, tenant, status, doc_type, duplicate_flag) VALUES(?,?,?,?,?,0)",
            (doc_id, "dummy.pdf", "demo", "REVIEW_PENDING", "invoice"),
        )
    utils.add_review_item(doc_id, reason="TEST", suggested=None, tenant="demo")

    _login(web_client, "admin", "secret")
    resp = web_client.get("/review")
    assert resp.status_code == 200
    csrf = _extract_csrf(resp.text)

    # CSRF inválido
    bad = web_client.post(
        "/review/bulk",
        data={"action": "duplicate", "doc_ids": [doc_id], "_csrf_token": "bad"},
        headers={"origin": "http://testserver"},
        follow_redirects=False,
    )
    assert bad.status_code == 400

    ok = web_client.post(
        "/review/bulk",
        data={"action": "duplicate", "doc_ids": [doc_id], "_csrf_token": csrf},
        headers={"origin": "http://testserver"},
        follow_redirects=False,
    )
    assert ok.status_code in (302, 303)
    doc_row = utils.get_doc(doc_id)
    assert doc_row["status"] == "ERROR"
    assert (doc_row["duplicate_flag"] or 0) == 1


def test_review_bulk_reprocess_calls_pipeline(temp_certiva_env, web_client, monkeypatch):
    utils = temp_certiva_env["utils"]
    called = {"count": 0}

    def fake_reprocess(doc_id: str) -> None:
        called["count"] += 1

    monkeypatch.setattr("src.hitl_service.pipeline.reprocess_from_json", fake_reprocess)

    utils.create_user("admin", auth.hash_password("secret"), role="admin", is_active=True)
    doc_id = "doc-reprocess"
    utils.json_dump(
        {
            "supplier": {"name": "Proveedor2", "nif": "B456"},
            "invoice": {"number": "F002", "date": "2025-02-10"},
            "totals": {"gross": 12},
            "metadata": {"doc_type": "invoice", "flow": "AP"},
        },
        temp_certiva_env["base"] / "OUT" / "json" / f"{doc_id}.json",
    )
    with utils.get_connection() as conn:
        conn.execute(
            "INSERT INTO docs(doc_id, filename, tenant, status, doc_type, duplicate_flag) VALUES(?,?,?,?,?,0)",
            (doc_id, "dummy.pdf", "demo", "REVIEW_PENDING", "invoice"),
        )
    utils.add_review_item(doc_id, reason="TEST", suggested=None, tenant="demo")

    _login(web_client, "admin", "secret")
    resp = web_client.get("/review")
    csrf = _extract_csrf(resp.text)

    resp = web_client.post(
        "/review/bulk",
        data={"action": "reprocess", "doc_ids": [doc_id], "_csrf_token": csrf},
        headers={"origin": "http://testserver"},
        follow_redirects=False,
    )
    assert resp.status_code in (302, 303)
    assert called["count"] == 1


def test_review_bulk_accept_triggers_reprocess(temp_certiva_env, web_client, monkeypatch):
    utils = temp_certiva_env["utils"]
    called = {"count": 0}

    def fake_reprocess(doc_id: str) -> None:
        called["count"] += 1

    monkeypatch.setattr("src.hitl_service.pipeline.reprocess_from_json", fake_reprocess)

    utils.create_user("admin", auth.hash_password("secret"), role="admin", is_active=True)
    doc_id = "doc-accept"
    utils.json_dump(
        {
            "supplier": {"name": "Proveedor3", "nif": "B789"},
            "invoice": {"number": "F003", "date": "2025-03-10"},
            "totals": {"gross": 15},
            "metadata": {"doc_type": "invoice", "flow": "AP"},
        },
        temp_certiva_env["base"] / "OUT" / "json" / f"{doc_id}.json",
    )
    utils.json_dump({"lines": [{"account": "600000", "vat_rate": 21}]}, temp_certiva_env["base"] / "OUT" / "json" / f"{doc_id}.entry.json")
    with utils.get_connection() as conn:
        conn.execute(
            "INSERT INTO docs(doc_id, filename, tenant, status, doc_type, duplicate_flag) VALUES(?,?,?,?,?,0)",
            (doc_id, "dummy.pdf", "demo", "REVIEW_PENDING", "invoice"),
        )
    utils.add_review_item(doc_id, reason="TEST", suggested=None, tenant="demo")

    _login(web_client, "admin", "secret")
    resp = web_client.get("/review")
    csrf = _extract_csrf(resp.text)

    resp = web_client.post(
        "/review/bulk",
        data={"action": "accept", "doc_ids": [doc_id], "_csrf_token": csrf},
        headers={"origin": "http://testserver"},
        follow_redirects=False,
    )
    assert resp.status_code in (302, 303)
    assert called["count"] == 1
