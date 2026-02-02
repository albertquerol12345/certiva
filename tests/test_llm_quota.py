from datetime import datetime, timedelta

import pytest

from src import llm_router, utils


def test_llm_quota_allows_calls_below_limit(temp_certiva_env, monkeypatch):
    monkeypatch.setattr(llm_router.settings, "llm_max_calls_tenant_daily", 5, raising=False)
    monkeypatch.setattr(llm_router.settings, "llm_max_calls_user_daily", 5, raising=False)
    with utils.get_connection() as conn:
        conn.execute("DELETE FROM llm_calls")
    response = llm_router.call_llm(
        llm_router.LLMTask.RAG_NORMATIVO,
        "Eres un asistente.",
        "Explica el IVA.",
        context="IVA soportado es deducible.",
        tenant="demo",
        user="tester",
    )
    assert "Simulación" in response  # dummy provider response
    with utils.get_connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM llm_calls").fetchone()[0]
    assert count == 1


def test_llm_quota_blocks_when_exceeded(temp_certiva_env, monkeypatch):
    monkeypatch.setattr(llm_router.settings, "llm_max_calls_tenant_daily", 1, raising=False)
    monkeypatch.setattr(llm_router.settings, "llm_max_calls_user_daily", 1, raising=False)
    with utils.get_connection() as conn:
        conn.execute("DELETE FROM llm_calls")
        conn.execute(
            """
            INSERT INTO llm_calls(task, provider, model, prompt_tokens, completion_tokens, latency_ms, tenant, username, created_at, error)
            VALUES(?, ?, ?, 0, 0, 0, ?, ?, ?, NULL)
            """,
            (
                "rag_normativo",
                "dummy",
                "dummy",
                "demo",
                "tester",
                (datetime.utcnow() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
    response = llm_router.call_llm(
        llm_router.LLMTask.RAG_NORMATIVO,
        "Eres un asistente.",
        "Explica el IVA.",
        context="IVA soportado es deducible.",
        tenant="demo",
        user="tester",
    )
    assert "Cuota diaria" in response
    with utils.get_connection() as conn:
        record = conn.execute(
            "SELECT provider, error FROM llm_calls ORDER BY id DESC LIMIT 1"
        ).fetchone()
    assert record["provider"] == "quota_guard"
    assert "Cuota diaria" in (record["error"] or "")
