from src import llm_router, utils


def test_llm_calls_are_logged(temp_certiva_env, monkeypatch):
    utils = temp_certiva_env["utils"]
    llm_router.settings.llm_rag_provider = "dummy"  # ensure no network
    with utils.get_connection() as conn:
        conn.execute("DELETE FROM llm_calls")
    response = llm_router.call_llm(
        llm_router.LLMTask.RAG_NORMATIVO,
        "Eres un asistente.",
        "Explica qué es el IVA.",
        context="IVA soportado es deducible.",
    )
    assert response
    with utils.get_connection() as conn:
        row = conn.execute("SELECT task, provider FROM llm_calls ORDER BY id DESC LIMIT 1").fetchone()
        assert row is not None
        assert row["task"] == llm_router.LLMTask.RAG_NORMATIVO.value
