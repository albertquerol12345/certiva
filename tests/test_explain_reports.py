from tests.test_reports import _seed_reporting_docs

from src import explain_reports


def test_explain_pnl_uses_llm(temp_certiva_env, monkeypatch):
    reports_module = temp_certiva_env["reports"]
    _seed_reporting_docs(temp_certiva_env)

    captured = {}

    def fake_call(task, system_prompt, user_prompt, context=None, **kwargs):
        captured["task"] = task
        captured["context"] = context
        return "Análisis simulado"

    monkeypatch.setattr(explain_reports.llm_router, "call_llm", fake_call)
    report = reports_module.build_pnl("demo", "2025-01-01", "2025-03-31")
    result = explain_reports.explain_pnl(report)
    assert result == "Análisis simulado"
    assert "Ingresos totales" in captured["context"]
