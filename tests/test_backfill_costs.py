from __future__ import annotations

from src import utils
import tools.backfill_llm_costs as backfill_llm_costs


def test_backfill_llm_costs_updates_cost(temp_certiva_env, monkeypatch):
    utils = temp_certiva_env["utils"]
    # tarifas mini para facilitar cálculo
    monkeypatch.setattr("src.config.settings.openai_mini_in_per_mtok", 1.0, raising=False)
    monkeypatch.setattr("src.config.settings.openai_mini_out_per_mtok", 1.0, raising=False)
    with utils.get_connection() as conn:
        conn.execute(
            """
            INSERT INTO llm_calls(task, model, prompt_tokens, completion_tokens, cost_eur)
            VALUES('test', 'gpt-mini', 1000, 2000, 0)
            """
        )
    updated = backfill_llm_costs.backfill()
    assert updated == 1
    with utils.get_connection() as conn:
        cost = conn.execute("SELECT cost_eur FROM llm_calls WHERE task='test'").fetchone()[0]
        # tokens totales 0.003 MTok → coste 0.003 €
        assert abs(cost - 0.003) < 1e-6
