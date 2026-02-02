from __future__ import annotations

import math


from src.experiments.dual_llm_tuning import evaluate_threshold_policy


def test_dual_llm_threshold_high_ratio_promotes_percentile() -> None:
    res = evaluate_threshold_policy(1000.0, premium_ratio=0.6, premium_values=[500, 1000, 2000, 3000])
    assert math.isclose(res["suggested_threshold"], 2100.0, rel_tol=1e-3)
    assert "subir" in res["reason"]


def test_dual_llm_threshold_low_ratio_reduces() -> None:
    res = evaluate_threshold_policy(1500.0, premium_ratio=0.02, premium_values=[500, 1000, 2000, 3000])
    # percentil 30 ≈ 950
    assert math.isclose(res["suggested_threshold"], 950.0, rel_tol=1e-3)
    assert "bajar" in res["reason"]


def test_dual_llm_threshold_within_range_keeps_current() -> None:
    res = evaluate_threshold_policy(1200.0, premium_ratio=0.1, premium_values=[700, 1200, 1800])
    assert math.isclose(res["suggested_threshold"], 1200.0, rel_tol=1e-9)
    assert "dentro del rango" in res["reason"]


def test_dual_llm_threshold_without_premium_docs_returns_current() -> None:
    res = evaluate_threshold_policy(900.0, premium_ratio=0.0, premium_values=[])
    assert res["suggested_threshold"] == 900.0
    assert res["reason"] == "sin_datos_premium"
