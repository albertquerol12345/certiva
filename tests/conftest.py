import importlib
import os
import shutil

import pytest

os.environ.setdefault("APP_ENV", "test")
os.environ.setdefault("LLM_PROVIDER_TYPE", "dummy")
os.environ.setdefault("LLM_STRATEGY", "mini_only")

from src import utils, provider_health
import src.bank_matcher as bank_matcher_module
import src.policy_sim as policy_sim_module
import src.reports as reports_module
import src.rules_engine as rules_engine_module


@pytest.fixture
def temp_certiva_env(tmp_path, monkeypatch):
    """Create an isolated filesystem/db for deterministic tests."""
    base = tmp_path / "certiva"
    (base / "OUT" / "json").mkdir(parents=True)
    (base / "OUT" / "reports").mkdir(parents=True)
    (base / "db").mkdir(parents=True)
    (base / "IN").mkdir(parents=True)
    original_data = utils.BASE_DIR / "data"
    if original_data.exists():
        shutil.copytree(original_data, base / "data")
    monkeypatch.setattr(utils, "BASE_DIR", base, raising=False)
    monkeypatch.setattr(utils, "DB_PATH", base / "db" / "docs.sqlite", raising=False)
    utils.init_db()
    provider_health.reset_all()
    reports = importlib.reload(reports_module)
    bank_matcher = importlib.reload(bank_matcher_module)
    rules_engine = importlib.reload(rules_engine_module)
    policy_sim = importlib.reload(policy_sim_module)
    return {
        "base": base,
        "reports": reports,
        "bank_matcher": bank_matcher,
        "rules_engine": rules_engine,
        "policy_sim": policy_sim,
        "utils": utils,
    }
