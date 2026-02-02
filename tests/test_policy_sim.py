from tests.test_reports import _seed_reporting_docs


def test_policy_simulation_outputs(temp_certiva_env):
    policy_sim = temp_certiva_env["policy_sim"]
    _seed_reporting_docs(temp_certiva_env)
    result = policy_sim.simulate_policy(policy_sim.POLICIES["balanced"])
    assert result["total_docs"] >= 2
    assert "auto_post" in result
    assert "sales_invoice" in result["by_doc_type"]
