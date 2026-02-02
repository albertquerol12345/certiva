from __future__ import annotations

from tools import preflight_check


def test_preflight_dummy_mode(capsys):
    exit_code = preflight_check.run_preflight(real_checks=False)
    captured = capsys.readouterr()
    assert exit_code in {0, 1}
    assert "filesystem" in captured.out
    assert "SQLite" in captured.out
