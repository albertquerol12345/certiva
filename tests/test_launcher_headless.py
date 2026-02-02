from __future__ import annotations

from pathlib import Path

from src import launcher


def test_headless_process_folder(monkeypatch, tmp_path, capsys):
    def fake_process(path, tenant, force_dummy, quiet=False, skip_probe=False):  # noqa: ARG001
        return tmp_path, ["doc1"]

    monkeypatch.setattr(launcher, "process_folder_batch", fake_process)
    launcher.headless_main(["process-folder", "--path", str(tmp_path), "--tenant", "demo"])
    captured = capsys.readouterr()
    assert str(tmp_path) in captured.out


def test_headless_dump_summary(tmp_path, capsys):
    lote = tmp_path / "OUT" / "demo" / "lote1"
    lote.mkdir(parents=True)
    resumen = lote / "RESUMEN.txt"
    resumen.write_text("Resumen demo", encoding="utf-8")
    launcher.headless_main(["dump-summary", "--lote", str(lote)])
    captured = capsys.readouterr()
    assert "Resumen demo" in captured.out


def test_headless_experiment_dual_llm(monkeypatch, tmp_path, capsys):
    called = {}

    def fake_run(batch_dir, tenant):
        called["batch"] = batch_dir
        called["tenant"] = tenant
        return {"batch_dir": tmp_path / "OUT" / tenant / "lote"}

    monkeypatch.setattr(launcher, "_ensure_experiment_samples", lambda: None)
    monkeypatch.setattr(launcher, "run_dual_llm_experiment", fake_run)
    launcher.headless_main(
        ["experiment-dual-llm", "--path", str(tmp_path / "IN"), "--tenant", "acme"]
    )
    captured = capsys.readouterr()
    assert "OUT/acme/lote" in captured.out
    assert called["tenant"] == "acme"
