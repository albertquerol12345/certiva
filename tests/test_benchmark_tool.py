from __future__ import annotations

from pathlib import Path

import pytest

from tools import benchmark as benchmark_tool
from src import utils


def test_benchmark_script_generates_report(temp_certiva_env, monkeypatch, tmp_path):
    base = temp_certiva_env["base"]
    clean_dir = base / "IN" / "clean"
    dirty_dir = base / "IN" / "dirty"
    clean_dir.mkdir(parents=True)
    dirty_dir.mkdir(parents=True)

    def fake_ensure(kind, count, seed):  # noqa: ARG001
        return clean_dir if kind == "clean" else dirty_dir

    monkeypatch.setattr(benchmark_tool, "ensure_inputs", fake_ensure)

    def fake_process(path: Path, tenant: str, force_dummy: bool, quiet: bool, skip_probe: bool = False):  # noqa: ARG001
        doc_ids = []
        for idx in range(2):
            doc_id = f"{path.name}_{idx}"
            doc_ids.append(doc_id)
            utils.insert_or_get_doc(doc_id, doc_id, f"{doc_id}.pdf", tenant)
            utils.update_doc_status(
                doc_id,
                "POSTED" if idx == 0 else "REVIEW_PENDING",
                llm_model_used="mini" if idx == 0 else "premium",
                llm_provider="openai",
                llm_time_ms=100,
                llm_tokens_in=idx * 10,
                llm_tokens_out=idx * 5,
                llm_cost_eur=0.01 * idx,
                ocr_time_ms=50,
                rules_time_ms=20,
                total_time_ms=200 + idx,
            )
        batch_dir = tmp_path / f"batch_{path.name}"
        batch_dir.mkdir(parents=True, exist_ok=True)
        return batch_dir, doc_ids

    monkeypatch.setattr(benchmark_tool, "process_folder_batch", fake_process)
    report_path = benchmark_tool.REPORT_DIR / "BENCHMARK_TEST.txt"
    monkeypatch.setattr(benchmark_tool, "REPORT_DIR", report_path.parent)
    output = benchmark_tool.run_benchmark(["clean", "dirty"], tenant="demo", count=2, seed=1)
    assert "Input clean" in output
    assert "Input dirty" in output
    assert any(path.name.startswith("BENCHMARK_") for path in report_path.parent.glob("BENCHMARK_*.txt"))
