from __future__ import annotations

from pathlib import Path

from tools import benchmark_pipeline as benchmark
from src import utils


def test_benchmark_creates_report(temp_certiva_env, monkeypatch, tmp_path):
    base = temp_certiva_env["base"]
    data_dir = base / "IN" / "bench_inputs"
    out_path = base / "OUT" / "BENCHMARK_TEST.txt"
    monkeypatch.setattr(benchmark, "DATASET_DIR", data_dir)
    monkeypatch.setattr(benchmark, "BENCHMARK_PATH", out_path)

    def fake_generate(count, out_tests, out_in, seed, purge):  # noqa: ARG001
        out_in.mkdir(parents=True, exist_ok=True)
        for idx in range(count):
            (out_in / f"sample_{idx}.pdf").write_text("pdf", encoding="utf-8")

    monkeypatch.setattr(benchmark.tests_generate, "generate_samples", fake_generate)
    processed: list[str] = []

    def fake_process(path: Path, tenant: str, force: bool = False):  # noqa: ARG001
        doc_id = f"{path.stem}_{len(processed)}"
        processed.append(doc_id)
        utils.insert_or_get_doc(doc_id, doc_id, path.name, tenant)
        with utils.get_connection() as conn:
            conn.execute(
                "UPDATE docs SET total_time_ms=?, ocr_time_ms=?, rules_time_ms=?, llm_time_ms=? WHERE doc_id = ?",
                (120.0, 80.0, 20.0, 10.0, doc_id),
            )
        return doc_id

    monkeypatch.setattr(benchmark.pipeline, "process_file", fake_process)
    results = benchmark.run_benchmark(count=4, tenant="demo", seed=1, concurrency_levels=[1])
    assert results
    assert out_path.exists()
    content = out_path.read_text(encoding="utf-8")
    assert "Concurrency: 1" in content
    assert processed  # ensure pipeline called
