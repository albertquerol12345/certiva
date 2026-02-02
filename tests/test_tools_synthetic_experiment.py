from __future__ import annotations

import json
from pathlib import Path

import pytest

from tools import run_small_synthetic_experiment as experiment


def test_ensure_clean_lot_generates_files(tmp_path, monkeypatch):
    clean_dir = tmp_path / "clean"
    tests_dir = tmp_path / "tests"
    monkeypatch.setattr(experiment, "CLEAN_DIR", clean_dir)
    monkeypatch.setattr(experiment, "TESTS_DIR", tests_dir)

    def fake_generate_samples(count, out_tests, out_in, seed, purge):  # noqa: ARG001
        out_tests.mkdir(parents=True, exist_ok=True)
        out_in.mkdir(parents=True, exist_ok=True)
        for idx in range(count):
            (out_in / f"doc_{idx:03d}.pdf").write_text("pdf", encoding="utf-8")

    monkeypatch.setattr(experiment.tests_generate, "generate_samples", fake_generate_samples)
    path = experiment.ensure_clean_lot(count=2, seed=1, purge=True)
    assert path == clean_dir
    assert len(list(clean_dir.glob("*.pdf"))) == 2


def test_ensure_dirty_lot_creates_files_from_source(tmp_path, monkeypatch):
    clean_dir = tmp_path / "clean"
    dirty_dir = tmp_path / "dirty"
    clean_dir.mkdir()
    (clean_dir / "sample.pdf").write_text("pdf", encoding="utf-8")
    monkeypatch.setattr(experiment, "CLEAN_DIR", clean_dir)
    monkeypatch.setattr(experiment, "DIRTY_DIR", dirty_dir)

    def fake_augment(source, dest, seed, purge, limit):  # noqa: ARG001
        dest.mkdir(parents=True, exist_ok=True)
        for idx, file in enumerate(sorted(source.glob("*.pdf"))[:limit]):
            (dest / f"{file.stem}_dirty_{idx}.pdf").write_text("dirty", encoding="utf-8")
        return dest.glob("*.pdf")

    monkeypatch.setattr(experiment.tests_augment, "augment_folder", fake_augment)
    path = experiment.ensure_dirty_lot(count=1, seed=5, source_dir=clean_dir, purge=True)
    assert path == dirty_dir
    files = list(dirty_dir.glob("*.pdf"))
    assert len(files) == 1


def test_collect_doc_metrics_counts_models(temp_certiva_env, monkeypatch):
    utils = temp_certiva_env["utils"]
    monkeypatch.setattr(experiment, "utils", utils, raising=False)
    base = temp_certiva_env["base"]
    json_dir = base / "OUT" / "json"
    (json_dir).mkdir(parents=True, exist_ok=True)
    doc_mini = "mini-doc"
    doc_premium = "premium-doc"
    with utils.get_connection() as conn:
        conn.execute(
            """
            INSERT INTO docs(doc_id, filename, tenant, status, total_time_ms, ocr_time_ms, rules_time_ms, llm_time_ms, global_conf, llm_model_used)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                doc_mini,
                "mini.pdf",
                "demo",
                "POSTED",
                120.0,
                80.0,
                10.0,
                5.0,
                0.92,
                "mini",
            ),
        )
        conn.execute(
            """
            INSERT INTO docs(doc_id, filename, tenant, status, total_time_ms, ocr_time_ms, rules_time_ms, llm_time_ms, global_conf, llm_model_used)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (
                doc_premium,
                "premium.pdf",
                "demo",
                "REVIEW_PENDING",
                150.0,
                90.0,
                15.0,
                20.0,
                0.55,
                "premium",
            ),
        )
    (json_dir / f"{doc_premium}.json").write_text(json.dumps({"totals": {"gross": 123.45}}), encoding="utf-8")
    metrics = experiment.collect_doc_metrics([doc_mini, doc_premium])
    assert metrics["doc_count"] == 2
    assert metrics["premium_docs"] == 1
    assert metrics["mini_docs"] == 1
    assert metrics["posted_docs"] == 1
    assert metrics["incident_docs"] == 1
    assert pytest.approx(metrics["premium_ratio"], rel=1e-6) == 0.5
    assert metrics["premium_gross_values"] == [123.45]


def test_build_report_writes_output(tmp_path):
    clean_metrics = {
        "doc_ids": ["a", "b"],
        "doc_count": 2,
        "mini_docs": 2,
        "premium_docs": 0,
        "premium_gross_values": [],
        "total_time_ms": [100.0, 120.0],
        "ocr_time_ms": [60.0, 65.0],
        "rules_time_ms": [10.0],
        "llm_time_ms": [5.0],
        "confidence": [0.95, 0.9],
        "posted_docs": 2,
        "incident_docs": 0,
        "premium_ratio": 0.0,
        "current_threshold": 1000.0,
    }
    dirty_metrics = {
        "doc_ids": ["c", "d"],
        "doc_count": 2,
        "mini_docs": 0,
        "premium_docs": 2,
        "premium_gross_values": [800.0, 1200.0],
        "total_time_ms": [200.0, 210.0],
        "ocr_time_ms": [100.0],
        "rules_time_ms": [20.0],
        "llm_time_ms": [50.0],
        "confidence": [0.6, 0.58],
        "posted_docs": 0,
        "incident_docs": 2,
        "premium_ratio": 1.0,
        "current_threshold": 1000.0,
    }
    clean_outcome = experiment.BatchOutcome(
        label="limpio",
        batch_dir=tmp_path / "batch_clean",
        doc_ids=["a", "b"],
        resumen={"Issues frecuentes": "NONE=0"},
        metrics=clean_metrics,
        suggestion={
            "current_threshold": 1000.0,
            "suggested_threshold": 950.0,
            "reason": "test_low",
            "premium_ratio": 0.0,
        },
    )
    dirty_outcome = experiment.BatchOutcome(
        label="dirty",
        batch_dir=tmp_path / "batch_dirty",
        doc_ids=["c", "d"],
        resumen={"Issues frecuentes": "LLM_ERROR=3"},
        metrics=dirty_metrics,
        suggestion={
            "current_threshold": 1000.0,
            "suggested_threshold": 1500.0,
            "reason": "test_high",
            "premium_ratio": 1.0,
        },
    )
    report_path = tmp_path / "report.txt"
    report = experiment.build_report([clean_outcome, dirty_outcome], report_path)
    assert "Threshold sugerido" in report
    assert "Comparativa" in report
    assert report_path.read_text(encoding="utf-8") == report
