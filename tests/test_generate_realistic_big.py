from __future__ import annotations

from pathlib import Path

from tests import generate_realistic_samples


def test_generate_realistic_samples_creates_expected_files(tmp_path):
    out_tests = tmp_path / "tests"
    out_in = tmp_path / "in"
    generate_realistic_samples.generate_samples(count=10, out_tests=out_tests, out_in=out_in, seed=5, purge=True)
    tests_files = sorted(out_tests.glob("*.pdf"))
    in_files = sorted(out_in.glob("*.pdf"))
    assert len(tests_files) == 10
    assert len(in_files) == 10
    assert tests_files[0].suffix.lower() == ".pdf"


def test_generate_realistic_samples_supports_no_pipeline_dir(tmp_path):
    out_tests = tmp_path / "tests_only"
    generate_realistic_samples.generate_samples(count=5, out_tests=out_tests, out_in=None, seed=7, purge=True)
    assert len(list(out_tests.glob("*.pdf"))) == 5
