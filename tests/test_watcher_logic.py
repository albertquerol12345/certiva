from __future__ import annotations

from pathlib import Path

from src import watcher


def _touch_pdf(path: Path, content: str = "dummy") -> None:
    path.write_text(content, encoding="utf-8")


def test_batch_watcher_processes_when_batch_ready(tmp_path, monkeypatch):
    processed_batches = []

    def fake_process(path, tenant, force=False):
        return path.stem

    def fake_build(doc_ids, tenant, batch_name, *args, **kwargs):  # noqa: ARG001
        processed_batches.append(doc_ids)
        return tmp_path / batch_name

    monkeypatch.setattr(watcher.pipeline, "process_file", fake_process)
    monkeypatch.setattr(watcher, "build_batch_outputs", fake_build)
    w = watcher.BatchWatcher(
        root=tmp_path,
        tenant="demo",
        pattern="*.pdf",
        recursive=False,
        archive_dir=None,
        batch_size=2,
        batch_timeout=10,
        stabilize_seconds=0.0,
        force=False,
        clock=lambda: 0.0,
    )
    _touch_pdf(tmp_path / "doc1.pdf")
    _touch_pdf(tmp_path / "doc2.pdf")
    ids = w.poll()
    assert set(ids) == {"doc1", "doc2"}
    assert [set(batch) for batch in processed_batches] == [{"doc1", "doc2"}]


def test_batch_watcher_uses_timeout(tmp_path, monkeypatch):
    processed = []

    def fake_process(path, tenant, force=False):
        return path.stem

    def fake_build(doc_ids, tenant, batch_name, *args, **kwargs):  # noqa: ARG001
        processed.append(doc_ids)
        return tmp_path / batch_name

    times = iter([0.0, 0.0, 6.0, 12.0, 12.0])

    def fake_clock():
        return next(times)

    monkeypatch.setattr(watcher.pipeline, "process_file", fake_process)
    monkeypatch.setattr(watcher, "build_batch_outputs", fake_build)
    w = watcher.BatchWatcher(
        root=tmp_path,
        tenant="demo",
        pattern="*.pdf",
        recursive=False,
        archive_dir=None,
        batch_size=3,
        batch_timeout=5,
        stabilize_seconds=0.0,
        force=False,
        clock=fake_clock,
    )
    _touch_pdf(tmp_path / "only.pdf")
    assert w.poll() == []  # primera llamada, sin timeout
    assert set(w.poll()) == {"only"}  # segunda llamada, supera timeout
    assert processed == [["only"]]


def test_batch_watcher_waits_until_files_stabilize(tmp_path, monkeypatch):
    processed = []

    def fake_process(path, tenant, force=False):
        processed.append(path.name)
        return path.stem

    def fake_build(doc_ids, tenant, batch_name, *args, **kwargs):  # noqa: ARG001
        return tmp_path / batch_name

    clock_values = iter([0.0, 1.0, 6.0, 8.0, 8.0])

    def fake_clock():
        try:
            return next(clock_values)
        except StopIteration:  # pragma: no cover - estabilidad extra
            return 8.0

    monkeypatch.setattr(watcher.pipeline, "process_file", fake_process)
    monkeypatch.setattr(watcher, "build_batch_outputs", fake_build)
    w = watcher.BatchWatcher(
        root=tmp_path,
        tenant="demo",
        pattern="*.pdf",
        recursive=False,
        archive_dir=None,
        batch_size=1,
        batch_timeout=30,
        stabilize_seconds=5.0,
        force=False,
        clock=fake_clock,
    )
    file_path = tmp_path / "unstable.pdf"
    _touch_pdf(file_path, "v1")
    assert w.poll() == []  # todavía no ha pasado el periodo de estabilización
    assert processed == []
    assert set(w.poll()) == {"unstable"}
    assert processed == ["unstable.pdf"]


def test_run_once_moves_files_to_archive(tmp_path, monkeypatch):
    inbox = tmp_path / "inbox"
    archive = tmp_path / "archive"
    inbox.mkdir()
    pdf = inbox / "invoice.pdf"
    _touch_pdf(pdf)
    processed = []

    def fake_process(path, tenant, force=False):
        processed.append(path.name)
        return path.stem

    def fake_build(doc_ids, tenant, batch_name, *args, **kwargs):  # noqa: ARG001
        return tmp_path / batch_name

    monkeypatch.setattr(watcher.pipeline, "process_file", fake_process)
    monkeypatch.setattr(watcher, "build_batch_outputs", fake_build)
    watcher.run_once(inbox, "demo", "*.pdf", False, archive, limit=None, force=False)
    assert processed == ["invoice.pdf"]
    assert not pdf.exists()
    assert (archive / "invoice.pdf").exists()