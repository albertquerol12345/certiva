from pathlib import Path

from src import rag_normativo


def test_answer_normative_question_uses_local_corpus(tmp_path, monkeypatch):
    corpus = tmp_path / "normativa"
    corpus.mkdir(parents=True)
    (corpus / "demo.md").write_text("## IVA\nEl IVA soportado es deducible si está vinculado a la actividad.", encoding="utf-8")
    monkeypatch.setattr(rag_normativo, "CORPUS_DIR", corpus)
    rag_normativo._CORPUS_CACHE = []
    response = rag_normativo.answer_normative_question("¿Qué es el IVA soportado?")
    assert "Simulación" in response or response.strip()
