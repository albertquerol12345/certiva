from __future__ import annotations

from pathlib import Path

from src import utils
from src.batch_writer import build_batch_outputs
from src.exporter import A3_CSV_COLUMNS


def _seed_doc(doc_id: str, tenant: str, status: str, llm_model: str, issues=None) -> None:
    utils.insert_or_get_doc(doc_id, doc_id, f"{doc_id}.pdf", tenant)
    utils.update_doc_status(
        doc_id,
        status,
        llm_provider="openai",
        llm_model_used=llm_model,
        llm_time_ms=150,
        llm_tokens_in=10,
        llm_tokens_out=5,
        llm_cost_eur=0.01,
        ocr_provider="azure",
        ocr_time_ms=200,
        rules_time_ms=50,
        issues=issues,
    )
    if status == "POSTED":
        csv_dir = utils.BASE_DIR / "OUT" / "csv"
        csv_dir.mkdir(parents=True, exist_ok=True)
        csv_path = csv_dir / f"{doc_id}.csv"
        if not csv_path.exists():
            with csv_path.open("w", encoding="utf-8", newline="") as fh:
                fh.write(",".join(A3_CSV_COLUMNS) + "\n")
                fh.write("2025-01-01,COMPRAS,INV-1,629000,100.00,0.00,Concepto,B12345678\n")


def test_resumen_includes_llm_breakdown(temp_certiva_env):
    tenant = "demo"
    _seed_doc("doc-mini", tenant, "POSTED", "mini")
    _seed_doc("doc-premium", tenant, "POSTED", "premium")
    _seed_doc("doc-inc", tenant, "REVIEW_PENDING", "mini", issues='["LLM_ERROR"]')
    batch_dir = build_batch_outputs(["doc-mini", "doc-premium", "doc-inc"], tenant, "batch_llm_stats")
    resumen = (batch_dir / "RESUMEN.txt").read_text(encoding="utf-8")
    assert "mini_docs (OK): 1" in resumen
    assert "premium_docs (OK): 1" in resumen
    assert "Documentos tratados: 3" in resumen
    assert "Documentos con incidencias: 1" in resumen
    assert "current_threshold_gross" in resumen
    assert "LLM costes/tokens" in resumen
