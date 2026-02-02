from __future__ import annotations

import json
from pathlib import Path

from src import config, llm_suggest, utils
from src.llm_providers import DummyLLMProvider
from src.batch_writer import build_batch_outputs
from src.exporter import A3_CSV_COLUMNS


def _basic_invoice(doc_id: str) -> dict:
    return {
        "doc_id": doc_id,
        "tenant": "demo",
        "supplier": {"name": "Proveedor Demo", "nif": "B12345678"},
        "invoice": {"number": "INV-1", "date": "2025-01-01", "due": "2025-01-30"},
        "totals": {"base": 100.0, "vat": 21.0, "gross": 121.0},
        "lines": [{"desc": "Servicio", "amount": 100.0, "vat_rate": 21.0}],
        "metadata": {"category": "servicios_prof", "doc_type": "invoice"},
    }


def test_llm_debug_traces_are_created(temp_certiva_env, monkeypatch):
    monkeypatch.setattr(config.settings, "debug_llm", True)
    config.set_llm_provider_override(DummyLLMProvider())
    doc_id = "doc-debug"
    invoice = _basic_invoice(doc_id)
    mapping = llm_suggest.suggest_mapping(invoice)
    assert mapping["account"]
    debug_root = Path(config.BASE_DIR / "OUT" / "debug" / doc_id)
    assert (debug_root / "prompt.json").exists()
    prompt_payload = json.loads((debug_root / "prompt.json").read_text())
    assert "B12345678" not in json.dumps(prompt_payload)
    # prepare CSV needed for batch
    utils.insert_or_get_doc(doc_id, doc_id, f"{doc_id}.pdf", "demo")
    utils.update_doc_status(doc_id, "POSTED")
    csv_path = config.BASE_DIR / "OUT" / "csv" / f"{doc_id}.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.write_text(",".join(A3_CSV_COLUMNS) + "\n2025-01-01,COMPRAS,INV1,600000,100.00,0.00,Concepto,B12345678\n", encoding="utf-8")
    batch_dir = build_batch_outputs([doc_id], "demo", "lote_debug")
    batch_prompt = batch_dir / doc_id / "debug" / "prompt.json"
    assert batch_prompt.exists()
