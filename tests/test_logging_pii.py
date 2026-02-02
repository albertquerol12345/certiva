import io
import logging

from src import utils


def test_logging_pii_filter_scrubs(monkeypatch):
    monkeypatch.setattr(utils.settings, "llm_enable_pii", False, raising=False)
    monkeypatch.setattr(utils.settings, "llm_pii_scrub_strict", True, raising=False)
    logger = logging.getLogger("pii-test")
    logger.handlers.clear()
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.addFilter(utils.PIIFilter())
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    logger.info("cliente: Juan Perez con NIF 12345678Z y IBAN ES7620770024003102575766")
    handler.flush()
    output = stream.getvalue()
    assert "[DOC_ID]" in output
    assert "[IBAN]" in output
    assert "Juan Perez" not in output
