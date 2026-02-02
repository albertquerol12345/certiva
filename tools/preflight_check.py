from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List, Tuple

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from src import config, utils  # noqa: E402


def check_filesystem() -> Tuple[bool, str]:
    target = utils.BASE_DIR / "OUT" / ".preflight_check"
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("ok", encoding="utf-8")
        target.unlink()
        return True, "OUT/ accesible"
    except Exception as exc:  # pragma: no cover - IO errors
        return False, f"No se pudo escribir en OUT/: {exc}"


def check_db() -> Tuple[bool, str]:
    try:
        with utils.get_connection() as conn:
            conn.execute("SELECT 1")
        return True, "SQLite accesible"
    except Exception as exc:  # pragma: no cover - DB errors
        return False, f"DB error: {exc}"


def check_azure(real_checks: bool) -> Tuple[bool, str]:
    if config.settings.ocr_provider_type != "azure":
        return True, "OCR dummy"
    if not (config.settings.azure_formrec_endpoint and config.settings.azure_formrec_key):
        return False, "Azure Form Recognizer sin credenciales"
    if not real_checks:
        return True, "Azure configurado (no probado)"
    try:  # pragma: no cover - depende de SDK
        from src.ocr_providers import AzureOCRProvider

        AzureOCRProvider(
            config.settings.azure_formrec_endpoint,
            config.settings.azure_formrec_key,
            config.settings.azure_formrec_model_id or "prebuilt-invoice",
        )
        return True, "Azure inicializado"
    except Exception as exc:
        return False, f"Azure init falló: {exc}"


def check_openai(real_checks: bool) -> Tuple[bool, str]:
    if config.settings.llm_provider_type != "openai":
        return True, "LLM dummy"
    if not config.settings.openai_api_key:
        return False, "OPENAI_API_KEY no configurada"
    if not real_checks:
        return True, "OpenAI configurado (no probado)"
    try:  # pragma: no cover - requiere SDK
        from src.llm_providers import OpenAILLMProvider

        OpenAILLMProvider(
            config.settings.openai_api_key,
            config.settings.openai_model_mini or config.settings.openai_model,
            config.settings.openai_api_base,
        )
        return True, "OpenAI inicializado"
    except Exception as exc:
        return False, f"OpenAI init falló: {exc}"


def run_preflight(real_checks: bool) -> int:
    checks: List[Tuple[str, Tuple[bool, str]]] = []
    checks.append(("filesystem", check_filesystem()))
    checks.append(("database", check_db()))
    checks.append(("azure", check_azure(real_checks)))
    checks.append(("openai", check_openai(real_checks)))
    status = 0
    print("=== CERTIVA Preflight ===")
    print(f"APP_ENV: {config.settings.app_env}")
    for name, (ok, message) in checks:
        label = "OK" if ok else "FAIL"
        print(f"[{label}] {name}: {message}")
        if not ok:
            status = 1
    if not real_checks:
        print("(Modo dummy/validación rápida; usa --real-checks para probar SDKs)")
    return status


def main() -> None:
    parser = argparse.ArgumentParser(description="Preflight check para CERTIVA")
    parser.add_argument(
        "--real-checks",
        action="store_true",
        help="Intentar inicializar Azure/OpenAI si hay credenciales",
    )
    args = parser.parse_args()
    sys.exit(run_preflight(args.real_checks))


if __name__ == "__main__":  # pragma: no cover - CLI
    main()
