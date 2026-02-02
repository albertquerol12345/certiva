"""Lanzador CLI interactivo para operar CERTIVA desde la terminal."""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List, Tuple

from . import hitl_cli, metrics, pipeline, reports, utils
from .config import (
    BASE_DIR,
    settings,
    get_ocr_provider,
    get_llm_provider,
    set_llm_provider_override,
    set_ocr_provider_override,
)
from .ocr_providers import DummyOCRProvider
from .llm_providers import DummyLLMProvider
from .batch_writer import build_batch_outputs
from .experiments.dual_llm_tuning import run_dual_llm_experiment


EXPERIMENT_DIR = BASE_DIR / "IN" / "lote_experimentos_azure_openai"


def _print_header() -> None:
    print("\n" + "=" * 52)
    print(" CERTIVA · Consola operativa")
    print("=" * 52)


def _wait_enter() -> None:
    input("\nPulsa ENTER para continuar...")


def _clear_overrides() -> None:
    set_ocr_provider_override(None)
    set_llm_provider_override(None)


def _set_providers(force_dummy: bool) -> Tuple[str, str]:
    if force_dummy:
        set_ocr_provider_override(DummyOCRProvider())
        set_llm_provider_override(DummyLLMProvider())
        return DummyOCRProvider().provider_name, DummyLLMProvider().provider_name
    _clear_overrides()
    current_ocr = get_ocr_provider()
    current_llm = get_llm_provider()
    return current_ocr.provider_name, current_llm.provider_name


def _run_preflight_probe(folder: Path, total_files: int, quiet: bool) -> None:
    if settings.ocr_provider_type != "azure":
        return
    sample = max(1, min(5, total_files))
    try:
        from tools import azure_probe
    except ImportError:  # pragma: no cover - defensive
        logger = logging.getLogger(__name__)
        logger.warning("No se pudo importar tools.azure_probe para ejecutar el probe de Azure.")
        return
    if not quiet:
        print(f"[+] Ejecutando azure_probe ({sample} PDFs) para chequear el estado de Azure OCR…")
    report, ok = azure_probe.probe(
        folder,
        count=sample,
        seed=123,
        max_rps=settings.azure_ocr_max_rps,
        timeout=settings.azure_ocr_read_timeout_sec,
    )
    if not ok:
        logger = logging.getLogger(__name__)
        logger.error("azure_probe detectó errores antes de procesar el lote:\n%s", report)
        raise RuntimeError(
            "PROVIDER_DEGRADED: azure_probe detectó 429/timeout. "
            "Ajusta AZURE_MAX_RPS o reintenta más tarde."
        )
    if not quiet:
        print("[+] azure_probe OK. Continuando con el procesamiento…")


def process_folder_batch(
    path: Path,
    tenant: str,
    force_dummy: bool,
    quiet: bool = False,
    skip_probe: bool = False,
) -> Tuple[Path, List[str]]:
    if not path.exists():
        raise FileNotFoundError(f"No existe la carpeta {path}")
    files = sorted([p for p in path.rglob("*.pdf")])
    if not files:
        raise RuntimeError("No se encontraron PDFs.")
    if not skip_probe and not force_dummy:
        _run_preflight_probe(path, len(files), quiet)
    utils.configure_logging()
    try:
        ocr_name, llm_name = _set_providers(force_dummy)
        if not quiet:
            print(f"Usando providers configurados → OCR: {ocr_name} · LLM: {llm_name}")
    except Exception as exc:
        if not quiet:
            print(f"Providers reales no configurados ({exc}). Recurriendo a modo dummy.\n")
        set_ocr_provider_override(DummyOCRProvider())
        set_llm_provider_override(DummyLLMProvider())
    processed = 0
    doc_ids: List[str] = []
    for file_path in files:
        try:
            doc_id = pipeline.process_file(file_path, tenant=tenant)
            if doc_id:
                processed += 1
                doc_ids.append(doc_id)
                if not quiet:
                    print(f"  ✓ {file_path.name} → {doc_id[:8]}…")
        except Exception as exc:  # pragma: no cover
            if not quiet:
                print(f"  × Error con {file_path.name}: {exc}")
    _clear_overrides()
    batch_name = f"{path.name}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    batch_dir = build_batch_outputs(doc_ids, tenant, batch_name)
    if not quiet:
        print(f"\nListo. Documentos procesados: {processed}")
        print(f"Lote disponible en {batch_dir}")
    return batch_dir, doc_ids


def _process_folder(path: Path, tenant: str, force_dummy: bool) -> None:
    try:
        process_folder_batch(path, tenant, force_dummy, quiet=False)
    except Exception as exc:
        print(str(exc))
        _wait_enter()
        return
    _wait_enter()


def option_process_demo() -> None:
    """Procesa los PDFs de demostración incluidos en tests/golden (modo dummy)."""
    demo_dir = BASE_DIR / "tests" / "golden"
    tenant = settings.default_tenant
    _process_folder(demo_dir, tenant, force_dummy=True)


def option_process_real() -> None:
    """Procesa una carpeta real usando los providers configurados."""
    default_folder = BASE_DIR / "IN" / settings.default_tenant
    folder_str = input(f"Carpeta a procesar [{default_folder}]: ").strip()
    folder = Path(folder_str) if folder_str else default_folder
    tenant_input = input(f"Tenant [{settings.default_tenant}]: ").strip() or settings.default_tenant
    _process_folder(folder, tenant_input, force_dummy=False)


def _ensure_experiment_samples() -> None:
    if EXPERIMENT_DIR.exists() and any(EXPERIMENT_DIR.glob("*.pdf")):
        return
    print("Generando PDFs sintéticos para el experimento dual LLM...")
    try:
        from tests import generate_realistic_samples
    except Exception as exc:  # pragma: no cover - import defensive
        print(f"No se pudieron generar los samples automáticamente ({exc}).")
        return
    try:
        generate_realistic_samples.main()
    except Exception as exc:  # pragma: no cover - generator failure
        print(f"Fallo generando samples: {exc}")
    else:
        print(f"Samples disponibles en {EXPERIMENT_DIR}")


def option_dual_llm_experiment() -> None:
    """Ejecuta el flujo dual LLM sobre el lote sintético y propone ajustes de threshold."""
    utils.configure_logging()
    _ensure_experiment_samples()
    try:
        result = run_dual_llm_experiment(EXPERIMENT_DIR, settings.default_tenant)
    except Exception as exc:
        print(f"No se pudo completar el experimento dual LLM: {exc}")
        _wait_enter()
        return
    batch_dir = result.get("batch_dir")
    if batch_dir:
        print(f"\nExperimento Dual LLM completado. Lote generado en {batch_dir}")
    suggestion_path = result.get("suggestion_path")
    if suggestion_path and suggestion_path.exists():
        print("\n--- Contenido de LLM_TUNING_SUGGESTIONS.txt ---")
        print(suggestion_path.read_text(encoding="utf-8"))
    else:
        print("No se encontró LLM_TUNING_SUGGESTIONS.txt en el lote.")
    _wait_enter()


def _format_money(value: float) -> str:
    return f"{value:,.2f}".replace(",", " ").replace(".", ",")


def option_show_metrics() -> None:
    """Muestra métricas clave en terminal."""
    utils.configure_logging()
    stats = metrics.gather_stats(settings.default_tenant)
    print("\nResumen de métricas")
    print("-" * 40)
    print(f"Docs totales: {stats['docs_total']} | Posteados: {stats['posted']}")
    print(f"Auto-post: {stats['auto_post_pct']:.1f}%")
    print(f"P50 total: {stats['total_p50']:.2f} min | P90 total: {stats['total_p90']:.2f} min")
    print(f"OCR medio: {stats['ocr_mean']:.2f}s · Validación: {stats['validation_mean']:.2f}s")
    print(f"Duplicados: {stats['duplicates']} · Reglas aprendidas: {stats['rules_learned']}")
    bank = stats.get("bank") or {}
    print(
        f"Conciliación: completos {bank.get('docs_fully',0)}/{bank.get('docs_total',0)} · "
        f"parciales {bank.get('docs_partial',0)}"
    )
    pnl = stats.get("pnl_summary") or {}
    if pnl:
        print(
            f"P&L mes → Ingresos {_format_money(pnl.get('total_income',0))} € | "
            f"Gastos {_format_money(pnl.get('total_expense',0))} € | "
            f"Resultado {_format_money(pnl.get('result',0))} €"
        )
    llm = stats.get("llm_stats") or {}
    if llm:
        print(
            f"LLM llamadas: {llm.get('total_calls',0)} · errores: {llm.get('errors',0)} "
            f"· latencia media: {llm.get('avg_latency_ms',0):.1f} ms"
        )
        tasks = ", ".join(f"{task} ({count})" for task, count in llm.get("by_task", []))
        if tasks:
            print(f"Tareas frecuentes: {tasks}")
    providers = stats.get("provider_breakdown") or {}
    ocr_breakdown = ", ".join(f"{prov}: {cnt}" for prov, cnt in providers.get("ocr", []))
    llm_breakdown = ", ".join(f"{prov}: {cnt}" for prov, cnt in providers.get("llm", []))
    if ocr_breakdown:
        print(f"OCR providers → {ocr_breakdown}")
    if llm_breakdown:
        print(f"LLM providers → {llm_breakdown}")
    print(
        f"Tiempos medios (ms) → OCR {stats.get('ocr_ms_mean',0):.1f} "
        f"| Rules {stats.get('rules_ms_mean',0):.1f} | LLM {stats.get('llm_ms_mean',0):.1f}"
    )
    _wait_enter()


def option_list_queue() -> None:
    """Listado rápido de la cola HITL."""
    utils.configure_logging()
    hitl_cli.list_queue()
    _wait_enter()


def option_review_interactive() -> None:
    """Abre la revisión interactiva en la propia terminal."""
    utils.configure_logging()
    hitl_cli.interactive()
    _wait_enter()


def option_open_reports() -> None:
    """Imprime un resumen rápido de reportes (P&L / IVA / Cashflow)."""
    utils.configure_logging()
    today = utils.today_iso()
    first_day = today[:-2] + "01"
    pnl = reports.build_pnl(settings.default_tenant, first_day, today)
    iva = reports.build_vat_report(settings.default_tenant, first_day, today)
    cash = reports.build_cashflow_forecast(settings.default_tenant, today, 3)
    print("\nReportes")
    print("-" * 40)
    print(f"P&L → Resultado {_format_money(pnl.get('result',0))} €")
    soportado = iva.get("soportado", {})
    total_soportado = sum((data or {}).get("vat", 0) for data in soportado.values())
    repercutido = iva.get("repercutido", {})
    total_repercutido = sum((data or {}).get("vat", 0) for data in repercutido.values())
    print(f"IVA soportado: {_format_money(total_soportado)} € · repercutido: {_format_money(total_repercutido)} €")
    if cash and cash.get("buckets"):
        buckets = "; ".join(
            f"{b['label']}: neto {_format_money(b['net'])} €" for b in cash["buckets"]
        )
        print(f"Cashflow 3 meses → {buckets}")
    _wait_enter()


MENU: Dict[str, tuple[str, Callable[[], None]]] = {
    "1": ("Procesar lote demo (dummy)", option_process_demo),
    "2": ("Procesar carpeta real (providers config)", option_process_real),
    "3": ("Ver métricas principales", option_show_metrics),
    "4": ("Listar cola HITL", option_list_queue),
    "5": ("Revisión interactiva HITL", option_review_interactive),
    "6": ("Resumen rápido de reportes", option_open_reports),
    "7": ("Experimento Dual LLM (lote sintético)", option_dual_llm_experiment),
    "q": ("Salir", lambda: None),
}


def headless_main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="CERTIVA launcher (headless)")
    sub = parser.add_subparsers(dest="command", required=True)

    pf_cmd = sub.add_parser("process-folder", help="Procesa una carpeta real")
    pf_cmd.add_argument("--path", required=True)
    pf_cmd.add_argument("--tenant", default=settings.default_tenant)
    pf_cmd.add_argument("--force-dummy", action="store_true")

    exp_cmd = sub.add_parser("experiment-dual-llm", help="Lanza el experimento dual LLM")
    exp_cmd.add_argument("--path", default=str(EXPERIMENT_DIR), help="Carpeta de entrada")
    exp_cmd.add_argument("--tenant", default=settings.default_tenant, help="Tenant del experimento")

    dump_cmd = sub.add_parser("dump-summary", help="Imprime un RESUMEN.txt")
    dump_cmd.add_argument("--lote", required=True)

    args = parser.parse_args(argv)
    if args.command == "process-folder":
        batch_dir, _ = process_folder_batch(Path(args.path), args.tenant, args.force_dummy, quiet=False)
        print(batch_dir)
    elif args.command == "experiment-dual-llm":
        utils.configure_logging()
        _ensure_experiment_samples()
        result = run_dual_llm_experiment(Path(args.path), args.tenant)
        print(result.get("batch_dir"))
    elif args.command == "dump-summary":
        resumen_path = Path(args.lote) / "RESUMEN.txt"
        if not resumen_path.exists():
            raise FileNotFoundError(f"No existe {resumen_path}")
        print(resumen_path.read_text(encoding="utf-8"))

def main() -> None:
    if "--headless" in sys.argv:
        index = sys.argv.index("--headless")
        headless_args = sys.argv[index + 1 :]
        headless_main(headless_args)
        return
    utils.configure_logging()
    while True:
        _print_header()
        for key, (label, _) in MENU.items():
            print(f"[{key}] {label}")
        choice = input("\nSelecciona una opción: ").strip().lower()
        if choice == "q":
            print("Hasta pronto.")
            return
        action = MENU.get(choice)
        if not action:
            print("Opción inválida.")
            _wait_enter()
            continue
        _, handler = action
        handler()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nCancelado por el usuario.")
