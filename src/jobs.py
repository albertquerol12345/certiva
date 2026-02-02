"""Simple job scheduler/orchestrator built on top of SQLite."""
from __future__ import annotations

import argparse
import json
import logging
import random
import socket
import sqlite3
import subprocess
import time
from dataclasses import replace
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

from . import bank_matcher, metrics, policy_sim, utils, alerts
from tools import backfill_llm_costs
from .config import settings
from .pipeline import process_file as pipeline_process
from .sources import ImapSource, LocalFolderSource, SftpSource

logger = logging.getLogger(__name__)
HOSTNAME = socket.gethostname()


class JobSkipped(Exception):
    """Raised when a job intentionally skips execution (e.g., not implemented)."""


def _process_source(source: LocalFolderSource) -> int:
    documents = source.list_new_documents()
    processed = 0
    for document in documents:
        doc_id = pipeline_process(document.path, tenant=source.tenant)
        source.mark_processed(document, doc_id)
        processed += 1
    return processed


def _json_config(job, overrides: Optional[Dict] = None) -> Dict:
    data = {}
    if job and job["config"]:
        try:
            data = json.loads(job["config"])
        except json.JSONDecodeError:
            data = {}
    if overrides:
        data.update(overrides)
    return data


def run_scan_folder(job, config: Dict) -> None:
    path = Path(config["path"])
    pattern = config.get("pattern", "*.pdf")
    recursive = config.get("recursive", True)
    tenant = config.get("tenant") or job["tenant"] or settings.default_tenant
    archive = config.get("archive")
    limit = config.get("limit")
    if not path.exists():
        logger.warning("Ruta %s no existe, job %s omitido", path, job["name"])
        return
    source = LocalFolderSource(
        name=job["name"],
        tenant=tenant,
        root=path,
        pattern=pattern,
        recursive=bool(recursive),
        archive_dir=Path(archive) if archive else None,
        max_files=limit,
    )
    processed = _process_source(source)
    logger.info("Job %s -> %d documentos procesados de %s", job["name"], processed, path)


def run_import_bank(job, config: Dict) -> None:
    tenant = config.get("tenant") or job["tenant"] or settings.default_tenant
    csv_path = Path(config["path"])
    profile_name = config.get("profile")
    direction = config.get("direction")
    account_id = config.get("account_id")
    positive_sign = config.get("positive_sign", "credit")
    auto_match = config.get("auto_match", True)
    tolerance = float(config.get("tolerance", 0.01))
    window = int(config.get("window", 10))
    profile_data = None
    if profile_name:
        profile_data = bank_matcher._load_profile(profile_name)  # type: ignore[attr-defined]
        if profile_data is None:
            logger.warning("Perfil bancario %s no encontrado, se usarán columnas por defecto", profile_name)
    count = bank_matcher.import_bank_csv(
        csv_path,
        tenant,
        profile=profile_data,
        fixed_account=account_id,
        fixed_direction=direction,
        positive_sign=positive_sign,
    )
    logger.info("Job %s -> importadas %d transacciones", job["name"], count)
    if auto_match:
        matched = bank_matcher.match_transactions(tenant, amount_tolerance=tolerance, date_window_days=window)
        logger.info("Job %s -> conciliados %d documentos", job["name"], matched)


def run_scan_imap(job, config: Dict) -> None:
    try:
        source = ImapSource(
            name=job["name"],
            tenant=config.get("tenant") or job["tenant"] or settings.default_tenant,
            host=config.get("host") or settings.imap_host,
            username=config.get("username") or settings.imap_user,
            password=config.get("password") or settings.imap_password,
            mailbox=config.get("mailbox", settings.imap_mailbox),
            download_dir=config.get("download_dir"),
        )
        processed = _process_source(source)  # currently NotImplemented -> JobSkipped
        logger.info("Job %s -> %d documentos vía IMAP", job["name"], processed)
    except NotImplementedError as exc:
        raise JobSkipped(str(exc))


def run_scan_sftp(job, config: Dict) -> None:
    try:
        source = SftpSource(
            name=job["name"],
            tenant=config.get("tenant") or job["tenant"] or settings.default_tenant,
            host=config.get("host") or settings.sftp_host,
            username=config.get("username") or settings.sftp_user,
            password=config.get("password") or settings.sftp_password,
            remote_path=config.get("remote_path", settings.sftp_remote_path),
            download_dir=config.get("download_dir"),
        )
        processed = _process_source(source)
        logger.info("Job %s -> %d documentos vía SFTP", job["name"], processed)
    except NotImplementedError as exc:
        raise JobSkipped(str(exc))


def run_preflight(job, config: Dict) -> None:
    logger.info("Job %s -> Preflight", job["name"])
    tenant = config.get("tenant") or job["tenant"] or settings.default_tenant
    metrics.print_preflight(tenant)


def run_policy(job, config: Dict) -> None:
    policy_name = config.get("policy", "balanced")
    base_policy = policy_sim.POLICIES.get(policy_name)
    if not base_policy:
        raise ValueError(f"Política desconocida {policy_name}")
    policy = replace(base_policy)
    min_conf = config.get("min_conf")
    if min_conf is not None:
        policy.min_conf_entry = float(min_conf)
    result = policy_sim.simulate_policy(
        policy,
        doc_type_prefix=config.get("doc_type"),
        manifest_path=Path(config["manifest"]) if config.get("manifest") else None,
    )
    logger.info(
        "Job %s -> %s auto-post %.1f%% (total %d)",
        job["name"],
        policy.name,
        result["auto_post_pct"],
        result["total_docs"],
    )


def run_golden(job, config: Dict) -> None:
    args: List[str] = ["python", "-m", "tests.run_golden"]
    if config.get("dirty"):
        args.append("--dirty")
    if config.get("reset", True):
        args.append("--reset")
    if config.get("force", True):
        args.append("--force")
    logger.info("Job %s -> ejecutando %s", job["name"], " ".join(args))
    subprocess.run(args, check=True)


def run_backup_db(job, config: Dict) -> None:
    """Copia la base de datos SQLite a OUT/backups/ con timestamp."""
    src = utils.DB_PATH
    dest_dir = Path(config.get("dest_dir") or (utils.BASE_DIR / "OUT" / "backups"))
    dest_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    dest = dest_dir / f"docs_{ts}.sqlite"
    if not src.exists():
        raise JobSkipped(f"No existe la base de datos en {src}")
    shutil.copy2(src, dest)
    logger.info("Job %s -> backup creado en %s", job["name"], dest)


def run_alerts(job, config: Dict) -> None:
    alerts_list = alerts.evaluate_alerts(tenant=config.get("tenant"))
    if not alerts_list:
        logger.info("Job %s -> sin alertas", job["name"])
        utils.record_job_run(job["id"], "success")
        return
    alerts.send_alerts(alerts_list, tenant=config.get("tenant"))
    logger.info("Job %s -> alertas enviadas: %s", job["name"], alerts_list)


def run_backfill_llm_costs(job, config: Dict) -> None:
    dry = bool(config.get("dry_run"))
    updated = backfill_llm_costs.backfill(dry_run=dry)
    logger.info("Job %s -> backfill cost_eur updated=%d dry=%s", job["name"], updated, dry)


JOB_RUNNERS = {
    "scan_folder": run_scan_folder,
    "scan_imap": run_scan_imap,
    "scan_sftp": run_scan_sftp,
    "import_bank_csv": run_import_bank,
    "run_preflight": run_preflight,
    "run_policy_sim": run_policy,
    "run_golden": run_golden,
    "backup_db": run_backup_db,
    "purge_out": lambda job, config: utils.delete_old_files(
        [
            utils.BASE_DIR / "OUT" / "json",
            utils.BASE_DIR / "OUT" / "csv",
            utils.BASE_DIR / "OUT" / "logs",
            utils.BASE_DIR / "OUT" / "debug",
            utils.BASE_DIR / "IN" / "archivado",
        ],
        int(config.get("days") or config.get("retention_days") or 30),
    ),
    "run_alerts": run_alerts,
    "backfill_llm_costs": run_backfill_llm_costs,
}


def _cmd_add_imap(args: argparse.Namespace) -> None:
    config = {
        "tenant": args.tenant,
        "host": args.host,
        "username": args.username,
        "password": args.password,
        "mailbox": args.mailbox,
    }
    job_id = utils.create_job(args.name, "scan_imap", tenant=args.tenant, config=config, schedule=args.schedule, enabled=True)
    print(f"Job scan_imap creado con id {job_id}")


def _cmd_add_sftp(args: argparse.Namespace) -> None:
    config = {
        "tenant": args.tenant,
        "host": args.host,
        "username": args.username,
        "password": args.password,
        "remote_path": args.remote_path,
    }
    job_id = utils.create_job(args.name, "scan_sftp", tenant=args.tenant, config=config, schedule=args.schedule, enabled=True)
    print(f"Job scan_sftp creado con id {job_id}")


def run_job(job: sqlite3.Row) -> None:  # type: ignore[name-defined]
    config = _json_config(job)
    job_type = job["job_type"]
    runner = JOB_RUNNERS.get(job_type)
    if not runner:
        raise ValueError(f"Tipo de job no soportado: {job_type}")
    max_retries = int(config.get("max_retries", 0) or 0)
    retry_delay = float(config.get("retry_delay", 5) or 5)
    utils.mark_job_started(job["id"], HOSTNAME)
    attempt = 0
    while True:
        try:
            runner(job, config)
            utils.record_job_run(job["id"], "success")
            break
        except JobSkipped as exc:
            utils.record_job_run(job["id"], "skipped", str(exc))
            break
        except NotImplementedError as exc:  # pragma: no cover - defensivo
            utils.record_job_run(job["id"], "skipped", str(exc))
            raise
        except Exception as exc:
            if attempt < max_retries:
                attempt += 1
                wait = max(1.0, retry_delay) * attempt + random.uniform(0, max(1.0, retry_delay))
                logger.warning(
                    "Job %s falló (%s). Reintento %d/%d en %.1fs",
                    job["name"],
                    exc,
                    attempt,
                    max_retries,
                    wait,
                )
                time.sleep(wait)
                continue
            utils.record_job_run(job["id"], "error", str(exc))
            raise


def cmd_list(_: argparse.Namespace) -> None:
    rows = utils.list_jobs()
    if not rows:
        print("No hay jobs definidos.")
        return
    for row in rows:
        print(
            f"[{row['id']}] {row['name']} ({row['job_type']}) tenant={row['tenant'] or '-'} "
            f"schedule={row['schedule'] or 'manual'} enabled={row['enabled']} last_status={row['last_status'] or '-'}"
        )


def cmd_run_due(_: argparse.Namespace) -> None:
    due = utils.next_due_jobs()
    if not due:
        print("No hay jobs pendientes.")
        return
    for job in due:
        print(f"Ejecutando job #{job['id']} ({job['name']}) ...")
        try:
            run_job(job)
        except JobSkipped as exc:
            print(f"Job #{job['id']} omitido: {exc}")
        except Exception as exc:  # pragma: no cover
            print(f"Job #{job['id']} falló: {exc}")


def cmd_run(args: argparse.Namespace) -> None:
    job = utils.get_job(args.id)
    if not job:
        raise SystemExit(f"No existe el job {args.id}")
    try:
        run_job(job)
    except Exception:
        raise


def cmd_enable(args: argparse.Namespace) -> None:
    utils.set_job_enabled(args.id, True)
    print(f"Job {args.id} habilitado")


def cmd_disable(args: argparse.Namespace) -> None:
    utils.set_job_enabled(args.id, False)
    print(f"Job {args.id} deshabilitado")


def cmd_delete(args: argparse.Namespace) -> None:
    utils.job_delete(args.id)
    print(f"Job {args.id} eliminado")


def cmd_add_scan(args: argparse.Namespace) -> None:
    config = {
        "path": args.path,
        "pattern": args.pattern,
        "recursive": args.recursive,
        "archive": args.archive,
        "tenant": args.tenant,
    }
    job_id = utils.create_job(args.name, "scan_folder", tenant=args.tenant, config=config, schedule=args.schedule, enabled=True)
    print(f"Job scan_folder creado con id {job_id}")


def cmd_add_bank(args: argparse.Namespace) -> None:
    config = {
        "path": args.path,
        "profile": args.profile,
        "direction": args.direction,
        "account_id": args.account_id,
        "positive_sign": args.positive_sign,
        "auto_match": args.auto_match,
        "tolerance": args.tolerance,
        "window": args.window,
        "tenant": args.tenant,
    }
    job_id = utils.create_job(args.name, "import_bank_csv", tenant=args.tenant, config=config, schedule=args.schedule, enabled=True)
    print(f"Job import_bank_csv creado con id {job_id}")


def cmd_add_preflight(args: argparse.Namespace) -> None:
    config = {"tenant": args.tenant}
    job_id = utils.create_job(args.name, "run_preflight", tenant=args.tenant, config=config, schedule=args.schedule, enabled=True)
    print(f"Job run_preflight creado con id {job_id}")


def cmd_add_policy(args: argparse.Namespace) -> None:
    config = {
        "policy": args.policy,
        "doc_type": args.doc_type,
        "manifest": args.manifest,
        "min_conf": args.min_conf,
    }
    job_id = utils.create_job(args.name, "run_policy_sim", tenant=args.tenant, config=config, schedule=args.schedule, enabled=True)
    print(f"Job run_policy_sim creado con id {job_id}")


def cmd_add_golden(args: argparse.Namespace) -> None:
    config = {"dirty": args.dirty, "reset": args.reset, "force": args.force}
    job_id = utils.create_job(args.name, "run_golden", config=config, schedule=args.schedule, enabled=True)
    print(f"Job run_golden creado con id {job_id}")


def cmd_add_backup(args: argparse.Namespace) -> None:
    config = {"dest_dir": args.dest_dir}
    job_id = utils.create_job(args.name, "backup_db", config=config, schedule=args.schedule, enabled=True)
    print(f"Job backup_db creado con id {job_id}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CERTIVA Job Scheduler")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="Listar jobs").set_defaults(func=cmd_list)
    sub.add_parser("run-due", help="Ejecuta todos los jobs pendientes").set_defaults(func=cmd_run_due)

    run_cmd = sub.add_parser("run", help="Ejecuta un job concreto")
    run_cmd.add_argument("--id", type=int, required=True)
    run_cmd.set_defaults(func=cmd_run)

    enable_cmd = sub.add_parser("enable", help="Habilita un job")
    enable_cmd.add_argument("--id", type=int, required=True)
    enable_cmd.set_defaults(func=cmd_enable)

    disable_cmd = sub.add_parser("disable", help="Deshabilita un job")
    disable_cmd.add_argument("--id", type=int, required=True)
    disable_cmd.set_defaults(func=cmd_disable)

    delete_cmd = sub.add_parser("delete", help="Elimina un job")
    delete_cmd.add_argument("--id", type=int, required=True)
    delete_cmd.set_defaults(func=cmd_delete)

    scan_cmd = sub.add_parser("add-scan-folder", help="Crea un job de escaneo local")
    scan_cmd.add_argument("--name", required=True)
    scan_cmd.add_argument("--tenant", default=settings.default_tenant)
    scan_cmd.add_argument("--path", required=True)
    scan_cmd.add_argument("--pattern", default="*.pdf")
    scan_cmd.add_argument("--recursive", action="store_true")
    scan_cmd.add_argument("--archive")
    scan_cmd.add_argument("--schedule", help="Ej. every_5m, hourly, daily_02:00")
    scan_cmd.set_defaults(func=cmd_add_scan)

    bank_cmd = sub.add_parser("add-import-bank", help="Crea un job de importación bancaria")
    bank_cmd.add_argument("--name", required=True)
    bank_cmd.add_argument("--tenant", default=settings.default_tenant)
    bank_cmd.add_argument("--path", required=True)
    bank_cmd.add_argument("--profile")
    bank_cmd.add_argument("--direction")
    bank_cmd.add_argument("--account-id")
    bank_cmd.add_argument("--positive-sign", default="credit", choices=["credit", "debit"])
    bank_cmd.add_argument("--auto-match", action="store_true")
    bank_cmd.add_argument("--tolerance", type=float, default=0.01)
    bank_cmd.add_argument("--window", type=int, default=10)
    bank_cmd.add_argument("--schedule")
    bank_cmd.set_defaults(func=cmd_add_bank)

    preflight_cmd = sub.add_parser("add-preflight", help="Crea un job de preflight SII")
    preflight_cmd.add_argument("--name", required=True)
    preflight_cmd.add_argument("--tenant", default=settings.default_tenant)
    preflight_cmd.add_argument("--schedule")
    preflight_cmd.set_defaults(func=cmd_add_preflight)

    policy_cmd = sub.add_parser("add-policy-sim", help="Crea un job de simulación de políticas")
    policy_cmd.add_argument("--name", required=True)
    policy_cmd.add_argument("--policy", default="balanced", choices=list(policy_sim.POLICIES.keys()))
    policy_cmd.add_argument("--tenant")
    policy_cmd.add_argument("--doc-type")
    policy_cmd.add_argument("--manifest")
    policy_cmd.add_argument("--min-conf", type=float)
    policy_cmd.add_argument("--schedule")
    policy_cmd.set_defaults(func=cmd_add_policy)

    golden_cmd = sub.add_parser("add-run-golden", help="Crea un job para ejecutar tests.run_golden")
    golden_cmd.add_argument("--name", required=True)
    golden_cmd.add_argument("--dirty", action="store_true")
    golden_cmd.add_argument("--reset", action="store_true")
    golden_cmd.add_argument("--force", action="store_true")
    golden_cmd.add_argument("--schedule")
    golden_cmd.set_defaults(func=cmd_add_golden)

    backup_cmd = sub.add_parser("add-backup-db", help="Crea un job para respaldar la base de datos SQLite")
    backup_cmd.add_argument("--name", required=True)
    backup_cmd.add_argument("--dest-dir", help="Directorio destino del backup", default=None)
    backup_cmd.add_argument("--schedule")
    backup_cmd.set_defaults(func=cmd_add_backup)

    purge_cmd = sub.add_parser("add-purge-out", help="Crea un job para purgar OUT/ archivos antiguos")
    purge_cmd.add_argument("--name", required=True)
    purge_cmd.add_argument("--days", type=int, help="Días de retención", default=None)
    purge_cmd.add_argument("--schedule")
    purge_cmd.set_defaults(func=lambda args: _cmd_add_purge(args))

    alerts_cmd = sub.add_parser("add-run-alerts", help="Crea un job para evaluar/enviar alertas")
    alerts_cmd.add_argument("--name", required=True)
    alerts_cmd.add_argument("--tenant", help="Tenant opcional")
    alerts_cmd.add_argument("--schedule")
    alerts_cmd.set_defaults(func=lambda args: _cmd_add_alerts(args))

    backfill_cmd = sub.add_parser("add-backfill-llm-costs", help="Job para recalcular cost_eur en llm_calls")
    backfill_cmd.add_argument("--name", required=True)
    backfill_cmd.add_argument("--dry-run", action="store_true")
    backfill_cmd.add_argument("--schedule")
    backfill_cmd.set_defaults(func=lambda args: _cmd_add_backfill(args))

    return parser


def _cmd_add_purge(args: argparse.Namespace) -> None:
    config = {"days": args.days}
    job_id = utils.create_job(args.name, "purge_out", config=config, schedule=args.schedule, enabled=True)
    print(f"Job purge_out creado con id {job_id}")


def _cmd_add_alerts(args: argparse.Namespace) -> None:
    config = {"tenant": args.tenant}
    job_id = utils.create_job(args.name, "run_alerts", config=config, schedule=args.schedule, enabled=True)
    print(f"Job run_alerts creado con id {job_id}")


def _cmd_add_backfill(args: argparse.Namespace) -> None:
    config = {"dry_run": args.dry_run}
    job_id = utils.create_job(args.name, "backfill_llm_costs", config=config, schedule=args.schedule, enabled=True)
    print(f"Job backfill_llm_costs creado con id {job_id}")


def main() -> None:
    utils.configure_logging()
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
