"""Batch-oriented watcher that scans IN/ folders and processes PDFs headlessly."""
from __future__ import annotations

import argparse
import logging
import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

from . import pipeline, utils
from .batch_writer import build_batch_outputs
from .config import settings

logger = logging.getLogger(__name__)


@dataclass
class FileState:
    path: Path
    last_size: int
    last_change: float
    first_seen: float


class BatchWatcher:
    def __init__(
        self,
        root: Path,
        tenant: str,
        pattern: str,
        recursive: bool,
        archive_dir: Optional[Path],
        batch_size: int,
        batch_timeout: int,
        stabilize_seconds: float = 2.0,
        force: bool = False,
        clock: Callable[[], float] = time.time,
        expected_files: Optional[int] = None,
        expected_pages: Optional[int] = None,
    ) -> None:
        self.root = root
        self.tenant = tenant
        self.pattern = pattern
        self.recursive = recursive
        self.archive_dir = archive_dir
        self.batch_size = max(1, batch_size)
        self.batch_timeout = max(0, batch_timeout)
        self.stabilize_seconds = max(0.0, stabilize_seconds)
        self.force = force
        self.clock = clock
        self.expected_files = expected_files
        self.expected_pages = expected_pages
        self._pending: Dict[Path, FileState] = {}
        self.last_batch_dir: Optional[Path] = None
        if archive_dir:
            archive_dir.mkdir(parents=True, exist_ok=True)
        self.root.mkdir(parents=True, exist_ok=True)

    def _list_files(self) -> List[Path]:
        iterator = self.root.rglob(self.pattern) if self.recursive else self.root.glob(self.pattern)
        return [path for path in iterator if path.is_file()]

    def _refresh_pending(self) -> None:
        now = self.clock()
        current = {path: path.stat().st_size for path in self._list_files()}
        # drop missing
        for path in list(self._pending.keys()):
            if path not in current:
                del self._pending[path]
        # update existing
        for path, size in current.items():
            state = self._pending.get(path)
            if state is None:
                self._pending[path] = FileState(path=path, last_size=size, last_change=now, first_seen=now)
            else:
                if size != state.last_size:
                    state.last_size = size
                    state.last_change = now

    def _select_batch(self) -> List[Path]:
        now = self.clock()
        ready_states = [state for state in self._pending.values() if now - state.last_change >= self.stabilize_seconds]
        if not ready_states:
            return []
        ready_states.sort(key=lambda st: st.first_seen)
        if len(ready_states) >= self.batch_size:
            selected = ready_states[: self.batch_size]
        elif self.batch_timeout and now - ready_states[0].first_seen >= self.batch_timeout:
            selected = ready_states
        else:
            return []
        for state in selected:
            self._pending.pop(state.path, None)
        return [state.path for state in selected]

    def _process_batch(self, paths: List[Path]) -> List[str]:
        doc_ids: List[str] = []
        timestamp = time.strftime("%Y%m%d_%H%M%S", time.gmtime(self.clock()))
        expected_pages = 0
        for path in paths:
            pages = utils.compute_pdf_page_count(path)
            if pages:
                expected_pages += pages
        for path in paths:
            try:
                doc_id = pipeline.process_file(path, tenant=self.tenant, force=self.force)
                if doc_id:
                    doc_ids.append(doc_id)
                    if self.archive_dir:
                        target = self.archive_dir / path.name
                        shutil.move(str(path), target)
                else:
                    logger.warning("Procesamiento de %s no devolvió doc_id", path.name)
            except Exception:  # pragma: no cover - defensive logging
                logger.exception("Error procesando %s", path)
        if doc_ids:
            batch_name = f"watch_{self.root.name}_{timestamp}"
            self.last_batch_dir = build_batch_outputs(
                doc_ids,
                self.tenant,
                batch_name,
                expected_files=self.expected_files or len(paths),
                expected_pages=self.expected_pages or (expected_pages if expected_pages else None),
            )
            logger.info("Lote %s completado (%d docs)", batch_name, len(doc_ids))
        return doc_ids

    def poll(self) -> List[str]:
        self._refresh_pending()
        batch_paths = self._select_batch()
        if not batch_paths:
            return []
        return self._process_batch(batch_paths)


def _acquire_lock(root: Path) -> Path:
    lock_path = root / ".certiva_watcher.lock"
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.close(fd)
    except FileExistsError:
        raise SystemExit(f"Ya existe un watcher activo en {root}")
    return lock_path


def run_once(path: Path, tenant: str, pattern: str, recursive: bool, archive: Optional[Path], limit: Optional[int], force: bool) -> None:
    files = sorted(path.rglob(pattern) if recursive else path.glob(pattern))
    if limit:
        files = files[:limit]
    watcher = BatchWatcher(
        root=path,
        tenant=tenant,
        pattern=pattern,
        recursive=recursive,
        archive_dir=archive,
        batch_size=len(files) or 1,
        batch_timeout=0,
        stabilize_seconds=0.0,
        force=force,
        expected_files=len(files) or None,
    )
    watcher._process_batch(files)


def run_loop(path: Path, tenant: str, pattern: str, recursive: bool, archive: Optional[Path], interval: int, force: bool, batch_size: int, batch_timeout: int, stabilize_seconds: float, expected_files: Optional[int] = None, expected_pages: Optional[int] = None) -> None:
    path.mkdir(parents=True, exist_ok=True)
    lock = _acquire_lock(path)
    watcher = BatchWatcher(
        root=path,
        tenant=tenant,
        pattern=pattern,
        recursive=recursive,
        archive_dir=archive,
        batch_size=batch_size,
        batch_timeout=batch_timeout,
        stabilize_seconds=stabilize_seconds,
        force=force,
        expected_files=expected_files,
        expected_pages=expected_pages,
    )
    logger.info(
        "Watcher iniciado en %s | batch_size=%d timeout=%ss glob=%s",
        path,
        batch_size,
        batch_timeout,
        pattern,
    )
    try:
        while True:
            processed = watcher.poll()
            if processed:
                logger.info("Procesados %d documentos en lote %s", len(processed), watcher.last_batch_dir)
            time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("Watcher detenido por el usuario")
    finally:
        try:
            os.unlink(lock)
        except FileNotFoundError:
            pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Watcher batch para carpetas locales")
    parser.add_argument("--path", default=str(utils.BASE_DIR / "IN"), help="Carpeta a escanear")
    parser.add_argument("--tenant", default=settings.default_tenant, help="Tenant por defecto")
    parser.add_argument("--glob", default=settings.watch_glob, help="Patrón de archivos (glob)")
    parser.add_argument("--recursive", action="store_true", help="Buscar recursivamente")
    parser.add_argument("--archive", help="Mover los ficheros procesados a esta carpeta")
    parser.add_argument("--interval", type=int, help="Si se indica, modo loop cada N segundos")
    parser.add_argument("--limit", type=int, help="Máximo de archivos (modo one-shot)")
    parser.add_argument("--force", action="store_true", help="Forzar reprocesado incluso si existe en docs")
    parser.add_argument("--batch-size", type=int, default=settings.watch_batch_size, help="Tamaño del lote")
    parser.add_argument("--batch-timeout", type=int, default=settings.watch_batch_timeout, help="Timeout en segundos para forzar lote incompleto")
    parser.add_argument("--stabilize-seconds", type=float, default=2.0, help="Segundos requeridos sin cambios antes de procesar")
    parser.add_argument("--expected-files", type=int, help="Nº de ficheros esperados en el lote (para alertas)")
    parser.add_argument("--expected-pages", type=int, help="Nº de páginas esperadas en el lote (para alertas)")
    return parser.parse_args()


def main() -> None:
    utils.configure_logging()
    args = parse_args()
    path = Path(args.path)
    archive = Path(args.archive) if args.archive else None
    if args.interval:
        run_loop(
            path,
            args.tenant,
            args.glob,
            args.recursive,
            archive,
            args.interval,
            args.force,
            batch_size=args.batch_size,
            batch_timeout=args.batch_timeout,
            stabilize_seconds=args.stabilize_seconds,
            expected_files=args.expected_files,
            expected_pages=args.expected_pages,
        )
    else:
        run_once(path, args.tenant, args.glob, args.recursive, archive, args.limit, args.force)


if __name__ == "__main__":
    main()
