"""Procesa los sets golden/dirty y genera métricas por categoría."""
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

from src import pipeline, utils, bank_matcher

BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = BASE_DIR / "tests" / "golden_manifest.csv"
GOLDEN_DIR = BASE_DIR / "tests" / "golden"
DIRTY_DIR = BASE_DIR / "tests" / "golden_dirty"


@dataclass
class DocInfo:
    doc_id: str
    filename: str


def _reset_tables() -> None:
    with utils.get_connection() as conn:
        for table in ("docs", "review_queue", "audit", "dedupe", "bank_tx", "matches"):
            conn.execute(f"DELETE FROM {table}")


def _load_manifest(path: Path) -> Dict[str, Dict[str, str]]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return {row["filename"]: row for row in reader}


def _base_filename(filename: str) -> str:
    if filename.endswith("-dirty.pdf"):
        return filename.replace("-dirty", "")
    return filename


def process_dir(directory: Path, force: bool) -> List[DocInfo]:
    files = sorted(p for p in directory.glob("*.pdf") if p.is_file())
    doc_infos: List[DocInfo] = []
    for file_path in files:
        doc_id = pipeline.process_file(file_path, force=force)
        if doc_id:
            doc_infos.append(DocInfo(doc_id=doc_id, filename=file_path.name))
    return doc_infos


def _fetch_doc_rows(doc_ids: List[str]) -> Tuple[Dict[str, Dict], set[str]]:
    if not doc_ids:
        return {}, set()
    placeholders = ",".join(["?"] * len(doc_ids))
    with utils.get_connection() as conn:
        rows = conn.execute(
            f"SELECT * FROM docs WHERE doc_id IN ({placeholders})",
            doc_ids,
        ).fetchall()
        audit_hitl = {
            row[0]
            for row in conn.execute("SELECT DISTINCT doc_id FROM audit WHERE step LIKE 'HITL%'")
        }
    return {row["doc_id"]: row for row in rows}, audit_hitl


def _calc_durations(doc_rows: Dict[str, Dict]) -> Tuple[List[float], List[float], List[float], List[float]]:
    total_durations: List[float] = []
    ocr_durations: List[float] = []
    validation_durations: List[float] = []
    entry_durations: List[float] = []

    def _parse_ts(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    for row in doc_rows.values():
        received = _parse_ts(row["received_ts"])
        ocr_ts = _parse_ts(row["ocr_ts"])
        validated_ts = _parse_ts(row["validated_ts"])
        entry_ts = _parse_ts(row["entry_ts"])
        posted = _parse_ts(row["posted_ts"])
        if received and posted:
            total_durations.append((posted - received).total_seconds() / 60)
        if received and ocr_ts:
            ocr_durations.append((ocr_ts - received).total_seconds())
        if ocr_ts and validated_ts:
            validation_durations.append((validated_ts - ocr_ts).total_seconds())
        if validated_ts and posted:
            entry_durations.append((posted - validated_ts).total_seconds())
    return total_durations, ocr_durations, validation_durations, entry_durations


def _percentile(values: List[float], pct: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    k = (len(values) - 1) * pct
    f = int(k)
    c = min(f + 1, len(values) - 1)
    if f == c:
        return values[int(k)]
    d0 = values[f] * (c - k)
    d1 = values[c] * (k - f)
    return d0 + d1


def summarize(doc_infos: List[DocInfo], manifest: Dict[str, Dict[str, str]]) -> None:
    doc_ids = [info.doc_id for info in doc_infos]
    doc_rows, audit_hitl = _fetch_doc_rows(doc_ids)
    total_durations, ocr_durations, validation_durations, entry_durations = _calc_durations(doc_rows)

    posted = [row for row in doc_rows.values() if row["status"] == "POSTED" and row["posted_ts"]]
    auto_post = [row for row in posted if row["doc_id"] not in audit_hitl]

    category_stats = defaultdict(lambda: Counter({"total": 0, "autopost": 0, "hitl": 0}))
    doc_type_stats = defaultdict(lambda: Counter({"total": 0, "autopost": 0, "hitl": 0}))
    issue_counter: Counter[str] = Counter()
    auto_total = 0

    for info in doc_infos:
        row = doc_rows.get(info.doc_id)
        if not row:
            continue
        manifest_key = _base_filename(info.filename)
        meta = manifest.get(manifest_key)
        category = meta.get("category") if meta else "sin_categoria"
        stats = category_stats[category]
        stats["total"] += 1
        is_auto = row["status"] == "POSTED" and row["doc_id"] not in audit_hitl
        if is_auto:
            stats["autopost"] += 1
            auto_total += 1
        else:
            stats["hitl"] += 1
        doc_type = (row["doc_type"] or (meta.get("doc_type") if meta else "") or "unknown").lower()
        dt_stats = doc_type_stats[doc_type]
        dt_stats["total"] += 1
        if is_auto:
            dt_stats["autopost"] += 1
        else:
            dt_stats["hitl"] += 1
        issues_raw = row["issues"]
        if issues_raw:
            try:
                issues = json.loads(issues_raw)
            except json.JSONDecodeError:
                issues = []
            for code in issues:
                issue_counter[code] += 1

    print(f"Procesados {len(doc_infos)} documentos")
    if posted:
        print(f"Auto-post (POSTED sin HITL / POSTED): {len(auto_post) / len(posted) * 100:.1f}% ({len(auto_post)}/{len(posted)})")
    else:
        print("Auto-post: 0% (no hay documentos publicados)")
    if doc_infos:
        print(f"Auto-post sobre el lote completo: {auto_total / len(doc_infos) * 100:.1f}% ({auto_total}/{len(doc_infos)})")
    print(f"P50 total (min): {_percentile(total_durations, 0.5):.2f} | P90 total (min): {_percentile(total_durations, 0.9):.2f}")

    print("\nResumen por categoría")
    print("{:<18} {:>5} {:>9} {:>9}".format("Categoría", "Docs", "%Auto", "HITL"))
    for category, stats in sorted(category_stats.items()):
        auto_pct = (stats["autopost"] / stats["total"] * 100) if stats["total"] else 0
        print("{:<18} {:>5} {:>8.1f}% {:>9}".format(category, stats["total"], auto_pct, stats["hitl"]))

    if doc_type_stats:
        print("\nResumen por doc_type")
        print("{:<15} {:>5} {:>9} {:>9}".format("Tipo", "Docs", "%Auto", "HITL"))
        for doc_type, stats in sorted(doc_type_stats.items()):
            auto_pct = (stats["autopost"] / stats["total"] * 100) if stats["total"] else 0
            print("{:<15} {:>5} {:>8.1f}% {:>9}".format(doc_type, stats["total"], auto_pct, stats["hitl"]))

    if issue_counter:
        print("\nIssues más comunes:")
        for code, count in issue_counter.most_common(8):
            print(f"  - {code}: {count}")

    if posted:
        mean_total = sum(total_durations) / len(total_durations) if total_durations else 0
        print(f"Tiempo total medio (min): {mean_total:.2f}")
        if ocr_durations:
            print(f"OCR medio (s): {sum(ocr_durations)/len(ocr_durations):.2f}")
        if validation_durations:
            print(f"Validación media (s): {sum(validation_durations)/len(validation_durations):.2f}")
        if entry_durations:
            print(f"Entrada->Post media (s): {sum(entry_durations)/len(entry_durations):.2f}")

    bank_stats = bank_matcher.gather_bank_stats()
    if bank_stats["tx_total"]:
        print(
            "\nConciliación bancaria: "
            f"{bank_stats['docs_fully']}/{bank_stats['docs_total']} completas · "
            f"{bank_stats['docs_partial']} parciales · "
            f"{bank_stats['tx_matched']}/{bank_stats['tx_total']} movimientos"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Procesa el golden set y muestra métricas")
    parser.add_argument("--dirty", action="store_true", help="Usa tests/golden_dirty")
    parser.add_argument("--force", action="store_true", help="Forzar reprocesado")
    parser.add_argument("--reset", action="store_true", help="Resetea tablas antes de ejecutar")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST, help="Ruta a golden_manifest.csv")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.reset:
        _reset_tables()
    directory = DIRTY_DIR if args.dirty else GOLDEN_DIR
    if not directory.exists():
        raise SystemExit(f"No existe la carpeta {directory}")
    manifest = _load_manifest(args.manifest)
    doc_infos = process_dir(directory, force=args.force)
    summarize(doc_infos, manifest)


if __name__ == "__main__":
    main()
