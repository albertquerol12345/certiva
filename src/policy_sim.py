"""Simulate different auto-post policies without mutating the database."""
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Dict, List, Optional
import sqlite3

from . import utils


@dataclass
class Policy:
    name: str
    min_conf_entry: float = 0.85
    always_review_issues: List[str] = field(default_factory=list)
    treat_no_rule_as_issue: bool = True
    allow_intracom_without_review: bool = False
    allow_nif_maybe_auto: bool = False
    allow_duplicates: bool = False


POLICIES: Dict[str, Policy] = {
    "conservative": Policy(
        name="conservative",
        min_conf_entry=0.9,
        always_review_issues=[
            "AMOUNT_MISMATCH",
            "FUTURE_DATE",
            "DUP_NIF_NUMBER",
            "DUP_NIF_GROSS",
        ],
        treat_no_rule_as_issue=True,
        allow_intracom_without_review=False,
        allow_nif_maybe_auto=False,
    ),
    "balanced": Policy(
        name="balanced",
        min_conf_entry=0.85,
        always_review_issues=["AMOUNT_MISMATCH", "FUTURE_DATE"],
        treat_no_rule_as_issue=True,
        allow_intracom_without_review=True,
        allow_nif_maybe_auto=False,
    ),
    "aggressive": Policy(
        name="aggressive",
        min_conf_entry=0.75,
        always_review_issues=["AMOUNT_MISMATCH"],
        treat_no_rule_as_issue=False,
        allow_intracom_without_review=True,
        allow_nif_maybe_auto=True,
        allow_duplicates=False,
    ),
}

RISK_WEIGHTS = {
    "AMOUNT_MISMATCH": 5,
    "FUTURE_DATE": 4,
    "NIF_SUSPECT": 3,
    "NIF_AMBIGUOUS": 2,
    "NO_RULE": 1,
    "INTRACOM_IVA0": 1,
    "DUP_NIF_NUMBER": 5,
    "DUP_NIF_GROSS": 4,
}


def _load_manifest_filter(manifest_path: Optional[Path]) -> Optional[set[str]]:
    if not manifest_path or not manifest_path.exists():
        return None
    filenames: set[str] = set()
    with manifest_path.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            filenames.add(row.get("filename"))
    return filenames


def _load_docs(doc_type_prefix: Optional[str], manifest_filter: Optional[set[str]]) -> List[sqlite3.Row]:
    with utils.get_connection() as conn:
        query = "SELECT * FROM docs"
        params: List = []
        if doc_type_prefix:
            query += " WHERE doc_type LIKE ?"
            params.append(f"{doc_type_prefix}%")
        docs = conn.execute(query, params).fetchall()
        if manifest_filter:
            docs = [doc for doc in docs if doc["filename"] in manifest_filter]
    return docs


def _issues_for_doc(doc: sqlite3.Row) -> List[str]:
    if not doc["issues"]:
        return []
    try:
        return json.loads(doc["issues"])
    except json.JSONDecodeError:
        return []


def _global_confidence(doc: sqlite3.Row) -> float:
    global_conf = doc["global_conf"]
    if global_conf is not None:
        return float(global_conf)
    ocr = doc["ocr_conf"] or 1.0
    entry = doc["entry_conf"] or 1.0
    return float(min(ocr, entry))


def simulate_policy(
    policy: Policy,
    doc_type_prefix: Optional[str] = None,
    manifest_path: Optional[Path] = None,
) -> Dict[str, Any]:
    manifest_filter = _load_manifest_filter(manifest_path)
    docs = _load_docs(doc_type_prefix, manifest_filter)
    by_doc_type: Dict[str, Dict[str, int]] = defaultdict(lambda: {"total": 0, "auto": 0})
    issue_counter: Counter[str] = Counter()
    risk_score = 0.0

    for doc in docs:
        doc_type = (doc["doc_type"] or "unknown").lower()
        issues = _issues_for_doc(doc)
        by_doc_type[doc_type]["total"] += 1
        auto_post = True
        global_conf = _global_confidence(doc)

        if global_conf < policy.min_conf_entry:
            auto_post = False
        if policy.treat_no_rule_as_issue and "NO_RULE" in issues:
            auto_post = False
        if any(issue in policy.always_review_issues for issue in issues):
            auto_post = False
        if (not policy.allow_intracom_without_review) and (
            "INTRACOM_IVA0" in issues or "intracom" in doc_type
        ):
            auto_post = False
        if (not policy.allow_nif_maybe_auto) and any(
            issue in {"NIF_SUSPECT", "NIF_AMBIGUOUS"} for issue in issues
        ):
            auto_post = False
        if (not policy.allow_duplicates) and doc["duplicate_flag"]:
            auto_post = False

        if auto_post:
            by_doc_type[doc_type]["auto"] += 1
            for issue in issues:
                issue_counter[issue] += 1
                risk_score += RISK_WEIGHTS.get(issue, 0.5)

    total_docs = len(docs)
    total_auto = sum(info["auto"] for info in by_doc_type.values())
    hitl = total_docs - total_auto
    return {
        "policy": policy.name,
        "total_docs": total_docs,
        "auto_post": total_auto,
        "auto_post_pct": (total_auto / total_docs * 100) if total_docs else 0.0,
        "hitl": hitl,
        "issue_counts": issue_counter,
        "risk_score": risk_score,
        "risk_per_doc": (risk_score / total_auto) if total_auto else 0.0,
        "by_doc_type": by_doc_type,
    }


def _print_report(result: Dict[str, Any]) -> None:
    print(f"Política: {result['policy']}")
    print(f"Documentos analizados: {result['total_docs']}")
    print(
        f"Auto-post: {result['auto_post']} ({result['auto_post_pct']:.1f}%) | "
        f"HITL: {result['hitl']}"
    )
    print(
        f"Riesgo total: {result['risk_score']:.2f} | Riesgo medio por auto-post: {result['risk_per_doc']:.2f}"
    )
    if result["issue_counts"]:
        print("Issues presentes en docs auto-post:")
        for issue, count in result["issue_counts"].most_common(8):
            print(f"  - {issue}: {count}")
    print("\nPor doc_type:")
    for doc_type, info in sorted(result["by_doc_type"].items()):
        total = info["total"]
        auto = info["auto"]
        pct = (auto / total * 100) if total else 0.0
        print(f"  - {doc_type}: auto {auto}/{total} ({pct:.1f}%)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Simulador de políticas de auto-post")
    parser.add_argument("--policy", choices=list(POLICIES.keys()), default="balanced")
    parser.add_argument("--doc-type", help="Filtra por prefijo de doc_type (ej. sales)")
    parser.add_argument("--manifest", type=Path, help="Limita a los docs presentes en el manifest indicado")
    parser.add_argument("--min-conf", type=float, help="Sobrescribe el min_conf_entry de la política")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    base_policy = POLICIES[args.policy]
    policy = replace(base_policy)
    if args.min_conf is not None:
        policy.min_conf_entry = args.min_conf
    result = simulate_policy(policy, doc_type_prefix=args.doc_type, manifest_path=args.manifest)
    _print_report(result)


if __name__ == "__main__":
    main()
