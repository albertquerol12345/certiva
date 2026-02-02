"""Analytics about vendor rules and learning gaps."""
from __future__ import annotations

import argparse
import csv
from collections import Counter
from pathlib import Path
from typing import Dict, Optional, Tuple

from . import utils


RULES_PATH = utils.BASE_DIR / "rules" / "vendor_map.csv"


def load_vendor_rules() -> Dict[Tuple[str, str], Dict[str, str]]:
    if not RULES_PATH.exists():
        return {}
    rules: Dict[str, Dict[str, str]] = {}
    with RULES_PATH.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            key = (row.get("tenant") or "default", row.get("nif") or row.get("supplier_name") or "")
            rules[key] = row
    return rules


def summarize_vendor_rules(tenant: Optional[str] = None) -> Dict[str, int]:
    rules = load_vendor_rules()
    counter: Counter[str] = Counter()
    for (rule_tenant, _), _row in rules.items():
        if tenant and rule_tenant.lower() != tenant.lower():
            continue
        counter[rule_tenant] += 1
    return dict(counter)


def find_no_rule_gaps(limit: int = 5, tenant: Optional[str] = None) -> Dict[str, int]:
    gaps: Counter[str] = Counter()
    with utils.get_connection() as conn:
        query = "SELECT doc_id, issues FROM docs WHERE issues IS NOT NULL"
        params = []
        if tenant:
            query += " AND tenant = ?"
            params.append(tenant)
        docs = conn.execute(query, params).fetchall()
    for row in docs:
        issues = row["issues"] or ""
        if "NO_RULE" not in issues:
            continue
        json_path = utils.BASE_DIR / "OUT" / "json" / f"{row['doc_id']}.json"
        if not json_path.exists():
            continue
        normalized = utils.read_json(json_path)
        supplier = normalized.get("supplier", {})
        name = supplier.get("name") or "Proveedor desconocido"
        gaps[name] += 1
    return dict(gaps.most_common(limit))


def summarize_learning_actions() -> Dict[str, int]:
    with utils.get_connection() as conn:
        learn_rules = conn.execute("SELECT COUNT(*) FROM audit WHERE step = 'LEARN_RULE'").fetchone()[0]
        hitl_accept = conn.execute("SELECT COUNT(*) FROM audit WHERE step = 'HITL_ACCEPT'").fetchone()[0]
    return {"learn_rule": learn_rules, "hitl_accept": hitl_accept}


def mapping_source_breakdown(tenant: Optional[str] = None) -> Dict[str, Dict[str, int]]:
    with utils.get_connection() as conn:
        audit_hitl = {
            row[0] for row in conn.execute("SELECT DISTINCT doc_id FROM audit WHERE step LIKE 'HITL%'").fetchall()
        }
        query = "SELECT doc_id, tenant, status FROM docs WHERE status IN ('POSTED','ENTRY_READY')"
        params = []
        if tenant:
            query += " AND tenant = ?"
            params.append(tenant)
        docs = conn.execute(query, params).fetchall()
    totals: Counter[str] = Counter()
    autopost: Counter[str] = Counter()
    for row in docs:
        if tenant and row["tenant"].lower() != tenant.lower():
            continue
        entry_path = utils.BASE_DIR / "OUT" / "json" / f"{row['doc_id']}.entry.json"
        if not entry_path.exists():
            continue
        entry = utils.read_json(entry_path)
        source = entry.get("mapping_source") or "unknown"
        totals[source] += 1
        if row["status"] == "POSTED" and row["doc_id"] not in audit_hitl:
            autopost[source] += 1
    return {"totals": dict(totals.most_common()), "auto_post": dict(autopost.most_common())}


def report(limit: int = 5, tenant: Optional[str] = None) -> None:
    header = f"=== Informe de aprendizaje de reglas ({tenant}) ===" if tenant else "=== Informe de aprendizaje de reglas ==="
    print(header)
    rule_summary = summarize_vendor_rules(tenant=tenant)
    if rule_summary:
        for tenant, count in rule_summary.items():
            print(f"Tenant {tenant}: {count} reglas registradas")
    else:
        print("No hay reglas en vendor_map.csv")

    actions = summarize_learning_actions()
    print(f"Acciones HITL → LEARN_RULE: {actions['learn_rule']} | HITL_ACCEPT: {actions['hitl_accept']}")

    mapping_stats = mapping_source_breakdown(tenant=tenant)
    if mapping_stats["totals"]:
        print("\nUso de mapping_source (total / auto-post):")
        for source, total in mapping_stats["totals"].items():
            auto = mapping_stats["auto_post"].get(source, 0)
            print(f"  - {source}: {total} docs · {auto} auto-post")

    gaps = find_no_rule_gaps(limit, tenant=tenant)
    if gaps:
        print(f"\nTop {limit} proveedores con incidencias NO_RULE:")
        for supplier, count in gaps.items():
            print(f" - {supplier}: {count} documentos")
    else:
        print("\nNo hay incidencias NO_RULE pendientes 🎉")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analytics sobre aprendizaje de reglas")
    parser.add_argument("report", nargs="?", default="report")
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--tenant", help="Filtrar por tenant")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.report:
        report(limit=args.limit, tenant=args.tenant)


if __name__ == "__main__":
    main()
