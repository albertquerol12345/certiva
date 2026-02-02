"""Financial reporting module (P&L, VAT, aging, cashflow) for CERTIVA."""
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from . import utils, explain_reports

REPORT_DIR = utils.BASE_DIR / "OUT" / "reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)


def _parse_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).date()
    except ValueError:
        normalized = utils.normalize_date(value)
        if normalized:
            return datetime.fromisoformat(normalized).date()
    return None


def _first_day_of_month(day: date) -> date:
    return date(day.year, day.month, 1)


def _last_day_of_month(day: date) -> date:
    next_month = day.replace(day=28) + timedelta(days=4)
    return next_month - timedelta(days=next_month.day)


def _ensure_date(value: Optional[str], default: Optional[date] = None) -> Optional[date]:
    parsed = _parse_date(value)
    if parsed:
        return parsed
    return default


def _load_entry(doc_id: str) -> Dict:
    entry_path = utils.BASE_DIR / "OUT" / "json" / f"{doc_id}.entry.json"
    if entry_path.exists():
        return utils.read_json(entry_path)
    return {}


def _load_normalized(doc_id: str) -> Dict:
    json_path = utils.BASE_DIR / "OUT" / "json" / f"{doc_id}.json"
    if json_path.exists():
        return utils.read_json(json_path)
    return {}


def iter_docs(
    tenant: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    statuses: Optional[List[str]] = None,
) -> Iterable[Dict]:
    statuses = statuses or ["POSTED"]
    date_from_parsed = _ensure_date(date_from)
    date_to_parsed = _ensure_date(date_to)
    query = "SELECT * FROM docs WHERE status IN ({})".format(
        ",".join(["?"] * len(statuses))
    )
    params: List = list(statuses)
    if tenant:
        query += " AND tenant = ?"
        params.append(tenant)
    with utils.get_connection() as conn:
        rows = conn.execute(query, params).fetchall()
    for row in rows:
        normalized = _load_normalized(row["doc_id"])
        entry = _load_entry(row["doc_id"])
        invoice = normalized.get("invoice", {})
        invoice_date = _parse_date(invoice.get("date"))
        if date_from_parsed and invoice_date and invoice_date < date_from_parsed:
            continue
        if date_to_parsed and invoice_date and invoice_date > date_to_parsed:
            continue
        due_date = _parse_date(invoice.get("due")) or invoice_date
        supplier = normalized.get("supplier", {})
        totals = normalized.get("totals", {})
        metadata = normalized.get("metadata", {})
        entry_lines = entry.get("lines", [])
        yield {
            "doc": row,
            "normalized": normalized,
            "entry": entry,
            "invoice_date": invoice_date,
            "due_date": due_date,
            "supplier": supplier,
            "totals": totals,
            "metadata": metadata,
            "entry_lines": entry_lines,
        }


ACCOUNT_GROUPS = {
    "INGRESOS": ["7"],
    "COSTE_VENTAS": ["60", "61"],
    "GASTOS_OPERACION": ["62", "63", "64", "65"],
    "IMPUESTOS": ["472", "477", "475"],
}


def account_group(account: str) -> str:
    for group, prefixes in ACCOUNT_GROUPS.items():
        if any(account.startswith(prefix) for prefix in prefixes):
            return group
    return "OTROS"


def build_pnl(tenant: Optional[str], date_from: Optional[str], date_to: Optional[str]) -> Dict:
    groups = defaultdict(float)
    total_income = 0.0
    total_expense = 0.0
    for item in iter_docs(tenant, date_from, date_to, statuses=["POSTED"]):
        for line in item["entry_lines"]:
            account = str(line.get("account", ""))
            group = account_group(account)
            debit = float(line.get("debit", 0) or 0)
            credit = float(line.get("credit", 0) or 0)
            if account.startswith("7"):
                amount = credit - debit
                total_income += amount
                groups[group] += amount
            elif account[0] in {"6", "4", "5"}:
                amount = debit - credit
                total_expense += amount
                groups[group] -= amount
    return {
        "tenant": tenant or "all",
        "from": date_from,
        "to": date_to,
        "groups": dict(groups),
        "total_income": total_income,
        "total_expense": total_expense,
        "result": total_income - total_expense,
    }


def build_vat_report(tenant: Optional[str], date_from: Optional[str], date_to: Optional[str]) -> Dict:
    result = {
        "tenant": tenant or "all",
        "from": date_from,
        "to": date_to,
        "soportado": defaultdict(lambda: {"base": 0.0, "vat": 0.0}),
        "repercutido": defaultdict(lambda: {"base": 0.0, "vat": 0.0}),
    }
    for item in iter_docs(tenant, date_from, date_to, statuses=["POSTED"]):
        flow = (item["metadata"].get("flow") or ("AR" if item["doc"]["doc_type"].startswith("sales") else "AP")).upper()
        totals = item["totals"]
        vat_rate = str(item["metadata"].get("vat_rate") or "mixed")
        if flow == "AP":
            bucket = result["soportado"][vat_rate]
        else:
            bucket = result["repercutido"][vat_rate]
        bucket["base"] += float(totals.get("base", 0) or 0)
        bucket["vat"] += float(totals.get("vat", 0) or 0)
    return result


AGING_BUCKETS = [
    ("no_vencido", -9999, 0),
    ("1_30", 1, 30),
    ("31_60", 31, 60),
    ("61_90", 61, 90),
    ("mas_90", 91, 9999),
]


def _bucket_for_days(days: int) -> str:
    for name, start, end in AGING_BUCKETS:
        if start <= days <= end:
            return name
    return "mas_90"


def build_aging(tenant: Optional[str], as_of: str, flow: str) -> Dict:
    as_of_date = _ensure_date(as_of, default=date.today())
    buckets = {name: {"docs": 0, "importe": 0.0} for name, *_ in AGING_BUCKETS}
    by_counterparty = defaultdict(lambda: defaultdict(float))
    for item in iter_docs(tenant, None, as_of, statuses=["POSTED", "ENTRY_READY"]):
        doc_flow = (item["metadata"].get("flow") or ("AR" if item["doc"]["doc_type"].startswith("sales") else "AP")).upper()
        if doc_flow != flow.upper():
            continue
        doc_row = item["doc"]
        pct = doc_row["reconciled_pct"] if "reconciled_pct" in doc_row.keys() else 0.0
        if pct >= 0.999:
            continue
        amount = float(item["totals"].get("gross", 0) or 0) * (1 - float(pct))
        due = item["due_date"] or item["invoice_date"] or as_of_date
        days = (as_of_date - due).days
        bucket_name = _bucket_for_days(days)
        buckets[bucket_name]["docs"] += 1
        buckets[bucket_name]["importe"] += amount
        counterparty = f"{item['supplier'].get('nif', '')} {item['supplier'].get('name', '')}".strip()
        by_counterparty[counterparty][bucket_name] += amount
    return {
        "tenant": tenant or "all",
        "as_of": as_of_date.isoformat(),
        "flow": flow.upper(),
        "buckets": buckets,
        "by_counterparty": {cp: dict(data) for cp, data in by_counterparty.items()},
    }


def build_cashflow_forecast(tenant: Optional[str], from_date: str, months: int = 3) -> Dict:
    start = _ensure_date(from_date, default=date.today())
    periods = []
    current = _first_day_of_month(start)
    for i in range(months):
        month_start = current + timedelta(days=31 * i)
        month_start = _first_day_of_month(month_start)
        month_end = _last_day_of_month(month_start)
        periods.append({"label": month_start.strftime("%Y-%m"), "start": month_start, "end": month_end, "in": 0.0, "out": 0.0})
    periods.append({"label": f"> {months}m", "start": periods[-1]["end"] + timedelta(days=1), "end": date.max, "in": 0.0, "out": 0.0})
    for item in iter_docs(tenant, None, None, statuses=["POSTED", "ENTRY_READY"]):
        flow = (item["metadata"].get("flow") or ("AR" if item["doc"]["doc_type"].startswith("sales") else "AP")).upper()
        doc_row = item["doc"]
        pct = doc_row["reconciled_pct"] if "reconciled_pct" in doc_row.keys() else 0.0
        remaining = float(item["totals"].get("gross", 0) or 0) * (1 - float(pct))
        if remaining <= 0:
            continue
        due = item["due_date"] or item["invoice_date"]
        if not due:
            continue
        for period in periods:
            if period["start"] <= due <= period["end"]:
                if flow == "AR":
                    period["in"] += remaining
                else:
                    period["out"] += remaining
                break
    for period in periods:
        period["net"] = period["in"] - period["out"]
    return {
        "tenant": tenant or "all",
        "from": start.isoformat(),
        "months": months,
        "buckets": periods,
    }


def _report_filename(prefix: str, tenant: Optional[str], suffix: str, ext: str) -> Path:
    tenant_part = tenant or "all"
    return REPORT_DIR / f"{prefix}_{tenant_part}_{suffix}.{ext}"


def _print_table(headers: List[str], rows: List[List[str]]) -> None:
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(str(cell)))
    fmt = " | ".join("{:" + str(w) + "}" for w in widths)
    print(fmt.format(*headers))
    print("-+-".join("-" * w for w in widths))
    for row in rows:
        print(fmt.format(*row))


def run_pnl(args: argparse.Namespace) -> None:
    report = build_pnl(args.tenant, args.date_from, args.date_to)
    if args.format == "text":
        rows = [[group, f"{amount:,.2f}"] for group, amount in report["groups"].items()]
        rows.append(["Resultado", f"{report['result']:,.2f}"])
        _print_table(["Grupo", "Importe"], rows)
    elif args.format == "csv":
        path = _report_filename("pnl", args.tenant, f"{args.date_from}_{args.date_to}", "csv")
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["grupo", "importe"])
            for group, amount in report["groups"].items():
                writer.writerow([group, amount])
        print(f"CSV generado en {path}")
    elif args.format == "json":
        path = _report_filename("pnl", args.tenant, f"{args.date_from}_{args.date_to}", "json")
        path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"JSON generado en {path}")


def run_iva(args: argparse.Namespace) -> None:
    report = build_vat_report(args.tenant, args.date_from, args.date_to)
    if args.format == "text":
        headers = ["Tipo", "Base soportada", "IVA soportado", "Base repercutida", "IVA repercutido"]
        rows = []
        keys = set(list(report["soportado"].keys()) + list(report["repercutido"].keys()))
        for key in sorted(keys):
            soportado = report["soportado"][key]
            repercutido = report["repercutido"][key]
            rows.append([
                key,
                f"{soportado['base']:,.2f}",
                f"{soportado['vat']:,.2f}",
                f"{repercutido['base']:,.2f}",
                f"{repercutido['vat']:,.2f}",
            ])
        _print_table(headers, rows)
    elif args.format == "csv":
        path = _report_filename("iva", args.tenant, f"{args.date_from}_{args.date_to}", "csv")
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["tipo", "base_soportada", "iva_soportado", "base_repercutida", "iva_repercutido"])
            keys = set(list(report["soportado"].keys()) + list(report["repercutido"].keys()))
            for key in sorted(keys):
                soportado = report["soportado"][key]
                repercutido = report["repercutido"][key]
                writer.writerow([key, soportado["base"], soportado["vat"], repercutido["base"], repercutido["vat"]])
        print(f"CSV generado en {path}")
    elif args.format == "json":
        path = _report_filename("iva", args.tenant, f"{args.date_from}_{args.date_to}", "json")
        path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"JSON generado en {path}")


def run_aging(args: argparse.Namespace) -> None:
    report = build_aging(args.tenant, args.as_of, args.flow)
    if args.format == "text":
        rows = [[bucket, info["docs"], f"{info['importe']:,.2f}"] for bucket, info in report["buckets"].items()]
        _print_table(["Bucket", "Docs", "Importe"], rows)
    elif args.format == "csv":
        path = _report_filename("aging", args.tenant, args.as_of, "csv")
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["bucket", "docs", "importe"])
            for bucket, info in report["buckets"].items():
                writer.writerow([bucket, info["docs"], info["importe"]])
        print(f"CSV generado en {path}")
    elif args.format == "json":
        path = _report_filename("aging", args.tenant, args.as_of, "json")
        path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"JSON generado en {path}")


def run_cashflow(args: argparse.Namespace) -> None:
    report = build_cashflow_forecast(args.tenant, args.date_from, args.months)
    if args.format == "text":
        rows = [[bucket["label"], f"{bucket['in']:,.2f}", f"{bucket['out']:,.2f}", f"{bucket['net']:,.2f}"] for bucket in report["buckets"]]
        _print_table(["Periodo", "Cobros", "Pagos", "Neto"], rows)
    elif args.format == "csv":
        path = _report_filename("cashflow", args.tenant, args.date_from, "csv")
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["periodo", "cobros", "pagos", "neto"])
            for bucket in report["buckets"]:
                writer.writerow([bucket["label"], bucket["in"], bucket["out"], bucket["net"]])
        print(f"CSV generado en {path}")
    elif args.format == "json":
        path = _report_filename("cashflow", args.tenant, args.date_from, "json")
        path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"JSON generado en {path}")


def run_explain_pnl(args: argparse.Namespace) -> None:
    report = build_pnl(args.tenant, args.date_from, args.date_to)
    print(explain_reports.explain_pnl(report))


def run_explain_iva(args: argparse.Namespace) -> None:
    report = build_vat_report(args.tenant, args.date_from, args.date_to)
    print(explain_reports.explain_vat(report))


def run_explain_cashflow(args: argparse.Namespace) -> None:
    report = build_cashflow_forecast(args.tenant, args.date_from, args.months)
    print(explain_reports.explain_cashflow(report))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CERTIVA reporting CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    pnl_cmd = sub.add_parser("pnl", help="Informe de pérdidas y ganancias")
    pnl_cmd.add_argument("--tenant")
    pnl_cmd.add_argument("--date-from", required=True)
    pnl_cmd.add_argument("--date-to", required=True)
    pnl_cmd.add_argument("--format", choices=["text", "csv", "json"], default="text")
    pnl_cmd.set_defaults(func=run_pnl)

    iva_cmd = sub.add_parser("iva", help="Informe de IVA")
    iva_cmd.add_argument("--tenant")
    iva_cmd.add_argument("--date-from", required=True)
    iva_cmd.add_argument("--date-to", required=True)
    iva_cmd.add_argument("--format", choices=["text", "csv", "json"], default="text")
    iva_cmd.set_defaults(func=run_iva)

    aging_cmd = sub.add_parser("aging", help="Antigüedad de saldos")
    aging_cmd.add_argument("--tenant")
    aging_cmd.add_argument("--as-of", required=True)
    aging_cmd.add_argument("--flow", choices=["AR", "AP"], required=True)
    aging_cmd.add_argument("--format", choices=["text", "csv", "json"], default="text")
    aging_cmd.set_defaults(func=run_aging)

    cf_cmd = sub.add_parser("cashflow", help="Cashflow forecast")
    cf_cmd.add_argument("--tenant")
    cf_cmd.add_argument("--date-from", required=True)
    cf_cmd.add_argument("--months", type=int, default=3)
    cf_cmd.add_argument("--format", choices=["text", "csv", "json"], default="text")
    cf_cmd.set_defaults(func=run_cashflow)

    ep_cmd = sub.add_parser("explain-pnl", help="Explicar un P&L en lenguaje natural")
    ep_cmd.add_argument("--tenant")
    ep_cmd.add_argument("--date-from", required=True)
    ep_cmd.add_argument("--date-to", required=True)
    ep_cmd.set_defaults(func=run_explain_pnl)

    ei_cmd = sub.add_parser("explain-iva", help="Explicar la liquidación de IVA")
    ei_cmd.add_argument("--tenant")
    ei_cmd.add_argument("--date-from", required=True)
    ei_cmd.add_argument("--date-to", required=True)
    ei_cmd.set_defaults(func=run_explain_iva)

    ec_cmd = sub.add_parser("explain-cashflow", help="Explicar el forecast de cashflow")
    ec_cmd.add_argument("--tenant")
    ec_cmd.add_argument("--date-from", required=True)
    ec_cmd.add_argument("--months", type=int, default=3)
    ec_cmd.set_defaults(func=run_explain_cashflow)

    return parser


def main() -> None:
    utils.configure_logging()
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
