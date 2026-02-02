"""Import and reconcile bank transactions against CERTIVA documents."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import defaultdict
import sqlite3
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from . import utils

DEFAULT_PROFILE = {
    "date": "Date",
    "amount": "Amount",
    "description": "Description",
    "currency": "Currency",
    "account": "Account",
    "direction": "Direction",
    "positive_sign": "credit",  # positive numbers are credits by default
}

AUTO_STATUS = "auto"
MANUAL_STATUS = "manual"


def _normalize_date(value: str) -> str:
    normalized = utils.normalize_date(value)
    if normalized:
        return normalized
    return datetime.fromisoformat(value).date().isoformat()


def _tx_id(tenant: str, date: str, amount: Decimal, description: str) -> str:
    raw = f"{tenant}|{date}|{amount}|{description}".encode()
    return hashlib.sha256(raw).hexdigest()
def compute_tx_hash(row: Dict[str, Any], tenant: str) -> str:
    """Idempotent hash for a bank transaction row."""
    date = _normalize_date(row.get("date") or row.get("Date") or row.get("Fecha") or row.get("fecha"))
    amount = utils.quantize_amount(row.get("amount") or row.get("Amount") or row.get("Importe") or 0)
    desc = (row.get("description") or row.get("Description") or row.get("Descripción") or "").strip()
    account = (row.get("account") or row.get("Account") or "").strip()
    direction = (row.get("direction") or row.get("Direction") or "").strip().upper()
    raw = f"{tenant}|{date}|{amount}|{desc}|{account}|{direction}".encode()
    return hashlib.sha256(raw).hexdigest()


def import_bank_csv(
    csv_path: Path,
    tenant: str,
    profile: Optional[Dict[str, str]] = None,
    fixed_account: Optional[str] = None,
    fixed_direction: Optional[str] = None,
    positive_sign: str = "credit",
) -> int:
    profile = profile or DEFAULT_PROFILE
    inserted = 0
    account_column = profile.get("account")
    direction_column = profile.get("direction")
    positive_sign = (profile.get("positive_sign") or positive_sign or "credit").lower()

    with csv_path.open("r", encoding="utf-8-sig") as fh, utils.get_connection() as conn:
        reader = csv.DictReader(fh)
        for row in reader:
            date_raw = row.get(profile.get("date", "Date"))
            amount_raw = row.get(profile.get("amount", "Amount"))
            if not date_raw or amount_raw in (None, ""):
                continue
            date_iso = _normalize_date(date_raw)
            amount = utils.quantize_amount(amount_raw)
            description = row.get(profile.get("description", "Description"), "").strip()
            currency = row.get(profile.get("currency", "Currency"), "EUR").strip() or "EUR"
            tx_id = _tx_id(tenant, date_iso, amount, description)
            tx_hash = compute_tx_hash(row, tenant)

            account_id = row.get(account_column, "").strip() if account_column else ""
            if fixed_account:
                account_id = fixed_account

            direction = row.get(direction_column, "").strip().upper() if direction_column else ""
            if fixed_direction:
                direction = fixed_direction.strip().upper()
            if not direction:
                is_positive = amount >= 0
                if positive_sign == "credit":
                    direction = "CREDIT" if is_positive else "DEBIT"
                else:
                    direction = "DEBIT" if is_positive else "CREDIT"

            conn.execute(
                """
                INSERT OR REPLACE INTO bank_tx(
                    tx_id, tenant, date, amount, currency, description, account_id, direction, raw, matched_doc_id, tx_hash
                )
                VALUES(
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    COALESCE((SELECT matched_doc_id FROM bank_tx WHERE tx_id = ?), NULL)
                )
                """,
                (
                    tx_id,
                    tenant,
                    date_iso,
                    float(amount),
                    currency,
                    description,
                    account_id or None,
                    direction or None,
                    json.dumps(row, ensure_ascii=False),
                    tx_id,
                    tx_hash,
                ),
            )
            inserted += 1
    return inserted


def _score_candidate(doc_info: Dict, tx_row: Dict) -> float:
    score = 0.0
    description = (tx_row["description"] or "").lower()
    invoice_number = (doc_info.get("invoice_number") or "").lower()
    supplier_name = (doc_info.get("supplier_name") or "").lower()
    date_diff = abs((datetime.fromisoformat(tx_row["date"]) - doc_info["date_obj"]).days)
    amount_diff = abs(doc_info["amount"] - abs(float(tx_row["amount"])))
    currency_tx = (tx_row["currency"] or "EUR").upper()
    if currency_tx != (doc_info.get("currency") or "EUR").upper():
        score -= 0.5

    if amount_diff <= 0.01:
        score += 0.5
    elif amount_diff <= 0.1:
        score += 0.25

    if invoice_number and invoice_number in description:
        score += 0.4

    if supplier_name:
        for token in supplier_name.split():
            if len(token) > 3 and token in description:
                score += 0.2
                break

    if date_diff == 0:
        score += 0.2
    elif date_diff <= 2:
        score += 0.1

    return score


def _expected_direction(doc_type: str) -> str:
    doc_type = (doc_type or "").lower()
    if doc_type.startswith("sales_credit_note"):
        return "DEBIT"
    if doc_type == "credit_note":
        return "CREDIT"
    if doc_type.startswith("sales"):
        return "CREDIT"
    return "DEBIT"


def _load_doc_info(doc_row: Dict) -> Optional[Dict]:
    json_path = utils.BASE_DIR / "OUT" / "json" / f"{doc_row['doc_id']}.json"
    if not json_path.exists():
        return None
    normalized = utils.read_json(json_path)
    totals = normalized.get("totals", {})
    metadata = normalized.get("metadata") or {}
    supplier = normalized.get("supplier", {})
    invoice = normalized.get("invoice", {})

    amount_decimal = utils.quantize_amount(totals.get("gross", 0))
    meta_gross = metadata.get("gross")
    if meta_gross:
        meta_amount = utils.quantize_amount(meta_gross)
        if amount_decimal == 0 or abs(amount_decimal - meta_amount) > Decimal("0.5"):
            amount_decimal = meta_amount
    amount = float(abs(amount_decimal))

    invoice_date = utils.normalize_date(metadata.get("date"))
    if not invoice_date:
        invoice_date = utils.normalize_date(invoice.get("date")) or utils.today_iso()
    date_obj = datetime.fromisoformat(invoice_date)

    invoice_number = metadata.get("invoice_number") or invoice.get("number", "")
    supplier_name = metadata.get("supplier") or supplier.get("name", "")
    doc_type = metadata.get("doc_type") or doc_row["doc_type"] or ""
    flow = (metadata.get("flow") or ("AR" if doc_type.lower().startswith("sales") else "AP")).upper()
    currency = metadata.get("currency") or invoice.get("currency") or "EUR"

    return {
        "doc_id": doc_row["doc_id"],
        "amount": amount,
        "date_obj": date_obj,
        "supplier_name": supplier_name,
        "invoice_number": invoice_number,
        "doc_type": doc_type,
        "flow": flow,
        "currency": currency,
    }


def _compute_remaining_amounts(conn, tx_rows: List[sqlite3.Row]) -> Dict[str, float]:
    usage = defaultdict(float)
    for row in conn.execute("SELECT tx_id, COALESCE(SUM(matched_amount),0) AS used FROM matches GROUP BY tx_id"):
        usage[row[0]] = float(row[1] or 0)
    remaining = {}
    for tx in tx_rows:
        remaining[tx["tx_id"]] = max(0.0, abs(float(tx["amount"])) - usage.get(tx["tx_id"], 0.0))
    return remaining


def _candidate_transactions(
    doc_info: Dict,
    bank_rows: List[sqlite3.Row],
    remaining: Dict[str, float],
    date_window: int,
) -> List[Tuple[float, sqlite3.Row]]:
    expected_direction = _expected_direction(doc_info.get("doc_type", ""))
    candidates: List[Tuple[float, sqlite3.Row]] = []
    for tx in bank_rows:
        if remaining.get(tx["tx_id"], 0.0) <= 0:
            continue
        tx_direction = (tx["direction"] or "").upper()
        if tx_direction and tx_direction != expected_direction:
            continue
        tx_date = datetime.fromisoformat(tx["date"])
        if abs((tx_date - doc_info["date_obj"]).days) > date_window:
            continue
        score = _score_candidate(doc_info, tx)
        if score <= 0:
            continue
        candidates.append((score, tx))
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates


def _delete_auto_matches_for_doc(conn, doc_id: str, remaining: Dict[str, float]) -> None:
    rows = conn.execute(
        "SELECT tx_id, matched_amount FROM matches WHERE doc_id = ? AND status != ?",
        (doc_id, MANUAL_STATUS),
    ).fetchall()
    if rows:
        conn.execute("DELETE FROM matches WHERE doc_id = ? AND status != ?", (doc_id, MANUAL_STATUS))
        for row in rows:
            remaining[row[0]] = remaining.get(row[0], 0.0) + float(row[1] or 0)
            utils.update_tx_match_flag_in_conn(conn, row[0])
        utils.recalc_doc_reconciliation_in_conn(conn, doc_id)


def _record_matches(
    conn: sqlite3.Connection,
    tenant: str,
    doc_id: str,
    tx_allocations: List[Tuple[str, float]],
    strategy: str,
    status: str,
    remaining: Dict[str, float],
) -> None:
    for tx_id, amount in tx_allocations:
        match_id = f"{doc_id}::{tx_id}::{uuid4().hex[:6]}"
        conn.execute(
            """
            INSERT INTO matches(match_id, tenant, doc_id, tx_id, matched_amount, score, strategy, status, created_at)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                match_id,
                tenant,
                doc_id,
                tx_id,
                float(amount),
                1.0,
                strategy,
                status,
                utils.iso_now(),
            ),
        )
        remaining[tx_id] = max(0.0, remaining.get(tx_id, 0.0) - float(amount))
        utils.update_tx_match_flag_in_conn(conn, tx_id)
    utils.recalc_doc_reconciliation_in_conn(conn, doc_id)


def match_transactions(
    tenant: str,
    amount_tolerance: float = 0.01,
    date_window_days: int = 10,
    score_threshold: float = 0.5,
) -> int:
    matched_docs = 0
    with utils.get_connection() as conn:
        docs = conn.execute(
            """
            SELECT doc_id, filename, doc_type, tenant, status
            FROM docs
            WHERE tenant = ? AND status IN ('POSTED','ENTRY_READY')
            """,
            (tenant,),
        ).fetchall()
        bank_rows = conn.execute("SELECT * FROM bank_tx WHERE tenant = ?", (tenant,)).fetchall()
        remaining = _compute_remaining_amounts(conn, bank_rows)
        window = timedelta(days=date_window_days)

        for doc in docs:
            doc_info = _load_doc_info(doc)
            if not doc_info or doc_info["amount"] <= 0:
                continue
            _delete_auto_matches_for_doc(conn, doc["doc_id"], remaining)
            direction = _expected_direction(doc_info.get("doc_type", ""))
            candidates = _candidate_transactions(doc_info, bank_rows, remaining, date_window_days)
            if not candidates:
                continue
            needed = doc_info["amount"]
            allocations: List[Tuple[str, float]] = []
            for score, tx in candidates:
                if score < score_threshold:
                    continue
                available = remaining.get(tx["tx_id"], 0.0)
                if available <= 0:
                    continue
                to_assign = min(available, needed)
                allocations.append((tx["tx_id"], to_assign))
                needed -= to_assign
                if needed <= amount_tolerance:
                    break
            if not allocations:
                continue
            status = "confirmed" if needed <= amount_tolerance else "partial"
            _record_matches(conn, tenant, doc["doc_id"], allocations, f"amount+date+{direction.lower()}", status, remaining)
            matched_docs += 1
    return matched_docs


def gather_bank_stats(
    tenant: Optional[str] = None,
    include_rows: bool = False,
    limit: int = 200,
) -> Dict[str, Any]:
    with utils.get_connection() as conn:
        doc_query = "SELECT reconciled_pct FROM docs"
        tx_query = "SELECT COUNT(*) FROM bank_tx"
        params: Tuple = ()
        if tenant:
            doc_query += " WHERE tenant = ?"
            tx_query += " WHERE tenant = ?"
            params = (tenant,)
        doc_rows = conn.execute(doc_query, params).fetchall()
        tx_total = conn.execute(tx_query, params).fetchone()[0]
        docs_total = len(doc_rows)
        fully = len([row[0] for row in doc_rows if (row[0] or 0) >= 0.999])
        partial = len([row[0] for row in doc_rows if 0 < (row[0] or 0) < 0.999])
        stats = {
            "docs_total": docs_total,
            "docs_fully": fully,
            "docs_partial": partial,
            "docs_unmatched": max(docs_total - fully - partial, 0),
        }
        matched_tx = conn.execute(
            "SELECT COUNT(*) FROM bank_tx WHERE matched_doc_id IS NOT NULL"
            + (" AND tenant = ?" if tenant else ""),
            params,
        ).fetchone()[0]
        stats.update(
            {
                "tx_total": tx_total,
                "tx_matched": matched_tx,
                "tx_unmatched": max(tx_total - matched_tx, 0),
            }
        )
        if include_rows:
            where_clause = "WHERE matched_doc_id IS NULL"
            match_clause = "WHERE matched_doc_id IS NOT NULL"
            params_rows: List[Any] = []
            if tenant:
                where_clause += " AND tenant = ?"
                match_clause += " AND tenant = ?"
                params_rows.append(tenant)
            unmatched_rows = conn.execute(
                f"""
                SELECT tx_id, date, amount, currency, description
                FROM bank_tx
                {where_clause}
                ORDER BY date DESC
                LIMIT ?
                """,
                [*params_rows, limit],
            ).fetchall()
            match_rows = conn.execute(
                f"""
                SELECT tx_id, date, amount, currency, description, matched_doc_id
                FROM bank_tx
                {match_clause}
                ORDER BY date DESC
                LIMIT ?
                """,
                [*params_rows, limit],
            ).fetchall()
            stats["unmatched"] = unmatched_rows
            stats["matches"] = match_rows
        return stats


def stats_report(tenant: Optional[str] = None) -> None:
    stats = gather_bank_stats(tenant)
    print("== Conciliación bancaria ==")
    if tenant:
        print(f"Tenant: {tenant}")
    print(
        f"Facturas conciliadas totalmente: {stats['docs_fully']} / {stats['docs_total']}"
    )
    print(f"Facturas con conciliación parcial: {stats['docs_partial']}")
    print(f"Facturas sin conciliación: {stats['docs_unmatched']}")
    print(f"Movimientos conciliados: {stats['tx_matched']} / {stats['tx_total']}")


def list_pending(tenant: str, doc_type: Optional[str] = None) -> None:
    with utils.get_connection() as conn:
        query = "SELECT doc_id, filename, doc_type, reconciled_pct FROM docs WHERE tenant = ?"
        params: List = [tenant]
        if doc_type:
            query += " AND doc_type LIKE ?"
            params.append(f"{doc_type}%")
        rows = conn.execute(query, params).fetchall()
        pending = [row for row in rows if (row[3] or 0) < 0.999]
        if not pending:
            print("No hay documentos pendientes de conciliación.")
        else:
            print(f"Docs pendientes ({len(pending)}):")
            for row in pending:
                pct = (row[3] or 0) * 100
                print(f" - {row[0][:8]} | {row[2]} | {pct:.1f}% reconciliado | {row[1]}")
        tx_rows = conn.execute(
            "SELECT tx_id, date, amount, description FROM bank_tx WHERE tenant = ? AND matched_doc_id IS NULL",
            (tenant,),
        ).fetchall()
        if tx_rows:
            print(f"Movimientos sin usar ({len(tx_rows)}):")
            for tx in tx_rows[:10]:
                print(f" - {tx['tx_id'][:8]} | {tx['date']} | {tx['amount']:.2f} | {tx['description']}")


def clear_reconciliation(doc_id: str, include_manual: bool = False) -> None:
    utils.clear_matches(doc_id, include_manual=include_manual)
    print(f"Conciliación eliminada para {doc_id}")


def manual_override(doc_id: str, tx_id: str, amount: float, tenant: str) -> None:
    utils.insert_manual_match(doc_id, tx_id, tenant, amount, status=MANUAL_STATUS)
    print(f"Match manual registrado doc={doc_id} ↔ tx={tx_id} ({amount:.2f})")


def clear_match(tx_id: str) -> None:
    """Elimina cualquier match asociado a un movimiento y recalcula doc y tx."""
    with utils.get_connection() as conn:
        doc_rows = conn.execute("SELECT DISTINCT doc_id FROM matches WHERE tx_id = ?", (tx_id,)).fetchall()
        conn.execute("DELETE FROM matches WHERE tx_id = ?", (tx_id,))
        utils.update_tx_match_flag_in_conn(conn, tx_id)
        for row in doc_rows:
            doc_id = row["doc_id"] if isinstance(row, sqlite3.Row) else row[0]
            utils.recalc_doc_reconciliation_in_conn(conn, doc_id)


def override_match(tx_id: str, doc_id: Optional[str], amount: Optional[float] = None, tenant: Optional[str] = None) -> None:
    """Fuerza un match manual (doc↔tx) eliminando matches previos del movimiento."""
    if not doc_id:
        raise ValueError("doc_id requerido para forzar el match")
    with utils.get_connection() as conn:
        tx_row = conn.execute("SELECT * FROM bank_tx WHERE tx_id = ?", (tx_id,)).fetchone()
        if not tx_row:
            raise ValueError(f"tx_id no encontrado: {tx_id}")
        tx_tenant = tx_row["tenant"]
        if tenant and tx_tenant != tenant:
            raise ValueError("El movimiento no pertenece al tenant activo")
        tx_amount = abs(float(tx_row["amount"] or 0))
        if tx_amount <= 0 and amount is None:
            raise ValueError("El movimiento no tiene importe; indica amount manual")
        use_amount = abs(float(amount)) if amount is not None else tx_amount
        if use_amount <= 0:
            raise ValueError("El importe debe ser > 0")
        if use_amount - tx_amount > 0.01:
            raise ValueError("El importe supera al movimiento bancario")
        # Limpia matches existentes del movimiento y recalcula docs afectados
        affected_docs = conn.execute("SELECT DISTINCT doc_id FROM matches WHERE tx_id = ?", (tx_id,)).fetchall()
        conn.execute("DELETE FROM matches WHERE tx_id = ?", (tx_id,))
        for row in affected_docs:
            doc_prev = row["doc_id"] if isinstance(row, sqlite3.Row) else row[0]
            utils.recalc_doc_reconciliation_in_conn(conn, doc_prev)
        # Inserta match manual
        match_id = f"{doc_id}::{tx_id}::{uuid4().hex[:6]}"
        conn.execute(
            """
            INSERT INTO matches(match_id, tenant, doc_id, tx_id, matched_amount, score, strategy, status, created_at, confirmed_at)
            VALUES(?, ?, ?, ?, ?, 1.0, 'manual_override', 'manual', ?, ?)
            """,
            (
                match_id,
                tx_tenant,
                doc_id,
                tx_id,
                float(use_amount),
                utils.iso_now(),
                utils.iso_now(),
            ),
        )
        utils.update_tx_match_flag_in_conn(conn, tx_id)
        utils.recalc_doc_reconciliation_in_conn(conn, doc_id)


def _load_profile(name: Optional[str]) -> Optional[Dict[str, str]]:
    if not name:
        return None
    profile_path = utils.BASE_DIR / "config" / "bank_profiles.json"
    if not profile_path.exists():
        return None
    data = json.loads(profile_path.read_text(encoding="utf-8"))
    return data.get(name)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Conciliación bancaria por CSV")
    sub = parser.add_subparsers(dest="command", required=True)

    imp = sub.add_parser("import", help="Importar un CSV de movimientos")
    imp.add_argument("--tenant", required=True)
    imp.add_argument("--file", type=Path, required=True)
    imp.add_argument("--profile", help="Perfil de columnas opcional")
    imp.add_argument("--account-id", help="Cuenta bancaria fija para todo el CSV")
    imp.add_argument("--direction", help="Dirección fija (CREDIT/DEBIT)")
    imp.add_argument(
        "--positive-sign",
        choices=["credit", "debit"],
        default="credit",
        help="Indica si importes positivos son CREDIT o DEBIT",
    )

    match = sub.add_parser("match", help="Ejecutar matching de movimientos contra facturas")
    match.add_argument("--tenant", required=True)
    match.add_argument("--tolerance", type=float, default=0.01)
    match.add_argument("--window", type=int, default=10, help="Ventana de días para buscar coincidencias")

    stats = sub.add_parser("stats", help="Ver métricas de conciliación")
    stats.add_argument("--tenant")

    listing = sub.add_parser("list", help="Listar pendientes de conciliación")
    listing.add_argument("--tenant", required=True)
    listing.add_argument("--doc-type", help="Filtra por prefijo de doc_type (ej. sales)")

    clr = sub.add_parser("clear", help="Eliminar conciliación de un documento")
    clr.add_argument("--doc", required=True)
    clr.add_argument("--include-manual", action="store_true")

    override = sub.add_parser("override", help="Registrar match manual doc↔movimiento")
    override.add_argument("--tenant", required=True)
    override.add_argument("--doc", required=True)
    override.add_argument("--tx", required=True)
    override.add_argument("--amount", type=float, required=True)

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.command == "import":
        profile = _load_profile(args.profile or args.tenant)
        count = import_bank_csv(
            args.file,
            args.tenant,
            profile=profile,
            fixed_account=args.account_id,
            fixed_direction=args.direction,
            positive_sign=args.positive_sign,
        )
        print(f"Importadas {count} transacciones para {args.tenant}")
    elif args.command == "match":
        count = match_transactions(args.tenant, amount_tolerance=args.tolerance, date_window_days=args.window)
        print(f"Conciliados {count} documentos para {args.tenant}")
    elif args.command == "stats":
        stats_report(args.tenant)
    elif args.command == "list":
        list_pending(args.tenant, args.doc_type)
    elif args.command == "clear":
        clear_reconciliation(args.doc, include_manual=args.include_manual)
    elif args.command == "override":
        manual_override(args.doc, args.tx, args.amount, args.tenant)


if __name__ == "__main__":
    main()
