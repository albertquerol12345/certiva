import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
import random
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from dateutil import parser as date_parser

from .config import settings
from .pii_scrub import scrub_pii

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "db" / "docs.sqlite"
LOG_PATH = BASE_DIR / "OUT" / "logs" / "certiva.log"
SUPPORTED_EXTENSIONS = {".pdf", ".png", ".jpg", ".jpeg"}
MONEY_PLACES = Decimal("0.01")

_logger_configured = False


class PIIFilter(logging.Filter):
    """Sanitize log messages to avoid leaking PII in plaintext."""

    def filter(self, record: logging.LogRecord) -> bool:
        if settings.llm_enable_pii:
            return True
        try:
            message = record.getMessage()
        except Exception:
            return True
        scrubbed = scrub_pii(
            message,
            strict=settings.llm_pii_scrub_strict,
            enabled=True,
        )
        record.msg = scrubbed
        record.args = ()
        return True


def money(value: Any) -> Decimal:
    """Convert any numeric-ish value (with , or .) into Decimal."""
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return Decimal("0")
        clean = text.replace(" ", "")
        if "," in clean and "." in clean:
            clean = clean.replace(".", "")
            clean = clean.replace(",", ".")
        elif "," in clean and "." not in clean:
            clean = clean.replace(",", ".")
        try:
            return Decimal(clean)
        except (InvalidOperation, ValueError):
            return Decimal("0")
    return Decimal("0")


def quantize_amount(value: Any) -> Decimal:
    return money(value).quantize(MONEY_PLACES, rounding=ROUND_HALF_UP)


def decimal_to_float(value: Decimal) -> float:
    return float(value.quantize(MONEY_PLACES, rounding=ROUND_HALF_UP))


def normalize_date(value: Any) -> Optional[str]:
    if not value:
        return None
    try:
        text = str(value).strip()
        if text and len(text) == 10 and text[4] == "-" and text[7] == "-" and text[:4].isdigit():
            return text
        dt = date_parser.parse(str(value), dayfirst=True, yearfirst=False)
        return dt.date().isoformat()
    except (ValueError, TypeError, OverflowError):
        return None


def today_iso() -> str:
    return date.today().isoformat()


def normalize_currency(value: Optional[str]) -> str:
    if not value:
        return "EUR"
    clean = value.strip().upper()
    return clean or "EUR"


_NIF_LETTERS = "TRWAGMYFPDXBNJZSQVHLCKE"
_CIF_LETTERS = "JABCDEFGHI"


def validate_spanish_nif(nif: str) -> str:
    """
    Returns one of: 'valid', 'maybe', 'invalid'.
    """
    if not nif:
        return "invalid"
    code = nif.strip().upper().replace(" ", "").replace("-", "")
    if code.startswith("ES") and len(code) > 2:
        base_status = validate_spanish_nif(code[2:])
        return base_status if base_status == "valid" else "maybe"
    if code.startswith("EU") and len(code) > 4:
        return "valid"
    if not (8 <= len(code) <= 10):
        return "invalid"
    if code.isdigit():
        return "invalid"

    # DNI
    if len(code) == 9 and code[:-1].isdigit() and code[-1].isalpha():
        expected = _NIF_LETTERS[int(code[:-1]) % 23]
        return "valid" if expected == code[-1] else "maybe"

    # NIE
    if len(code) == 9 and code[0] in "XYZ" and code[1:-1].isdigit() and code[-1].isalpha():
        prefix = {"X": "0", "Y": "1", "Z": "2"}[code[0]]
        expected = _NIF_LETTERS[int(prefix + code[1:-1]) % 23]
        return "valid" if expected == code[-1] else "maybe"

    # CIF
    if len(code) == 9 and code[0] in "ABCDEFGHJNPQRSUVW" and code[1:-1].isdigit():
        digits = code[1:8]
        even_sum = sum(int(d) for d in digits[1::2])
        odd_sum = 0
        for d in digits[::2]:
            prod = int(d) * 2
            odd_sum += prod // 10 + prod % 10
        control_digit = (10 - ((even_sum + odd_sum) % 10)) % 10
        expected_digit = str(control_digit)
        expected_letter = _CIF_LETTERS[control_digit]
        last = code[-1]
        if code[0] in "PQRSNW":
            return "valid" if last == expected_letter else "maybe"
        if code[0] in "ABEH":
            return "valid" if last == expected_digit else "maybe"
        return "valid" if last in (expected_digit, expected_letter) else "maybe"

    # Fallback regex
    if len(code) >= 8:
        return "maybe"
    return "invalid"

def configure_logging() -> None:
    global _logger_configured
    if _logger_configured:
        return
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(LOG_PATH, maxBytes=5 * 1024 * 1024, backupCount=5)
    fmt = "%(asctime)s [%(levelname)s] %(name)s :: %(message)s"
    pii_filter = PIIFilter()
    handler.addFilter(pii_filter)
    logging.basicConfig(level=logging.INFO, handlers=[handler], format=fmt)
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(fmt))
    console.addFilter(pii_filter)
    logging.getLogger().addHandler(console)
    _logger_configured = True

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def iso_now() -> str:
    return utcnow().isoformat()

def compute_sha256(path: Path) -> str:
    sha = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            sha.update(chunk)
    return sha.hexdigest()


def compute_pdf_page_count(path: Path) -> Optional[int]:
    """Cuenta páginas PDF usando pdfminer; None si no se puede leer."""
    if path.suffix.lower() != ".pdf":
        return None
    try:
        from pdfminer.pdfpage import PDFPage  # type: ignore
    except Exception:
        return None
    try:
        with path.open("rb") as fh:
            return sum(1 for _ in PDFPage.get_pages(fh))
    except Exception:
        return None


def json_dump(data: Dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=2)


def delete_old_files(paths: List[Path], max_age_days: int) -> int:
    """
    Elimina archivos en `paths` con mtime anterior a max_age_days.
    Devuelve el número de archivos borrados.
    """
    cutoff = datetime.now() - timedelta(days=max_age_days)
    removed = 0
    for base in paths:
        if not base.exists():
            continue
        for f in base.rglob("*"):
            if f.is_file():
                try:
                    mtime = datetime.fromtimestamp(f.stat().st_mtime)
                    if mtime < cutoff:
                        f.unlink()
                        removed += 1
                except OSError:
                    continue
    return removed


def compute_llm_cost(model: str, prompt_tokens: float, completion_tokens: float) -> float:
    """
    Calcula coste aproximado en € según modelo y tarifas configuradas.
    Si no se puede determinar el modelo, usa tarifas premium.
    """
    model_lower = (model or "").lower()
    if "mini" in model_lower or "small" in model_lower:
        pricing_in = settings.openai_mini_in_per_mtok
        pricing_out = settings.openai_mini_out_per_mtok
    else:
        pricing_in = settings.openai_premium_in_per_mtok
        pricing_out = settings.openai_premium_out_per_mtok
    # tokens están en unidades, tarifas en MTok
    cost = (prompt_tokens / 1000000.0) * pricing_in + (completion_tokens / 1000000.0) * pricing_out
    return float(round(cost, 6))

def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS docs (
            doc_id TEXT PRIMARY KEY,
            sha256 TEXT,
            filename TEXT,
            tenant TEXT,
            status TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            ocr_conf REAL,
            entry_conf REAL,
            global_conf REAL,
            doc_type TEXT,
            issues TEXT,
            duplicate_flag INTEGER DEFAULT 0,
            reconciled_amount REAL DEFAULT 0,
            reconciled_pct REAL DEFAULT 0,
            paid_flag INTEGER DEFAULT 0,
            paid_ts TIMESTAMP,
            received_ts TIMESTAMP,
            ocr_ts TIMESTAMP,
            validated_ts TIMESTAMP,
            entry_ts TIMESTAMP,
            posted_ts TIMESTAMP,
            error TEXT,
            page_count INTEGER
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ocr_queue (
            doc_id TEXT PRIMARY KEY,
            tenant TEXT,
            path TEXT,
            tries INTEGER DEFAULT 0,
            last_error TEXT,
            enqueued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    try:
        cur.execute("PRAGMA table_info(docs)")
        columns = {row[1] for row in cur.fetchall()}
        if "error" not in columns:
            cur.execute("ALTER TABLE docs ADD COLUMN error TEXT")
        if "global_conf" not in columns:
            cur.execute("ALTER TABLE docs ADD COLUMN global_conf REAL")
        if "issues" not in columns:
            cur.execute("ALTER TABLE docs ADD COLUMN issues TEXT")
        if "doc_type" not in columns:
            cur.execute("ALTER TABLE docs ADD COLUMN doc_type TEXT")
        if "reconciled_amount" not in columns:
            cur.execute("ALTER TABLE docs ADD COLUMN reconciled_amount REAL DEFAULT 0")
        if "reconciled_pct" not in columns:
            cur.execute("ALTER TABLE docs ADD COLUMN reconciled_pct REAL DEFAULT 0")
        if "paid_flag" not in columns:
            cur.execute("ALTER TABLE docs ADD COLUMN paid_flag INTEGER DEFAULT 0")
        if "paid_ts" not in columns:
            cur.execute("ALTER TABLE docs ADD COLUMN paid_ts TIMESTAMP")
        for column, ddl in {
            "ocr_provider": "TEXT",
            "llm_provider": "TEXT",
            "ocr_time_ms": "REAL",
            "llm_time_ms": "REAL",
            "total_time_ms": "REAL",
            "rules_time_ms": "REAL",
            "llm_model_used": "TEXT",
            "llm_tokens_in": "REAL",
            "llm_tokens_out": "REAL",
            "llm_cost_eur": "REAL",
            "page_count": "INTEGER",
        }.items():
            if column not in columns:
                cur.execute(f"ALTER TABLE docs ADD COLUMN {column} {ddl}")
    except sqlite3.OperationalError:
        pass
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS review_queue (
            doc_id TEXT PRIMARY KEY,
            reason TEXT,
            suggested TEXT,
            tenant TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    try:
        cur.execute("PRAGMA table_info(review_queue)")
        columns = {row[1] for row in cur.fetchall()}
        if "tenant" not in columns:
            cur.execute("ALTER TABLE review_queue ADD COLUMN tenant TEXT")
    except sqlite3.OperationalError:
        pass
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS audit (
            doc_id TEXT,
            step TEXT,
            who TEXT,
            before TEXT,
            after TEXT,
            ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dedupe (
            doc_id TEXT PRIMARY KEY,
            tenant TEXT,
            supplier_nif TEXT,
            inv_number TEXT,
            inv_date TEXT,
            gross NUMERIC
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bank_tx (
            tx_id TEXT PRIMARY KEY,
            tenant TEXT,
            date TEXT,
            amount REAL,
            currency TEXT,
            description TEXT,
            account_id TEXT,
            direction TEXT,
            raw TEXT,
            matched_doc_id TEXT,
            tx_hash TEXT
        )
        """
    )
    try:
        cur.execute("PRAGMA table_info(bank_tx)")
        columns = {row[1] for row in cur.fetchall()}
        if "account_id" not in columns:
            cur.execute("ALTER TABLE bank_tx ADD COLUMN account_id TEXT")
        if "direction" not in columns:
            cur.execute("ALTER TABLE bank_tx ADD COLUMN direction TEXT")
        if "tx_hash" not in columns:
            cur.execute("ALTER TABLE bank_tx ADD COLUMN tx_hash TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute("PRAGMA table_info(dedupe)")
        columns = {row[1] for row in cur.fetchall()}
        if "tenant" not in columns:
            cur.execute("ALTER TABLE dedupe ADD COLUMN tenant TEXT")
    except sqlite3.OperationalError:
        pass
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS matches (
            match_id TEXT PRIMARY KEY,
            tenant TEXT,
            doc_id TEXT,
            tx_id TEXT,
            matched_amount REAL,
            score REAL,
            strategy TEXT,
            status TEXT DEFAULT 'auto',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            confirmed_at TIMESTAMP
        )
        """
    )
    try:
        cur.execute("PRAGMA table_info(matches)")
        columns = {row[1] for row in cur.fetchall()}
        if "matched_amount" not in columns:
            cur.execute("ALTER TABLE matches ADD COLUMN matched_amount REAL")
        if "status" not in columns:
            cur.execute("ALTER TABLE matches ADD COLUMN status TEXT DEFAULT 'auto'")
        if "confirmed_at" not in columns:
            cur.execute("ALTER TABLE matches ADD COLUMN confirmed_at TIMESTAMP")
    except sqlite3.OperationalError:
        pass
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            job_type TEXT NOT NULL,
            tenant TEXT,
            config TEXT,
            schedule TEXT,
            enabled INTEGER DEFAULT 1,
            last_run_at TIMESTAMP,
            last_status TEXT,
            last_error TEXT,
            run_started_at TIMESTAMP,
            run_host TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    try:
        cur.execute("PRAGMA table_info(jobs)")
        columns = {row[1] for row in cur.fetchall()}
        if "schedule" not in columns:
            cur.execute("ALTER TABLE jobs ADD COLUMN schedule TEXT")
        if "enabled" not in columns:
            cur.execute("ALTER TABLE jobs ADD COLUMN enabled INTEGER DEFAULT 1")
        if "last_error" not in columns:
            cur.execute("ALTER TABLE jobs ADD COLUMN last_error TEXT")
        if "run_started_at" not in columns:
            cur.execute("ALTER TABLE jobs ADD COLUMN run_started_at TIMESTAMP")
        if "run_host" not in columns:
            cur.execute("ALTER TABLE jobs ADD COLUMN run_host TEXT")
    except sqlite3.OperationalError:
        pass
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS llm_calls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task TEXT,
            provider TEXT,
            model TEXT,
            prompt_tokens INTEGER,
            completion_tokens INTEGER,
            latency_ms REAL,
            tenant TEXT,
            username TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            error TEXT
        )
        """
    )
    try:
        cur.execute("PRAGMA table_info(llm_calls)")
        columns = {row[1] for row in cur.fetchall()}
        if "tenant" not in columns:
            cur.execute("ALTER TABLE llm_calls ADD COLUMN tenant TEXT")
        if "username" not in columns:
            cur.execute("ALTER TABLE llm_calls ADD COLUMN username TEXT")
        if "cost_eur" not in columns:
            cur.execute("ALTER TABLE llm_calls ADD COLUMN cost_eur REAL")
    except sqlite3.OperationalError:
        pass
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'operator',
            is_active INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS login_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT,
            ip TEXT,
            success INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    index_statements = [
        "CREATE INDEX IF NOT EXISTS idx_docs_status_tenant ON docs(status, tenant)",
        "CREATE INDEX IF NOT EXISTS idx_docs_doc_type ON docs(doc_type)",
        "CREATE INDEX IF NOT EXISTS idx_docs_updated_at ON docs(updated_at)",
        "CREATE INDEX IF NOT EXISTS idx_bank_tx_tenant_matched ON bank_tx(tenant, matched_doc_id)",
        "CREATE INDEX IF NOT EXISTS idx_matches_doc_id ON matches(doc_id)",
        "CREATE INDEX IF NOT EXISTS idx_jobs_enabled_schedule ON jobs(enabled, schedule)",
        "CREATE INDEX IF NOT EXISTS idx_review_queue_created_at ON review_queue(created_at)",
        "CREATE INDEX IF NOT EXISTS idx_audit_doc_step ON audit(doc_id, step)",
        "CREATE INDEX IF NOT EXISTS idx_dedupe_tenant_nif ON dedupe(tenant, supplier_nif, inv_number, inv_date)",
        "CREATE INDEX IF NOT EXISTS idx_login_attempts_user_time ON login_attempts(username, created_at)",
    ]
    for stmt in index_statements:
        try:
            cur.execute(stmt)
        except sqlite3.OperationalError:
            pass
    conn.commit()
    conn.close()

init_db()

@contextmanager
def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=3000;")
    except sqlite3.OperationalError:
        pass
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()

def insert_or_get_doc(doc_id: str, sha256: str, filename: str, tenant: str) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO docs(doc_id, sha256, filename, tenant, status, received_ts, created_at, updated_at)
            VALUES(?, ?, ?, ?, 'RECEIVED', ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (doc_id, sha256, filename, tenant, iso_now()),
        )

def update_doc_status(doc_id: str, status: str, **kwargs: Any) -> None:
    columns = ["status = ?", "updated_at = ?"]
    values: List[Any] = [status, iso_now()]
    for key, value in kwargs.items():
        columns.append(f"{key} = ?")
        values.append(value)
    values.append(doc_id)
    with get_connection() as conn:
        conn.execute(f"UPDATE docs SET {', '.join(columns)} WHERE doc_id = ?", values)


def update_doc_metadata(doc_id: str, **kwargs: Any) -> None:
    if not kwargs:
        return
    kwargs["updated_at"] = iso_now()
    columns = [f"{key} = ?" for key in kwargs]
    values = list(kwargs.values())
    values.append(doc_id)
    with get_connection() as conn:
        conn.execute(f"UPDATE docs SET {', '.join(columns)} WHERE doc_id = ?", values)


def persist_issues(doc_id: str, issues: List[str]) -> None:
    import json as _json

    payload = _json.dumps(issues, ensure_ascii=False)
    with get_connection() as conn:
        conn.execute(
            "UPDATE docs SET issues = ?, updated_at = ? WHERE doc_id = ?",
            (payload, iso_now(), doc_id),
        )


def persist_batch_warnings(batch_name: str, warnings: List[str]) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO audit(doc_id, step, who, before, after, ts)
            VALUES(?, 'BATCH_WARNING', 'system', NULL, ?, CURRENT_TIMESTAMP)
            """,
            (batch_name, ",".join(warnings)),
        )

def record_stage_timestamp(doc_id: str, stage: str) -> None:
    stage_map = {
        "received": "received_ts",
        "ocr": "ocr_ts",
        "validated": "validated_ts",
        "entry": "entry_ts",
        "posted": "posted_ts",
    }
    column = stage_map.get(stage)
    if not column:
        return
    with get_connection() as conn:
        conn.execute(
            f"UPDATE docs SET {column} = ?, updated_at = ? WHERE doc_id = ?",
            (iso_now(), iso_now(), doc_id),
        )


def enqueue_ocr_retry(doc_id: str, tenant: str, path: str, error: str) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO ocr_queue(doc_id, tenant, path, tries, last_error, enqueued_at)
            VALUES(?, ?, ?, COALESCE((SELECT tries FROM ocr_queue WHERE doc_id = ?),0), ?, CURRENT_TIMESTAMP)
            """,
            (doc_id, tenant, path, doc_id, error[:500]),
        )


def iter_ocr_queue(limit: int = 10):
    with get_connection() as conn:
        return conn.execute(
            "SELECT doc_id, tenant, path, tries, last_error, enqueued_at FROM ocr_queue ORDER BY enqueued_at ASC LIMIT ?",
            (limit,),
        ).fetchall()


def mark_ocr_retry(doc_id: str, *, success: bool, error: Optional[str] = None) -> None:
    with get_connection() as conn:
        if success:
            conn.execute("DELETE FROM ocr_queue WHERE doc_id = ?", (doc_id,))
        else:
            conn.execute(
                "UPDATE ocr_queue SET tries = tries + 1, last_error = ?, enqueued_at = CURRENT_TIMESTAMP WHERE doc_id = ?",
                (error[:500] if error else None, doc_id),
            )

def add_review_item(doc_id: str, reason: str, suggested: Optional[Dict[str, Any]], tenant: Optional[str] = None) -> None:
    payload = json.dumps(suggested or {}, ensure_ascii=False)
    doc_row = get_doc(doc_id)
    tenant_value = tenant or (doc_row["tenant"] if doc_row else settings.default_tenant)
    with get_connection() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO review_queue(doc_id, reason, suggested, tenant, created_at)
            VALUES(?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (doc_id, reason, payload, tenant_value),
        )

def remove_review_item(doc_id: str) -> None:
    with get_connection() as conn:
        conn.execute("DELETE FROM review_queue WHERE doc_id = ?", (doc_id,))


def fetch_matches_for_doc(doc_id: str) -> List[sqlite3.Row]:
    with get_connection() as conn:
        cur = conn.execute(
            """
            SELECT m.*, b.date, b.amount, b.currency, b.description, b.account_id, b.direction
            FROM matches m
            LEFT JOIN bank_tx b ON b.tx_id = m.tx_id
            WHERE m.doc_id = ?
            ORDER BY b.date
            """,
            (doc_id,),
        )
        return cur.fetchall()


def _doc_gross_amount(doc_id: str) -> float:
    json_path = BASE_DIR / "OUT" / "json" / f"{doc_id}.json"
    if not json_path.exists():
        return 0.0
    data = read_json(json_path)
    gross_raw = (data.get("totals") or {}).get("gross", 0)
    return float(quantize_amount(gross_raw)) if gross_raw is not None else 0.0


def _update_tx_match_flag(conn: sqlite3.Connection, tx_id: str) -> None:
    row = conn.execute(
        "SELECT COUNT(DISTINCT doc_id) AS cnt FROM matches WHERE tx_id = ?",
        (tx_id,),
    ).fetchone()
    count = row[0] if row else 0
    if count == 0:
        conn.execute("UPDATE bank_tx SET matched_doc_id = NULL WHERE tx_id = ?", (tx_id,))
    elif count == 1:
        doc_row = conn.execute(
            "SELECT doc_id FROM matches WHERE tx_id = ? LIMIT 1",
            (tx_id,),
        ).fetchone()
        conn.execute(
            "UPDATE bank_tx SET matched_doc_id = ? WHERE tx_id = ?",
            (doc_row[0], tx_id),
        )
    else:
        conn.execute("UPDATE bank_tx SET matched_doc_id = 'MULTI' WHERE tx_id = ?", (tx_id,))


def _recalc_doc_reconciliation_conn(conn: sqlite3.Connection, doc_id: str) -> None:
    total_matched = conn.execute(
        "SELECT COALESCE(SUM(matched_amount), 0) FROM matches WHERE doc_id = ?",
        (doc_id,),
    ).fetchone()[0]
    gross = _doc_gross_amount(doc_id)
    pct = float(total_matched / gross) if gross else 0.0
    doc_row = conn.execute(
        "SELECT doc_type FROM docs WHERE doc_id = ?",
        (doc_id,),
    ).fetchone()
    doc_type = (doc_row[0] or "").lower() if doc_row and doc_row[0] else ""
    is_sales = doc_type.startswith("sales")
    paid_flag = 1 if is_sales and pct >= 0.999 else 0
    paid_ts_value = iso_now() if paid_flag else None
    conn.execute(
        """
        UPDATE docs
        SET reconciled_amount = ?,
            reconciled_pct = ?,
            paid_flag = ?,
            paid_ts = CASE WHEN ? = 1 THEN COALESCE(paid_ts, ?) ELSE NULL END,
            updated_at = ?
        WHERE doc_id = ?
        """,
        (
            float(total_matched),
            float(min(pct, 1.0)),
            paid_flag,
            paid_flag,
            paid_ts_value,
            iso_now(),
            doc_id,
        ),
    )


def recalc_doc_reconciliation(doc_id: str) -> None:
    with get_connection() as conn:
        _recalc_doc_reconciliation_conn(conn, doc_id)


def _clear_matches_conn(conn: sqlite3.Connection, doc_id: str, include_manual: bool = False) -> None:
    rows = conn.execute(
        "SELECT tx_id, matched_amount FROM matches WHERE doc_id = ? AND (? = 1 OR status != 'manual')",
        (doc_id, 1 if include_manual else 0),
    ).fetchall()
    conn.execute(
        "DELETE FROM matches WHERE doc_id = ? AND (? = 1 OR status != 'manual')",
        (doc_id, 1 if include_manual else 0),
    )
    for row in rows:
        _update_tx_match_flag(conn, row[0])
    _recalc_doc_reconciliation_conn(conn, doc_id)


def clear_matches(doc_id: str, include_manual: bool = False) -> None:
    with get_connection() as conn:
        _clear_matches_conn(conn, doc_id, include_manual)


def recalc_doc_reconciliation_in_conn(conn: sqlite3.Connection, doc_id: str) -> None:
    _recalc_doc_reconciliation_conn(conn, doc_id)


def update_tx_match_flag(tx_id: str) -> None:
    with get_connection() as conn:
        _update_tx_match_flag(conn, tx_id)


def clear_matches_in_conn(conn: sqlite3.Connection, doc_id: str, include_manual: bool = False) -> None:
    _clear_matches_conn(conn, doc_id, include_manual)


def update_tx_match_flag_in_conn(conn: sqlite3.Connection, tx_id: str) -> None:
    _update_tx_match_flag(conn, tx_id)


# --- Jobs helpers ---

def create_job(
    name: str,
    job_type: str,
    tenant: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
    schedule: Optional[str] = None,
    enabled: bool = True,
) -> int:
    payload = json.dumps(config or {}, ensure_ascii=False)
    with get_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO jobs(name, job_type, tenant, config, schedule, enabled, last_status, created_at, updated_at)
            VALUES(?, ?, ?, ?, ?, ?, NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (name, job_type, tenant, payload, schedule, 1 if enabled else 0),
        )
        return cur.lastrowid


def list_jobs(only_enabled: Optional[bool] = None) -> List[sqlite3.Row]:
    query = "SELECT * FROM jobs"
    params: List[Any] = []
    if only_enabled is not None:
        query += " WHERE enabled = ?"
        params.append(1 if only_enabled else 0)
    query += " ORDER BY id"
    with get_connection() as conn:
        return conn.execute(query, params).fetchall()


def get_job(job_id: int) -> Optional[sqlite3.Row]:
    with get_connection() as conn:
        return conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()


def _update_job_fields(job_id: int, **fields: Any) -> None:
    if not fields:
        return
    columns = [f"{key} = ?" for key in fields.keys()]
    values = list(fields.values())
    columns.append("updated_at = ?")
    values.append(iso_now())
    values.append(job_id)
    with get_connection() as conn:
        conn.execute(f"UPDATE jobs SET {', '.join(columns)} WHERE id = ?", values)


def set_job_enabled(job_id: int, enabled: bool) -> None:
    _update_job_fields(job_id, enabled=1 if enabled else 0)


def update_job_schedule(job_id: int, schedule: Optional[str]) -> None:
    _update_job_fields(job_id, schedule=schedule)


def record_job_run(job_id: int, status: str, error: Optional[str] = None) -> None:
    _update_job_fields(
        job_id,
        last_run_at=iso_now(),
        last_status=status,
        last_error=error[:500] if error else None,
        run_started_at=None,
        run_host=None,
    )


def update_job_config(job_id: int, config: Dict[str, Any]) -> None:
    _update_job_fields(job_id, config=json.dumps(config or {}, ensure_ascii=False))


def job_delete(job_id: int) -> None:
    with get_connection() as conn:
        conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))


def mark_job_started(job_id: int, host: str) -> None:
    _update_job_fields(job_id, run_started_at=iso_now(), run_host=host)


def clear_job_start(job_id: int) -> None:
    _update_job_fields(job_id, run_started_at=None, run_host=None)


# --- User helpers ---

def get_user_by_username(username: str) -> Optional[sqlite3.Row]:
    with get_connection() as conn:
        return conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()


def list_users() -> List[sqlite3.Row]:
    with get_connection() as conn:
        return conn.execute("SELECT * FROM users ORDER BY username").fetchall()


def create_user(username: str, password_hash: str, role: str = "admin", is_active: bool = True) -> int:
    with get_connection() as conn:
        cur = conn.execute(
            """
            INSERT INTO users(username, password_hash, role, is_active, created_at, updated_at)
            VALUES(?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            (username, password_hash, role, 1 if is_active else 0),
        )
        return cur.lastrowid


def update_user_password(username: str, new_password_hash: str) -> None:
    with get_connection() as conn:
        conn.execute(
            "UPDATE users SET password_hash = ?, updated_at = ? WHERE username = ?",
            (new_password_hash, iso_now(), username),
        )


def set_user_active(username: str, is_active: bool) -> None:
    with get_connection() as conn:
        conn.execute(
            "UPDATE users SET is_active = ?, updated_at = ? WHERE username = ?",
            (1 if is_active else 0, iso_now(), username),
        )


def set_user_role(username: str, role: str) -> None:
    with get_connection() as conn:
        conn.execute(
            "UPDATE users SET role = ?, updated_at = ? WHERE username = ?",
            (role, iso_now(), username),
        )


def _parse_iso_ts(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _parse_schedule_interval(schedule: str) -> Optional[timedelta]:
    if not schedule:
        return None
    s = schedule.lower()
    if s.startswith("every_"):
        remainder = s[len("every_") :]
        number_part = ""
        unit_part = ""
        for ch in remainder:
            if ch.isdigit():
                number_part += ch
            else:
                unit_part = remainder[len(number_part) :].strip("_")
                break
        value = int(number_part) if number_part else 1
        unit_part = unit_part or "minutes"
        if unit_part in {"m", "min", "mins", "minute", "minutes"}:
            return timedelta(minutes=value)
        if unit_part in {"h", "hour", "hours"}:
            return timedelta(hours=value)
        if unit_part in {"d", "day", "days"}:
            return timedelta(days=value)
        return timedelta(minutes=value)
    if s == "hourly":
        return timedelta(hours=1)
    if s == "daily":
        return timedelta(days=1)
    if s.startswith("every") and s.endswith("minutes"):
        try:
            value = int(s.split("_")[1])
        except (IndexError, ValueError):
            value = 5
        return timedelta(minutes=value)
    return None


def _parse_daily_time(schedule: str) -> Optional[datetime_time]:
    if schedule.startswith("daily_"):
        _, _, time_part = schedule.partition("_")
        try:
            hour_str, minute_str = time_part.split(":")
            return datetime_time(int(hour_str), int(minute_str))
        except ValueError:
            return datetime_time(2, 0)
    return None


def _job_is_due(job: sqlite3.Row, now: Optional[datetime] = None) -> bool:
    if not job["enabled"]:
        return False
    schedule = job["schedule"]
    if not schedule:
        return False
    schedule = schedule.strip()
    if not schedule:
        return False
    now = now or utcnow()
    last_run = _parse_iso_ts(job["last_run_at"])
    lower = schedule.lower()
    jitter = False
    if lower.endswith("_jitter"):
        jitter = True
        lower = lower[: -len("_jitter")]
    if lower.startswith("daily_"):
        target_time = _parse_daily_time(lower) or datetime_time(2, 0)
        if last_run is None:
            return now.time() >= target_time
        if now.date() > last_run.date() and now.time() >= target_time:
            return True
        if (now - last_run) >= timedelta(days=2):
            return True
        return False
    if lower == "daily":
        interval = timedelta(days=1)
    else:
        interval = _parse_schedule_interval(lower)
    if interval is None:
        return False
    if last_run is None:
        return True
    if jitter:
        base_seconds = max(interval.total_seconds(), 60.0)
        variation = max(base_seconds * 0.1, 30.0)
        adjusted = base_seconds + random.uniform(-variation, variation)
        interval = timedelta(seconds=max(60.0, adjusted))
    return now - last_run >= interval


def next_due_jobs(now: Optional[datetime] = None) -> List[sqlite3.Row]:
    now = now or utcnow()
    due: List[sqlite3.Row] = []
    lock_window = timedelta(minutes=5)
    for job in list_jobs(only_enabled=True):
        run_started_raw = job["run_started_at"] if "run_started_at" in job.keys() else None
        if run_started_raw and not job["last_status"]:
            started = _parse_iso_ts(run_started_raw)
            if started and (now - started) < lock_window:
                continue
        if _job_is_due(job, now):
            due.append(job)
    return due


def insert_manual_match(doc_id: str, tx_id: str, tenant: str, matched_amount: float, status: str = "manual") -> None:
    match_id = f"{doc_id}::{tx_id}::{int(datetime.now().timestamp() * 1000)}"
    with get_connection() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO matches(match_id, tenant, doc_id, tx_id, matched_amount, score, strategy, status, created_at, confirmed_at)
            VALUES(?, ?, ?, ?, ?, 1.0, 'manual_override', ?, ?, ?)
            """,
            (match_id, tenant, doc_id, tx_id, float(matched_amount), status, iso_now(), iso_now()),
        )
        _update_tx_match_flag(conn, tx_id)
        _recalc_doc_reconciliation_conn(conn, doc_id)


def fetch_review_queue(
    limit: Optional[int] = None,
    offset: int = 0,
    tenant: Optional[str] = None,
) -> List[sqlite3.Row]:
    query = "SELECT * FROM review_queue"
    params: List[Any] = []
    if tenant:
        query += " WHERE tenant = ?"
        params.append(tenant)
    query += " ORDER BY created_at"
    if limit:
        query += " LIMIT ? OFFSET ?"
        params.extend([int(limit), int(offset)])
    with get_connection() as conn:
        cur = conn.execute(query, params)
        return cur.fetchall()

def add_audit(doc_id: str, step: str, who: str, before: Optional[Dict[str, Any]], after: Optional[Dict[str, Any]]) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO audit(doc_id, step, who, before, after, ts) VALUES(?, ?, ?, ?, ?, ?)
            """,
            (
                doc_id,
                step,
                who,
                json.dumps(before, ensure_ascii=False) if before else None,
                json.dumps(after, ensure_ascii=False) if after else None,
                iso_now(),
            ),
        )

def upsert_dedupe(doc_id: str, tenant: str, supplier_nif: str, inv_number: str, inv_date: str, gross: Any) -> None:
    iso_date = normalize_date(inv_date) or today_iso()
    gross_amount = quantize_amount(gross)
    with get_connection() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO dedupe(doc_id, tenant, supplier_nif, inv_number, inv_date, gross)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (doc_id, tenant, supplier_nif, inv_number, iso_date, float(gross_amount)),
        )


def find_duplicates(
    tenant: str, supplier_nif: str, inv_number: str, gross: Any, lookback_days: int = 180
) -> List[sqlite3.Row]:
    if not supplier_nif:
        return []
    gross_amount = quantize_amount(gross)
    cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()
    query = """
        SELECT * FROM dedupe
        WHERE tenant = ?
          AND supplier_nif = ?
          AND inv_date >= ?
          AND (
            (inv_number IS NOT NULL AND inv_number != '' AND inv_number = ?)
            OR (ABS(gross - ?) <= 0.01)
          )
    """
    with get_connection() as conn:
        cur = conn.execute(
            query,
            (tenant, supplier_nif, cutoff, inv_number or "", float(gross_amount)),
        )
        return cur.fetchall()

def get_doc(doc_id: str) -> Optional[sqlite3.Row]:
    with get_connection() as conn:
        cur = conn.execute("SELECT * FROM docs WHERE doc_id = ?", (doc_id,))
        return cur.fetchone()

def list_docs_by_status(status: str) -> List[sqlite3.Row]:
    with get_connection() as conn:
        cur = conn.execute(
            "SELECT * FROM docs WHERE status = ? ORDER BY created_at DESC", (status,)
        )
        return cur.fetchall()

def store_error(doc_id: str, message: str) -> None:
    with get_connection() as conn:
        conn.execute(
            "UPDATE docs SET status = 'ERROR', error = ?, updated_at = ? WHERE doc_id = ?",
            (message[:500], iso_now(), doc_id),
        )

def read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)

def csv_exists(doc_id: str) -> bool:
    target = BASE_DIR / "OUT" / "csv" / f"{doc_id}.csv"
    return target.exists()

def load_vendor_rules(csv_path: Path) -> List[Dict[str, Any]]:
    import pandas as pd

    if not csv_path.exists():
        return []
    df = pd.read_csv(csv_path)
    return df.fillna("").to_dict(orient="records")


def append_vendor_rule(csv_path: Path, row: Dict[str, Any]) -> None:
    header_needed = not csv_path.exists()
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    import csv

    with csv_path.open("a", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh, fieldnames=["tenant", "supplier_name", "nif", "account", "iva_type", "notes"]
        )
        if header_needed:
            writer.writeheader()
        writer.writerow(row)


def log_llm_call(
    task: str,
    provider: str,
    model: str,
    prompt_tokens: int,
    completion_tokens: int,
    latency_ms: float,
    error: Optional[str] = None,
    tenant: Optional[str] = None,
    username: Optional[str] = None,
) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO llm_calls(task, provider, model, prompt_tokens, completion_tokens, latency_ms, tenant, username, error)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task,
                provider,
                model,
                prompt_tokens,
                completion_tokens,
                latency_ms,
                tenant,
                username,
                error[:500] if error else None,
            ),
        )


def record_login_attempt(username: str, ip: str, success: bool) -> None:
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO login_attempts(username, ip, success, created_at)
            VALUES(?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (username, ip, 1 if success else 0),
        )


def failed_attempts_since(username: str, minutes: int) -> int:
    window = (utcnow() - timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")
    with get_connection() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) FROM login_attempts
            WHERE username = ?
              AND success = 0
              AND created_at >= ?
            """,
            (username, window),
        ).fetchone()
    return row[0] if row else 0


def check_llm_quota(tenant: Optional[str], username: Optional[str]) -> Optional[str]:
    """Return an error message if the tenant/user exceeded their LLM daily quota."""
    window = (utcnow() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    with get_connection() as conn:
        if tenant and settings.llm_max_calls_tenant_daily:
            row = conn.execute(
                """
                SELECT COUNT(*) FROM llm_calls
                WHERE tenant = ?
                  AND created_at >= ?
                """,
                (tenant, window),
            ).fetchone()
            if row and row[0] >= settings.llm_max_calls_tenant_daily:
                return f"Cuota diaria de LLM alcanzada para el tenant {tenant}"
        if username and settings.llm_max_calls_user_daily:
            row = conn.execute(
                """
                SELECT COUNT(*) FROM llm_calls
                WHERE username = ?
                  AND created_at >= ?
                """,
                (username, window),
            ).fetchone()
            if row and row[0] >= settings.llm_max_calls_user_daily:
                return f"Cuota diaria de LLM alcanzada para el usuario {username}"
    return None
