"""Validation helpers for A3 CSV output."""
from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
import re
from typing import List, Tuple

from .exporter import A3_CSV_COLUMNS
from .config import get_tenant_config

Error = Tuple[int, str, str]


def _is_float(value: str) -> bool:
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False


def _validate_row(row: List[str], line_no: int, tenant_cfg=None) -> List[Error]:
    errors: List[Error] = []
    tenant_cfg = tenant_cfg or {}
    allowed_accounts = set(tenant_cfg.get("allowed_accounts") or [])
    allowed_diaries = set(tenant_cfg.get("allowed_diaries") or [])
    if len(row) != len(A3_CSV_COLUMNS):
        errors.append((line_no, "*", f"Número de columnas inválido ({len(row)})"))
        return errors
    fecha, diario, documento, cuenta, debe, haber, concepto, nif = row
    try:
        datetime.fromisoformat(fecha)
    except ValueError:
        errors.append((line_no, "Fecha", "Formato ISO inválido"))
    if not diario:
        errors.append((line_no, "Diario", "Vacío"))
    elif allowed_diaries and diario not in allowed_diaries:
        errors.append((line_no, "Diario", "Diario no permitido"))
    if not documento:
        errors.append((line_no, "Documento", "Vacío"))
    if not cuenta or not cuenta.isdigit():
        errors.append((line_no, "Cuenta", "Debe ser numérica"))
    elif allowed_accounts and cuenta not in allowed_accounts:
        errors.append((line_no, "Cuenta", "Cuenta no permitida para este tenant"))
    decimal_pattern = re.compile(r"^-?\d+(?:\.\d{1,2})?$")
    if "," in debe:
        errors.append((line_no, "Debe", "Separador decimal debe ser '.'"))
    if not _is_float(debe) or not decimal_pattern.match(debe):
        errors.append((line_no, "Debe", "Valor no numérico o >2 decimales"))
    if "," in haber:
        errors.append((line_no, "Haber", "Separador decimal debe ser '.'"))
    if not _is_float(haber) or not decimal_pattern.match(haber):
        errors.append((line_no, "Haber", "Valor no numérico o >2 decimales"))
    if not concepto:
        errors.append((line_no, "Concepto", "Vacío"))
    if nif:
        if len(nif) < 6 or len(nif) > 15:
            errors.append((line_no, "NIF", "Longitud sospechosa"))
    return errors


def validate_a3_csv(csv_path: Path, tenant: str = "default") -> List[Error]:
    errors: List[Error] = []
    if not csv_path.exists():
        return errors
    tenant_cfg = get_tenant_config(tenant)
    with csv_path.open("r", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration:
            return [(1, "*", "Archivo vacío")]
        if header != A3_CSV_COLUMNS:
            errors.append((1, "Cabecera", "Cabecera A3 inválida"))
        for idx, row in enumerate(reader, start=2):
            errors.extend(_validate_row(row, idx, tenant_cfg))
    return errors


def parse_a3_error_log(text: str) -> List[Error]:
    """
    Intenta parsear mensajes típicos de A3 como:
    - "Linea 3: Campo Cuenta no existe"
    - "Línea 5 -> Diario inválido"
    Devuelve lista de tuplas (linea, campo, mensaje).
    """
    errors: List[Error] = []
    pattern = re.compile(r"[Ll][ií]nea\s+(\d+)\s*[:\-–>]\s*(.*)", re.IGNORECASE)
    for raw_line in text.splitlines():
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        match = pattern.search(raw_line)
        if match:
            line_no = int(match.group(1))
            rest = match.group(2).strip()
            # Separar campo + mensaje si es posible
            parts = rest.split(None, 1)
            if len(parts) == 2:
                field, msg = parts
            else:
                field, msg = "*", rest
            errors.append((line_no, field.strip(" :"), msg.strip()))
        else:
            errors.append((0, "*", raw_line))
    return errors
