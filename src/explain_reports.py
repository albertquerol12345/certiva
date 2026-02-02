"""Explicaciones en lenguaje natural para P&L, IVA y Cashflow."""
from __future__ import annotations

import logging
from typing import Dict, Optional

from . import llm_router

logger = logging.getLogger(__name__)


def _fmt(value: float) -> str:
    return f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _serialize_pnl(report: Dict) -> str:
    groups = report.get("groups", {})
    lines = [
        f"Ingresos totales: {_fmt(report.get('total_income', 0.0))}",
        f"Gastos totales: {_fmt(report.get('total_expense', 0.0))}",
        f"Resultado: {_fmt(report.get('result', 0.0))}",
        "",
        "Detalle por grupo:",
    ]
    for group, amount in sorted(groups.items()):
        lines.append(f"- {group}: {_fmt(amount)}")
    return "\n".join(lines)


def _serialize_vat(report: Dict) -> str:
    soportado = report.get("soportado", {})
    repercutido = report.get("repercutido", {})
    total_soportado = sum(bucket["vat"] for bucket in soportado.values())
    total_repercutido = sum(bucket["vat"] for bucket in repercutido.values())
    lines = [
        f"IVA soportado total: {_fmt(total_soportado)}",
        f"IVA repercutido total: {_fmt(total_repercutido)}",
        f"Saldo (repercutido - soportado): {_fmt(total_repercutido - total_soportado)}",
        "",
        "Desglose soportado:",
    ]
    for key, bucket in sorted(soportado.items()):
        lines.append(f"- {key}: base {_fmt(bucket['base'])}, IVA {_fmt(bucket['vat'])}")
    lines.append("Desglose repercutido:")
    for key, bucket in sorted(repercutido.items()):
        lines.append(f"- {key}: base {_fmt(bucket['base'])}, IVA {_fmt(bucket['vat'])}")
    return "\n".join(lines)


def _serialize_cashflow(report: Dict) -> str:
    buckets = report.get("buckets", [])
    lines = []
    for bucket in buckets:
        lines.append(
            f"{bucket['label']}: cobros {_fmt(bucket['in'])}, pagos {_fmt(bucket['out'])}, neto {_fmt(bucket['net'])}"
        )
    return "\n".join(lines)


def explain_pnl(report: Dict, tenant: Optional[str] = None, user: Optional[str] = None) -> str:
    context = _serialize_pnl(report)
    system_prompt = (
        "Eres un analista financiero que prepara resúmenes para pymes. "
        "Debes resaltar insights accionables (márgenes, crecimiento, gastos anómalos) y usar un tono cercano."
    )
    user_prompt = "Redacta un análisis breve (3-5 párrafos) del P&L resumido en el contexto."
    return llm_router.call_llm(
        llm_router.LLMTask.EXPLICAR_PNL,
        system_prompt,
        user_prompt,
        context=context,
        tenant=tenant,
        user=user,
    )


def explain_vat(report: Dict, tenant: Optional[str] = None, user: Optional[str] = None) -> str:
    context = _serialize_vat(report)
    system_prompt = (
        "Eres un asesor fiscal especializado en IVA español. "
        "Explica si el resultado es a pagar o a compensar y qué factores lo causan."
    )
    user_prompt = "Describe esta liquidación de IVA para que la entienda un gerente, incluyendo recomendaciones."
    return llm_router.call_llm(
        llm_router.LLMTask.EXPLICAR_IVA,
        system_prompt,
        user_prompt,
        context=context,
        tenant=tenant,
        user=user,
    )


def explain_cashflow(report: Dict, tenant: Optional[str] = None, user: Optional[str] = None) -> str:
    context = _serialize_cashflow(report)
    system_prompt = (
        "Eres un controller financiero. Identifica tensiones de caja, periodos con falta de cobros y necesidad de financiación."
    )
    user_prompt = "Analiza el forecast de cashflow y sugiere acciones para estabilizar la caja."
    return llm_router.call_llm(
        llm_router.LLMTask.EXPLICAR_CASHFLOW,
        system_prompt,
        user_prompt,
        context=context,
        tenant=tenant,
        user=user,
    )
