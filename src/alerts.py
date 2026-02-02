from __future__ import annotations

import logging
from typing import List, Optional

import httpx

from . import metrics, utils, provider_health
from .config import settings

logger = logging.getLogger(__name__)


def evaluate_alerts(tenant: Optional[str] = None) -> List[str]:
    """Devuelve lista de alertas activas según umbrales de settings."""
    alerts: List[str] = []
    stats = metrics.gather_stats(tenant=tenant)
    preflight = metrics.gather_preflight(tenant=tenant)
    queue_size = len(utils.fetch_review_queue(tenant=tenant))
    if queue_size > settings.alert_review_queue_threshold:
        alerts.append(f"HITL pendiente: {queue_size} documentos (umbral {settings.alert_review_queue_threshold})")
    batch_warnings = stats.get("batch_warnings") or {}
    if settings.alert_batch_warning and sum(batch_warnings.values()) > 0:
        alerts.append(f"Batch warnings detectados: {dict(batch_warnings)}")
    pages = stats.get("pages") or {}
    if pages.get("zero_page_docs", 0) >= settings.alert_zero_page_threshold:
        alerts.append(f"Documentos con 0 páginas: {pages.get('zero_page_docs', 0)}")
    issue_counts = preflight.get("issue_counts") or {}
    if issue_counts:
        top_issue, count = issue_counts.most_common(1)[0]
        if count > 0:
            alerts.append(f"Issue más frecuente: {top_issue} ({count})")
    llm = stats.get("llm_stats") or {}
    daily_cost = llm.get("cost_today_eur", 0.0)
    if daily_cost and daily_cost > settings.llm_cost_alert_daily_eur:
        alerts.append(f"Coste LLM diario {daily_cost:.2f}€ supera umbral {settings.llm_cost_alert_daily_eur}€")
    prov = provider_health.snapshot()
    degraded = [name for name, state in prov.items() if state.get("degraded")]
    if degraded:
        alerts.append(f"Proveedores degradados: {', '.join(str(name) for name in degraded)}")
    return alerts


def send_alerts(alerts: List[str], tenant: Optional[str] = None) -> bool:
    """Envía alertas a webhook si está configurado; siempre loggea warning."""
    if not alerts:
        return False
    prefix = f"[tenant={tenant}]" if tenant else "[tenant=all]"
    lines_payload = "\n".join(f"• {a}" for a in alerts)
    message = f"{prefix}\n{lines_payload}"
    logger.warning("ALERTAS CERTIVA\n%s", message)
    webhook = settings.alert_webhook_url
    if webhook:
        try:
            payload: Dict[str, Any]
            fmt = (settings.alert_webhook_format or "slack").lower()
            if fmt == "teams":
                payload = {"text": message}
            elif fmt == "raw":
                payload = {"message": message}
            else:  # slack
                payload = {"text": message}
            httpx.post(webhook, json=payload, timeout=5.0)
        except Exception as exc:  # pragma: no cover
            logger.error("No se pudo enviar webhook de alertas: %s", exc)
            return False
    return True


def main() -> None:
    utils.configure_logging()
    alerts_found = evaluate_alerts()
    if not alerts_found:
        print("Sin alertas.")
        return
    send_alerts(alerts_found)
    print("Alertas enviadas/registradas:")
    for msg in alerts_found:
        print(f"- {msg}")


if __name__ == "__main__":
    main()
