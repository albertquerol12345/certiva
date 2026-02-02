"""
Ejecuta alertas y envía webhook si procede. Útil para cron/Alertmanager.
"""
from __future__ import annotations

from src import alerts, utils


def main() -> None:
    utils.configure_logging()
    found = alerts.evaluate_alerts()
    if not found:
        print("Sin alertas.")
        return
    alerts.send_alerts(found)
    print("Alertas registradas/enviadas:")
    for msg in found:
        print(f"- {msg}")


if __name__ == "__main__":
    main()
