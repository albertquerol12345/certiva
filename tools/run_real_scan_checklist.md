Objetivo: validar con escáner barato y A3 real, detectando problemas antes de demos.

Checklist rápido (ejecutar en orden):

1) Preparar lote físico
- Reúne ~50 docs variados: facturas A4 limpias, tickets arrugados, papel térmico borroso, algún ticket manchado.
- Incluye 1-2 facturas con IRPF/suplidos si tienes.
- Ordena en el ADF y comprueba manualmente nº de páginas (para detectar doble alimentación).

2) Captura con escáner barato
- Escanea a `IN/real_test/` y anota: páginas esperadas vs escaneadas. Si falta alguna, registra marca/modelo y síntoma.
- Si el escáner no cuenta páginas, usa `pdfinfo archivo.pdf` para verificar páginas por fichero.

3) Procesar con pipeline
```bash
.venv/bin/python -m src.watcher --path IN/real_test --tenant demo --recursive --batch-timeout 30 --stabilize-seconds 2
```
- Revisa `OUT/` del lote: `RESUMEN.txt`, incidencias, `a3_errors.txt`, debug LLM si hay.

4) Verificar A3 / errores
- Si tienes log de importación A3, parsea:
```bash
python -m tools.parse_a3_errors --file OUT/.../a3_errors.txt --format txt
```
- Ajusta `config/tenants.json` (allowed_accounts/diaries) según errores recurrentes.

5) Páginas y doble alimentación
- Comprueba `RESUMEN.txt` y dashboard: batch warnings de `BATCH_MISSING_PAGES`/`BATCH_ZERO_PAGES`.
- Si faltan páginas, revisa físicamente el taco: ¿doble alimentación? ¿tirones del ADF? anota % de fallos.

6) OCR/LLM calidad
- Muestra 5-10 JSON en `OUT/json/` de casos problemáticos (tickets borrosos).
- Si OCR falla, activa modelo premium solo en dudas (LLM_STRATEGY=dual_cascade) para ese lote.

7) Conciliación y coste
- Si hay extracto bancario, importa CSV y ejecuta conciliación: `python -m src.bank_matcher match --tenant demo`.
- Revisa `/metrics` o Grafana: cola HITL, batch warnings, coste LLM diario.

8) Alertas
- Configura `ALERT_WEBHOOK_URL` (slack/teams/raw) y ejecuta: `python -m tools.alert_runner`.
- Programa jobs en cron/systemd: `run_alerts` (cada hora) y `backfill_llm_costs` (diario).

Resultado esperado:
- Lista de incidencias reales (OCR, A3, doble alimentación).
- Ajustes propuestos en `tenants.json` y thresholds.
- Métricas de coste y cola HITL del lote real.
