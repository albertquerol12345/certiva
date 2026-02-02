# Certiva (Portfolio Copy)

![Certiva preview](assets/preview.png)

Local pipeline to normalize invoices, apply deterministic rules, add human-in-the-loop review when needed, and export journal entries compatible with a3innuva.

## Why it matters
- Cuts manual accounting work by auto-normalizing and pre-validating documents
- Keeps full audit trail and confidence scoring for safe review
- Designed for real ops: throttling, retries, circuit breakers, and metrics

## What is included here
- Core Python pipeline (OCR -> normalization -> rules -> export)
- HITL web UI for review and corrections
- Tests and synthetic samples
- Metrics stack config (Prometheus/Grafana)

Sensitive data and keys are removed from this portfolio copy.

## Quick start (dummy OCR)
```bash
cd CERTIVA
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Process a sample folder (dummy OCR)
python -m src.watcher --path IN/demo --tenant demo --recursive
```

## Demo (headless, synthetic invoices)
```bash
python -m src.launcher --headless process-folder --path tests/golden --tenant demo --force-dummy
```
Outputs are written to `OUT/demo/<batch>/` with `RESUMEN.txt` and `incidencias.csv`.

**Expected output (sample HITL queue):**
![Certiva HITL queue](assets/preview.png)

## Web UI (HITL review)
```bash
.venv/bin/uvicorn src.webapp:app --reload
```
Open http://localhost:8000

## Real OCR (optional)
1) Copy `.env.sample` to `.env`
2) Set Azure Form Recognizer endpoint/key
3) Rerun the watcher

## Tests
```bash
.venv/bin/pytest
```

## Repo structure (simplified)
- `src/` pipeline, rules, OCR providers, export
- `tests/` synthetic samples + unit tests
- `config/` dashboards and policies
- `IN/` input drop folder (empty here)
- `OUT/` outputs (empty here)

## Notes
- Full technical README is in `README_FULL.md`.
- This is a portfolio-safe snapshot; no client data or secrets.
