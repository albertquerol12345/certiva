# CERTIVA — Invoice Automation (HITL)

Pipeline local para normalizar facturas, aplicar reglas deterministas, pedir revisión humana cuando corresponde y exportar asientos compatibles con a3innuva.

![Certiva preview](assets/preview.gif)

---

## ✅ Qué hace

- **Ingesta de PDFs** → OCR (Azure o dummy)
- **Normalización + reglas** → propuesta de asiento
- **HITL** → cola de revisión (CLI y web)
- **Export CSV** → compatible con a3innuva
- **Estado en SQLite** + logs

---

## 📦 Dataset demo

- `tests/golden/` → 34 PDFs de demo
- `tests/golden_dirty/` → variantes degradadas para pruebas de OCR

---

## ⚡ Quick Start (demo local)

```bash
git clone https://github.com/albertquerol12345/certiva.git
cd certiva
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Opcional: sin claves externas
cp .env.sample .env
# En .env: OCR_PROVIDER=dummy

python -m src.demo --reset
```

Salida:
- `OUT/json/`, `OUT/csv/`, `OUT/RESUMEN.txt`
- `db/docs.sqlite`

---

## 🧑‍⚖️ Revisión humana (HITL)

```bash
python -m src.hitl_cli list
python -m src.hitl_cli review
```

Web UI:
```bash
uvicorn src.webapp:app --reload
# http://localhost:8000/review
```

---

## 📚 Documentación

- [DEMO.md](DEMO.md) — guía rápida
- [README_FULL.md](README_FULL.md) — documentación técnica completa

---

## 🛠️ Stack

Python · pandas · SQLite · FastAPI · OCR (Azure/dummy)
