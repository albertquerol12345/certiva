# CERTIVA — Demo Guide

Pipeline local para procesar facturas de demo y generar CSV compatible con a3innuva.

---

## ✅ Dataset incluido

- `tests/golden/` → 34 PDFs de demo (limpios)
- `tests/golden_dirty/` → variantes degradadas (ruido/blur)

---

## 🚀 Ejecutar demo

```bash
git clone https://github.com/albertquerol12345/certiva.git
cd certiva
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Opcional: usar OCR dummy para no necesitar claves
cp .env.sample .env
# En .env: OCR_PROVIDER=dummy

python -m src.demo --reset
```

**Salida (variable según configuración):**
- `OUT/json/`
- `OUT/csv/`
- `OUT/RESUMEN.txt`
- `db/docs.sqlite`

---

## 🧑‍⚖️ HITL (revisión humana)

CLI:
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

## 📌 Notas

- Las métricas exactas dependen del OCR/LLM configurado y de los umbrales.
- El demo está pensado para validar flujo, no para medir rendimiento final.
