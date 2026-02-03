# CERTIVA — Demo Guide

See the pipeline in action with 34 demo invoices.

---

## 🎯 What You'll See

**Before:** 34 PDF invoices scattered in a folder  
**After:** Structured CSV ready for import + audit trail of every decision

---

## 🚀 Run the Demo

```bash
# 1. Clone and enter
git clone https://github.com/albertquerol12345/certiva.git
cd certiva_mvp

# 2. Run demo (no setup needed)
python -m src.demo --reset
```

**Output:**
```
=== CERTIVA Demo ===
Processing 34 invoices from tests/golden/
✓ 24 invoices auto-posted (70.6%)
⚠ 7 invoices flagged for review (HITL queue)
✗ 3 invoices with errors (see incidencias.csv)

Output written to: OUT/demo/demo_20250203_143022/
├── a3_asientos.csv      # Import-ready for a3innuva
├── incidencias.csv      # Documents needing attention
├── RESUMEN.txt          # Full metrics report
└── logs/                # Detailed processing logs
```

---

## 📊 Sample Output

### CSV Export (a3innuva-ready)
```csv
Fecha,Diario,Documento,Cuenta,Debe,Haber,Concepto,NIF
2025-01-15,COMPRAS,INV-001,628000,120.00,0.00,Suministros,A12345678
2025-01-15,COMPRAS,INV-001,472000,25.20,0.00,IVA Soportado,
2025-01-15,COMPRAS,INV-001,410000,0.00,145.20,Proveedores,A12345678
```

### Metrics Report (RESUMEN.txt excerpt)
```
Documentos totales: 34
Publicado: 24
Auto-post: 70.6%
Tiempo total medio: 1.8 min
P50 OCR: 1.2s | P90 OCR: 3.1s
Duplicados detectados: 2
Reglas aprendidas: 3
```

---

## 🖼️ Visual Walkthrough

| Step | Input | Output |
|------|-------|--------|
| 1 | PDF Invoice | Raw text extraction |
| 2 | Raw data | Normalized JSON + confidence scores |
| 3 | Rules engine | Proposed accounting entry |
| 4 | Confidence check | Auto-post OR HITL queue |
| 5 | Final export | CSV + audit trail |

---

## 🔍 Explore the HITL Queue

```bash
# View pending reviews
python -m src.hitl_cli list

# Interactive review (atajos: A=Accept, E=Edit, D=Duplicate, S=Skip)
python -m src.hitl_cli review
```

Or launch the web UI:
```bash
uvicorn src.webapp:app --reload
# Open http://localhost:8000/review
```

---

## 📁 Demo Dataset

Location: `tests/golden/`

| Category | Count | Description |
|----------|-------|-------------|
| Suministros | 6 | Utility bills (electricity, water, internet) |
| Alquiler | 3 | Office rent invoices |
| Software | 5 | SaaS subscriptions |
| Intracom | 4 | EU intra-community invoices |
| Abonos | 2 | Credit notes |
| Ventas | 10 | Sales invoices (AR flow) |

**Dirty variants:** `tests/golden_dirty/` — Same invoices with blur, rotation, compression artifacts to test OCR resilience.

---

## 🎓 Learning Path

1. **Basic demo** (this file) — See the pipeline work
2. **With HITL** — `python -m src.demo --reset --hitl` — Review one document interactively
3. **Golden set test** — `python -m tests.run_golden` — Compare clean vs dirty performance
4. **Custom invoices** — Drop your own PDFs in `IN/demo/` and run `python -m src.watcher --path IN/demo`

---

## 💡 Key Takeaways

- ✅ **Idempotent** — Same PDF produces same output (SHA-256 hashing)
- ✅ **Resumable** — Stop and restart without losing progress
- ✅ **Observable** — Every decision logged in SQLite
- ✅ **Safe** — PII scrubbed from logs, confidence thresholds prevent auto-posting risky docs
