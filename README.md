# CERTIVA MVP

![Python](https://img.shields.io/badge/python-3.11+-blue.svg)
![Tests](https://img.shields.io/badge/tests-pytest-green.svg)
![Docker](https://img.shields.io/badge/docker-ready-blue.svg)

**Automate invoice processing: PDF → OCR → Rules → CSV (a3innuva-compatible)**

Local pipeline with human-in-the-loop review, SQLite audit trail, and production resilience patterns (retries, circuit breakers, PII scrubbing).

![Demo Preview](assets/preview.gif)

---

## ⚡ Quick Start (3 commands)

```bash
git clone https://github.com/albertquerol12345/certiva.git
cd certiva_mvp
python -m src.demo --reset
```

This processes 34 demo invoices and shows metrics. No API keys needed (uses dummy OCR).

---

## 📹 Demo Video

🎬 [Watch 30s demo](assets/demo.mp4)

---

## 🏗️ Architecture

```
┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐
│  PDF    │ →  │   OCR   │ →  │  Rules  │ →  │  HITL   │ →  │   CSV   │
│  Input  │    │ (Azure) │    │ Engine  │    │ Review  │    │  Export │
└─────────┘    └─────────┘    └─────────┘    └─────────┘    └─────────┘
     ↓              ↓              ↓              ↓              ↓
  IN/ folder    Confidence   Auto-post if   Web UI      a3innuva
                scoring      > threshold    Queue       compatible
```

**Key Features:**
- ✅ **Resumable processing** — SHA-256 deduplication
- ✅ **Human-in-the-loop** — Review queue for low-confidence docs
- ✅ **Audit trail** — SQLite logs every action (HITL, exports, errors)
- ✅ **Resilience** — Circuit breakers, exponential backoff, PII scrubbing
- ✅ **Multi-tenant** — Per-client configs and isolation

---

## 📊 Demo Scale Metrics

| Metric | Value |
|--------|-------|
| Demo invoices | 34 (24 AP, 10 AR) |
| Processing time | ~2 min for full batch |
| Auto-post rate | ~70% (clean) / ~60% (dirty) |
| Output | CSV + JSON + SQLite audit |

---

## 🚀 Use Cases

- **Small businesses** — Automate AP/AR entry without expensive ERP modules
- **Accountants** — Reduce manual invoice coding time
- **Developers** — Example of production-grade Python pipeline patterns

---

## 📚 Documentation

- [Full Technical Docs](README_FULL.md) — Detailed setup, Docker, API keys, troubleshooting
- [DEMO.md](DEMO.md) — Step-by-step demo with screenshots
- [tests/golden/](tests/golden/) — 34 demo invoices (clean + dirty variants)

---

## 🛠️ Tech Stack

**Core:** Python 3.11+ · Pandas · Pydantic · SQLite  
**OCR:** Azure Form Recognizer (prebuilt-invoice) / Dummy fallback  
**Web:** FastAPI · Jinja2 · bcrypt auth  
**Ops:** Docker · Prometheus/Grafana (optional) · pytest

---

## ⚠️ Disclaimer

This is a **demo-scale MVP**. It processes ~34 invoices reliably but is not production-ready for high-volume use without additional hardening (see [README_FULL.md](README_FULL.md)).
