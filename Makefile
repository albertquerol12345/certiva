PYTHON ?= python3
PIP ?= pip
UVICORN ?= uvicorn

.PHONY: help install lint fmt test web watcher metrics-up metrics-down

help:
	@echo "make install        # instala deps + deps dev"
	@echo "make lint           # ruff + black --check"
	@echo "make fmt            # black formatea"
	@echo "make test           # pytest"
	@echo "make web            # lanza webapp en localhost:8000"
	@echo "make watcher        # watcher sobre IN/demo (tenant demo)"
	@echo "make metrics-up     # levanta Prometheus/Grafana"
	@echo "make metrics-down   # para Prometheus/Grafana"

install:
	$(PIP) install -r requirements.txt -r requirements-dev.txt

lint:
	ruff check .
	black --check .

fmt:
	black .

test:
	$(PYTHON) -m pytest

web:
	$(UVICORN) src.webapp:app --reload --host 0.0.0.0 --port 8000

watcher:
	$(PYTHON) -m src.watcher --path IN/demo --tenant demo --recursive --batch-timeout 60 --stabilize-seconds 2

metrics-up:
	docker-compose -f docker-compose.metrics.yml up -d

metrics-down:
	docker-compose -f docker-compose.metrics.yml down
