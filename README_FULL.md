# CERTIVA MVP

Pipeline local para normalizar facturas, aplicar reglas deterministas, pedir ayuda humana cuando haga falta y exportar asientos compatibles con a3innuva.

## Estructura principal

```
certiva_mvp/
  IN/                      # Arrastra aquí PDFs/imagenes de facturas
  OUT/
    json/                  # Normalizados + entry proposals
    csv/                   # CSV Diario a3innuva
    logs/                  # Rotación de logs
  rules/vendor_map.csv     # Mapping proveedor→cuenta/IVA
  db/docs.sqlite           # SQLite con estados y métricas
  src/                     # Código Python modular (watcher, OCR, reglas, CLI…)
  tests/                   # Golden set limpio/sucio + scripts de augment y batch
```

## Requisitos

- Python 3.11+
- `poppler-utils` para `tests/augment.py`
- Para dummy OCR → PDFs con texto embebido (los golden incluidos)
- Para OCR real → credenciales de **Azure Form Recognizer** (prebuilt-invoice).

## Instalación rápida

```bash
cd certiva_mvp
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Entorno recomendado para desarrollo y tests

- Usa el `venv` anterior para evitar restricciones del sistema (PEP 668).
- Ejecuta la web en local con:
  ```bash
  .venv/bin/uvicorn src.webapp:app --reload
  ```
- Ejecuta tests con:
  ```bash
  .venv/bin/pytest
  ```

### Observabilidad rápida (Prometheus/Grafana)

- Levanta el stack de métricas:
  ```bash
  docker-compose -f docker-compose.metrics.yml up -d
  ```
  Prometheus: http://localhost:9090 · Grafana: http://localhost:3000 (admin/admin).
- Prometheus scrapea `http://localhost:8000/metrics`. Ajusta `config/prometheus.yml` si despliegas en otra URL.
- Alertas rápidas vía webhook: configura `ALERT_WEBHOOK_URL` en `.env` y programa `python -m tools.alert_runner` en cron.
- Dashboards: se aprovisionan solos al levantar Grafana (ruta `config/grafana_dashboards/certiva_overview.json`).
- Backfill costes LLM: `python -m tools.backfill_llm_costs` o crea un job `add-backfill-llm-costs` en `jobs.py`.
 - Formato webhook: `ALERT_WEBHOOK_FORMAT=slack|teams|raw` (por defecto slack).
 - Parseo de errores A3: `python -m tools.parse_a3_errors --file a3_errors.log --format txt|json`.

### Variables de entorno (`.env`)

Copia `.env.sample` → `.env` y completa. Campos clave:

```
APP_ENV=dev                               # dev/test/prod → ajusta defaults de logging/limites
OCR_PROVIDER=azure|google|dummy           # hoy implementado azure + dummy fallback
DEFAULT_TENANT=demo
AZURE_FORMREC_ENDPOINT=...
AZURE_FORMREC_KEY=...
AZURE_MAX_RPS=0.8                       # nuevo throttle (token bucket) → RPS sostenido
AZURE_MAX_INFLIGHT=1                    # nº máximo de OCR simultáneos
AZURE_OCR_MAX_RPS=0.8                   # alias legacy (se mantiene por compatibilidad)
AZURE_OCR_MAX_CONCURRENCY=1             # alias legacy
AZURE_OCR_RETRY_TOTAL=4                 # reintentos para 429/5xx/timeouts
AZURE_OCR_RETRY_BACKOFF=1.0             # factor de escala del backoff (0.8s → 2.1s → 5s → 11s)
AZURE_OCR_RETRY_MAX_SLEEP=45            # tope de espera tras Retry-After
AZURE_OCR_READ_TIMEOUT_SEC=120          # timeout de lectura del poller
AZURE_OCR_CACHE_DIR=OUT/ocr_cache       # caché por hash SHA256
AZURE_OCR_ENABLE_CACHE=1                # 1=usar caché, 0=desactivar
MIN_CONF_OCR=0.90
MIN_CONF_ENTRY=0.85
CONFIDENCE_MIN_OK=0.80
LLM_PROVIDER_TYPE=dummy
LLM_STRATEGY=mini_only                    # o dual_cascade para cascada mini→premium
OPENAI_API_KEY=...                        # opcional para llm_suggest
OPENAI_MODEL_MINI=gpt-5.1-codex-mini
OPENAI_MODEL_PREMIUM=gpt-5.1-codex
LLM_PREMIUM_THRESHOLD_GROSS=1000
OCR_BREAKER_THRESHOLD=3                     # nº de fallos consecutivos antes de degradar OCR
LLM_BREAKER_THRESHOLD=3                     # nº de fallos consecutivos antes de degradar LLM
OPENAI_MINI_IN_PER_MTOK=0.20                # € / MTok de entrada (mini)
OPENAI_MINI_OUT_PER_MTOK=0.80               # € / MTok de salida (mini)
OPENAI_PREMIUM_IN_PER_MTOK=1.00             # € / MTok de entrada (premium)
OPENAI_PREMIUM_OUT_PER_MTOK=4.00            # € / MTok de salida (premium)
LLM_TIMEOUT_SECONDS=20
LLM_ENABLE_PII=false                      # true = se permite enviar PII al LLM
LLM_PII_SCRUB_STRICT=false                # true = scrub agresivo en prompts/logs
LLM_MAX_CALLS_TENANT_DAILY=10000          # soft-limit diario por tenant
LLM_MAX_CALLS_USER_DAILY=2000             # soft-limit diario por usuario
LLM_COST_ALERT_DAILY_EUR=50               # umbral de alerta por coste diario LLM
OUT_RETENTION_DAYS=30                     # retención de OUT/ e IN/archivado en días
ALERT_WEBHOOK_URL=
ALERT_WEBHOOK_FORMAT=slack
DEBUG_LLM=0                               # activa los JSONs redacted en OUT/debug/<doc>
LLM_DEBUG_REDACT_PII=1                    # garantiza PII scrub en el debug
WATCH_BATCH_SIZE=50                       # nº de PDFs necesarios para disparar un lote automático
WATCH_BATCH_TIMEOUT=300                   # timeout (s) para procesar aunque no haya batch_size
WATCH_GLOB=*.pdf
PIPELINE_CONCURRENCY=1                    # nº de hilos simultáneos (prod=1)
```

> **Dummy OCR** permite probar el pipeline con los PDFs generados en `tests/golden/` sin claves externas.

### Perfiles (`APP_ENV`)

- `dev`: experiencia interactiva, `DEBUG_LLM` opcional y límites altos para iterar rápido.
- `test`: pensado para `pytest`, fuerza `DEBUG_LLM=0` aunque lo olvides en `.env`.
- `prod`: desactiva debug LLM, obliga `LLM_DEBUG_REDACT_PII=1`, fija `PIPELINE_CONCURRENCY=1` y baja los límites (`LLM_MAX_CALLS_TENANT_DAILY=2000`, `LLM_MAX_CALLS_USER_DAILY=500`). La decisión de autopost + thresholds finales se gobierna por `config/tenants/<tenant>/policies.yaml`.

## Uso básico

### 1. Lanzar watcher local

```bash
source .venv/bin/activate
# Escaneo puntual
python -m src.watcher --path IN/demo --tenant demo --recursive

# Watcher ligero cada 60s
python -m src.watcher --path IN/demo --tenant demo --recursive --interval 60
```

- Calcula SHA-256 → `doc_id` (idempotente)
- Ejecuta OCR + normalización (`OUT/json/<doc_id>.json`)
- Aplica reglas (`OUT/json/<doc_id>.entry.json`)
- Auto-exporta CSV (`OUT/csv/<doc_id>.csv`) cuando supera los umbrales
- Mide timestamps por etapa (`docs` en SQLite)
- El `RESUMEN.txt` añade tablas con `LLM costes/tokens` (docs mini/premium, tokens IN/OUT y coste estimado) y `Provider health`, para auditar consumo y degradaciones sin abrir la BD.

#### Watcher batch + archivo automático

El watcher soporta lotes por tamaño (`--batch-size`) o timeout (`--batch-timeout`), espera a que los PDFs se estabilicen (`--stabilize-seconds`) y puede mover los originales a un `--archive` tras procesarlos. Ejemplo:

```bash
python -m src.watcher --path IN/demo --tenant demo --batch-size 100 --batch-timeout 180 --archive PROCESADO --recursive
```

Se crea un lock `.certiva_watcher.lock` para evitar instancias duplicadas y las rutas `WATCH_BATCH_SIZE/WATCH_BATCH_TIMEOUT/WATCH_GLOB` del `.env` sirven de valores por defecto para los flags anteriores.

### Resiliencia OCR/LLM

- Las excepciones de Azure OCR u OpenAI (429, 5xx, timeouts) se reintentan con backoff exponencial y, si se agotan, el pipeline marca el doc como `ERROR/REVIEW_PENDING` con `issues = ["OCR_TEMP_ERROR"/"LLM_TEMP_ERROR", "PROVIDER_UNAVAILABLE"]`. No hay tracebacks sin controlar y el lote final los verá en `incidencias.csv`.
- Cuando el OCR real no está disponible, `DummyOCRProvider` toma el relevo y se añade `OCR_PROVIDER_FALLBACK` para forzar revisión.
- Si un proveedor acumula más de `OCR_BREAKER_THRESHOLD`/`LLM_BREAKER_THRESHOLD` fallos consecutivos se abre un **circuit breaker** (`PROVIDER_DEGRADED`), se dejan de hacer llamadas durante el lote y los documentos pendientes quedan en incidencias. El bloque “Provider health” del `RESUMEN` muestra el estado/fallos/umbral y el tiempo medio hasta la degradación.
- `DEBUG_LLM` guarda `prompt/response/parsed.json` (en `OUT/<lote>/<doc>/debug/`) siempre con PII redactada (`LLM_DEBUG_REDACT_PII=1` por defecto).

### Estabilidad Azure OCR

- El proveedor aplica **token bucket** + **semáforo** configurables: `AZURE_MAX_RPS` (o `AZURE_OCR_MAX_RPS`) y `AZURE_MAX_INFLIGHT` (alias `AZURE_OCR_MAX_CONCURRENCY`). Si ves 429, bájalos (por ejemplo `AZURE_MAX_RPS=0.6`) y vuelve a lanzar.
- Los errores 408/429/5xx respetan `Retry-After` y, si no llega cabecera, usan un backoff fijo `0.8s → 2.1s → 5s → 11s` con jitter y tope `AZURE_OCR_RETRY_MAX_SLEEP`. Tras agotar `AZURE_OCR_RETRY_TOTAL=4`, el doc cae a `OCR_TEMP_ERROR` y se encola en `ocr_queue` para reprocesarlo con `python tools/reprocess_ocr_queue.py`.
- Activa la caché (`AZURE_OCR_ENABLE_CACHE=1`) para evitar reprocesar hashes ya vistos. El bloque “Azure OCR health” del `RESUMEN` muestra % cache hit, p50/p95 de latencia, RPS efectivo, reintentos y distribución de status.
- Antes de cada lote real (CLI headless o `tools/run_small_synthetic_experiment`) se ejecuta automáticamente `tools.azure_probe`: `python tools/azure_probe.py --path IN/lote_sintetico_grande --n 5 --rps 0.8`. Si devuelve exit code 2 o reporta 429/timeout, el lote se aborta (`PROVIDER_DEGRADED`) para evitar 0 % OK.
- Usa `python tools/azure_probe.py --path ... --n 20 --rps 0.8 --timeout 150` para calibrar un RPS sostenible y revisar `OUT/AZURE_PROBE_<ts>.txt`. `tools/run_small_synthetic_experiment` expone el mismo chequeo antes de procesar los lotes clean/dirty.

### Preflight antes de lanzar un lote real

```bash
python tools/preflight_check.py          # modo rápido (sin tocar Azure/OpenAI)
python tools/preflight_check.py --real-checks  # intenta inicializar SDKs si hay claves
```

El preflight verifica escritura en `OUT/`, acceso a SQLite y credenciales de Azure/OpenAI (indicando si estás en modo dummy). Con `--real-checks` intenta instanciar los SDKs para detectar errores de configuración antes de llamar al pipeline.

### 1.b UI web HITL

```bash
source .venv/bin/activate
uvicorn src.webapp:app --reload
```

Rutas principales:

- `/` → Dashboard (métricas + checklist pre-SII).
- `/review` → Lista de documentos `REVIEW_PENDING`.
- `/review/<doc_id>` → Detalle con acciones **Aceptar**, **Editar**, **Duplicado** y **Reprocesar** (aprendiendo reglas por NIF si procede).

En la tabla de pendientes se muestra también el `doc_type` clasificado automáticamente (factura, crédito, intracom, ticket…), lo que ayuda a priorizar la revisión.
Además puedes filtrar desde la propia web (`/review?doc_type=sales`) para centrarte sólo en ventas u otro tipo concreto.

- La cola HITL ahora tiene **paginación** (`HITL_PAGE_SIZE`), filtros por `doc_type`, botones “anterior/siguiente” y acciones masivas (aceptar, duplicar, reprocesar) con CSRF/origin enforcement.
- Cada vista muestra el **tenant activo**; los admin pueden cambiarlo con `?tenant=foo`, los operadores quedan acotados al `DEFAULT_TENANT`.

### Multi-tenant y scoping

- Los docs, la cola HITL y los logs LLM guardan el `tenant`, y todas las vistas (dashboard, review, assistant, report explainers) filtran por el tenant actual.
- Admins pueden alternar tenant con el query param `?tenant=foo` (se recuerda en la sesión); los operadores/visores solo ven `DEFAULT_TENANT`.
- Cada acción de revisión valida que el `doc_id` pertenezca al tenant en curso, evitando IDOR entre clientes.

### Autenticación y roles

La UI ya no es pública: usa login con roles (`admin`, `operator`, `viewer`). Configura `WEB_SESSION_SECRET` en `.env` (elige un valor largo y aleatorio) y crea el primer usuario:

```bash
python -m src.auth create-admin
```

Después accede a `/login`, inicia sesión y navega normalmente. Los admin disponen de accesos extra a la consola (jobs/tenants) y todas las contraseñas se guardan con bcrypt en la tabla `users` de SQLite.  
Si trabajas sin HTTPS (entorno local), puedes desactivar las cookies estrictas cambiando `WEB_SESSION_SECURE_COOKIES=0` en `.env`; en producción debe permanecer en `1`.

- El login tiene **rate limiting** configurable (`AUTH_MAX_FAILS` + `AUTH_LOCK_MINUTES`), rotación de sesión y verificación de `Origin/Referer` en cada POST.
- Nuevos comandos CLI: `python -m src.auth change-password --username ...`, `activate`, `deactivate`, además del listado existente.
- Desde la web `/admin/users` (solo admin) puedes ver todos los usuarios, cambiar roles y activar/desactivar cuentas reutilizando CSRF + controles de origen.

### Launcher en modo headless

Para automatizar pipelines sin prompts interactivos existe un modo headless:

```bash
# Procesar carpeta con los providers configurados
python -m src.launcher --headless process-folder --tenant demo --path IN/demo/lote_2025

# Ejecutar el experimento Dual LLM indicando tenant/carpeta
python -m src.launcher --headless experiment-dual-llm --tenant demo --path IN/lote_experimentos_azure_openai

# Mostrar un resumen ya generado
python -m src.launcher --headless dump-summary --lote OUT/demo/lote_2025_1101
```

La salida va a stdout (ideal para cron/CI) y respeta providers/tenants igual que el menú tradicional. En modo headless, si el OCR real es Azure se ejecuta antes `azure_probe`; si detecta 429/timeout el lote se aborta con `PROVIDER_DEGRADED` para evitar 0 % OK.

## Experimentos con facturas sintéticas y Dual LLM

- Genera un lote sintético realista ejecutando:

  ```bash
  source .venv/bin/activate
  python tests/generate_realistic_samples.py --count 200 --out-tests tests/realistic_big --out-in IN/lote_sintetico_grande --seed 123
  ```

  Esto crea 200 facturas limpias en `tests/realistic_big/` (versionadas) y en `IN/lote_sintetico_grande/` listas para el launcher. Puedes variar `--count` hasta 1000 y usar `--no-purge` si quieres mantener lotes previos.

- Para obtener la versión “foto degradada” ejecuta:

  ```bash
  python tests/augment.py --source IN/lote_sintetico_grande --dest IN/lote_sintetico_grande_dirty --seed 456
  ```

  El script aplica rotaciones, blur, brillo/contraste y compresión agresiva, generando copias `*-dirty.pdf`. Usa `--source tests/realistic_big` si prefieres degradar el set versionado o añade `--limit` para muestrear pocos ficheros.

- Configura `.env` con `OCR_PROVIDER_TYPE=azure`, `LLM_PROVIDER_TYPE=openai`, `LLM_STRATEGY=dual_cascade`, `OPENAI_MODEL_MINI=gpt-5.1-codex-mini`, `OPENAI_MODEL_PREMIUM=gpt-5.1-codex` y tus claves reales.

- Lanza el launcher y selecciona la opción **7 (Experimento Dual LLM)**:

  ```bash
  python -m src.launcher
  ```

  El experimento reprocesa `IN/lote_experimentos_azure_openai/`, genera un lote `OUT/<tenant>/<lote>/` y deja `LLM_TUNING_SUGGESTIONS.txt` junto a `a3_asientos.csv` / `incidencias.csv` / `RESUMEN.txt`. Ese fichero resume cuántos docs usaron el modelo mini vs premium y propone un nuevo `LLM_PREMIUM_THRESHOLD_GROSS`.

- Para usar tus propios PDFs (por ejemplo descargados públicamente), crea una carpeta `IN/<tenant>/<nombre_lote>/` y lanza la opción **2** del launcher. El pipeline escribirá el lote correspondiente en `OUT/<tenant>/<nombre_lote_timestamp>/` con el mismo contrato A3.

### Policies por tenant

Cada tenant puede afinar la operación creando `config/tenants/<tenant>/policies.yaml`:

```yaml
autopost_enabled: true
autopost_categories_safe:
  - suministros
  - software
llm_premium_threshold_gross: 1500
canary_sample_pct: 0.05
```

El orden de precedencia es `policies.yaml` > `.env` > defaults/`APP_ENV`. Con `autopost_enabled=false` todo documento termina en incidencias (`POLICY_AUTOREVIEW`), `autopost_categories_safe` limita los OK automáticos a categorías controladas, `canary_sample_pct` fuerza un muestreo (`CANARY_SAMPLE`) y `llm_premium_threshold_gross` sobreescribe el umbral mini→premium por tenant sin tocar `.env`.

### Experimento sintético pequeño (limpio vs dirty)

Cuando quieras un sanity check rápido sin menús interactivos ejecuta:

```bash
python tools/run_small_synthetic_experiment.py --count 50 --seed 123 --tenant demo --process-clean --process-dirty
```

El script genera (si no existen) `IN/lote_sintetico_grande/` y `IN/lote_sintetico_grande_dirty/`, corre `tools.azure_probe` antes de cada lote y, si la sonda devuelve exit 2, aborta con `PROVIDER_DEGRADED` para evitar gastar tiempo en 0 % OK. Cuando la sonda pasa, procesa ambos lotes con `process_folder_batch --skip-probe`, lee los `RESUMEN.txt` y deja un informe comparativo en `OUT/ANALISIS_COMPARATIVO_SINTETICO.txt`. Admite flags como `--generator-only` (sólo crear PDFs), `--process-clean` / `--process-dirty` (para correr un único lote) y `--out-report` para guardar el informe en otra ruta.

Ejemplo real (10 documentos clean vs dirty):

```
=== CERTIVA · Experimento Sintético Pequeño ===

Lote limpio: OUT/demo/limpio_20250116_103030
  Documentos: 10
  OK (POSTED): 9 (90.0%)
  Incidencias: 1 (10.0%)
  mini_docs: 9 (90.0%)
  premium_docs: 1 (10.0%)
  Issues frecuentes: -
  Latencias totales ms p50/p95: 640.0 / 920.0
  OCR ms p50/p95: 380.0 / 610.0
  Rules ms p50/p95: 70.0 / 100.0
  LLM ms p50/p95: 110.0 / 210.0
  Confianza global p50/p95: 0.91 / 0.95
  Threshold actual: 1000.00
  Threshold sugerido: 1450.00 (premium_ratio dentro del objetivo)

Lote dirty: OUT/demo/dirty_20250116_103525
  Documentos: 10
  OK (POSTED): 6 (60.0%)
  Incidencias: 4 (40.0%)
  mini_docs: 6 (60.0%)
  premium_docs: 4 (40.0%)
  Issues frecuentes: OCR_TEMP_ERROR=2; LINES_INCOMPLETE=1; AMOUNT_MISMATCH=1
  Latencias totales ms p50/p95: 880.0 / 1410.0
  OCR ms p50/p95: 520.0 / 900.0
  Rules ms p50/p95: 85.0 / 120.0
  LLM ms p50/p95: 150.0 / 260.0
  Confianza global p50/p95: 0.82 / 0.90
  Threshold actual: 1000.00
  Threshold sugerido: 1200.00 (premium_ratio > objetivo)

Comparativa (primer vs segundo lote):
  Δ% OK: -30.0 pp
  Δ premium_ratio: +30.0 pp
  Δ confianza p50: -0.09
  Issues limpio: -
  Issues dirty: OCR_TEMP_ERROR=2; LINES_INCOMPLETE=1; AMOUNT_MISMATCH=1
```

### Benchmark dummy reproducible

Para medir throughput local sin tocar Azure/OpenAI:

```bash
python tools/benchmark_pipeline.py --count 50 --concurrency 1 2 4 --tenant demo
```

El script genera PDFs dummy (si no existen), procesa cada lote forzando `DummyOCRProvider/DummyLLMProvider` con los niveles de concurrencia indicados y deja un informe en `OUT/BENCHMARK.txt` con docs/min y p50/p95 de OCR/Rules/LLM/Total. Es perfecto para comparar hardware o validar que un cambio no penaliza el tiempo total.

### Benchmark clean vs dirty (Azure/OpenAI)

```bash
python tools/benchmark.py --count 50 --seed 123 --tenant demo --input clean --input dirty
```

Genera (si hace falta) los lotes sintéticos limpios/dirty, procesa cada uno con los providers configurados y escribe `OUT/BENCHMARK_<timestamp>.txt` con p50/p95 de OCR/LLM/Total, % auto-post, % premium, tokens IN/OUT y coste estimado por lote, además de un bloque comparativo clean vs dirty. Usa `--input clean` o `--input dirty` para limitarse a un solo set o `--out-report` para guardar el informe en otra ruta.

## Orquestación y Jobs

CERTIVA incluye un scheduler ligero en SQLite para lanzar tareas recurrentes:

```bash
# Ver jobs definidos
python -m src.jobs list

# Crear un job que escanee la carpeta IN/demo cada 5 minutos
python -m src.jobs add-scan-folder --name scan_demo --tenant demo --path IN/demo --schedule every_5m

# Importar el CSV bancario todas las noches
python -m src.jobs add-import-bank --name bank_demo --tenant demo --path tests/bank_demo.csv --schedule daily_02:00 --auto-match

# Ejecutar jobs que toquen (pensado para cron cada minuto)
python -m src.jobs run-due
```

Tipos soportados hoy: `scan_folder`, `import_bank_csv`, `run_preflight`, `run_policy_sim`, `run_golden`. Cada job guarda `last_run_at/last_status` y se muestra en el dashboard para tener visibilidad de la orquestación.

- Cualquier job puede declarar `{"max_retries": 3, "retry_delay": 10}` en su `config` para tener reintentos con *backoff* exponencial + jitter.
- Los schedules soportan sufijo `_jitter` (`every_5m_jitter`) para repartir ejecuciones y evitar thundering herd; además `run_started_at` actúa como lock blando cuando otro worker lo esté procesando.

### Consola admin (jobs y tenants)

Con una sesión `admin` puedes gestionar la orquestación directamente desde la web:

- `/admin/jobs`: listado de jobs con botones para ejecutar, habilitar/deshabilitar o borrar cada uno (muestran último estado y error si lo hubiera).
- `/admin/tenants`: tabla editable de `tenants.json` (ERP, diarios, cuentas, notas) y creación de nuevos tenants sin tocar ficheros.
- `/admin/tenants/<tenant>`: formulario con validación básica; los cambios se guardan inmediatamente y el motor recarga la configuración.

## Ejecución con Docker

El repositorio incluye un `Dockerfile` + `docker-compose.yml` para desplegar CERTIVA sin depender del host:

```bash
docker compose up --build
```

Esto instala dependencias, expone `uvicorn` en `0.0.0.0:8000` y monta `IN/`, `OUT/` y `db/` como volúmenes persistentes. Tras el primer arranque crea un usuario dentro del contenedor:

```bash
docker compose exec certiva python -m src.auth create-admin
```

Con ese usuario ya puedes entrar en `http://localhost:8000/login` y usar tanto la UI como los CLIs (`jobs`, `bank_matcher`, `reports`, etc.) desde `docker compose exec`.

### Salud del servicio

El API expone `/healthz` (ping rápido), `/readyz` (comprueba lectura/escritura en SQLite y en `IN/`, `OUT/json`, `OUT/csv`, `OUT/logs`) y `/metrics` en formato Prometheus para monitorizar KPIs clave. El `docker-compose.yml` incluye un healthcheck basado en `curl` para facilitar despliegues supervisados.

### 2. Revisar cola HITL

```bash
# Vista rápida
python -m src.hitl_cli list

# Revisión interactiva (atajos A/E/D/R/S)
python -m src.hitl_cli review

# Filtrar por doc_type (ej. sólo ventas)
python -m src.hitl_cli review --doc-type sales
```

- En cada documento puedes **Aceptar (A)**, **Editar (E)**, **Duplicar (D)**, **Reprocesar (R)** o **Saltar (S)**.
- Si la incidencia es `NO_RULE`, el flujo ofrece crear la regla (NIF→cuenta/IVA) y aplicarla automáticamente al resto de pendientes del mismo NIF.
- Toda acción queda registrada en `db/audit` (`HITL_*`, `LEARN_RULE`) y, tras aceptar, el pipeline reprocesa el documento para generar/exportar el CSV.

### 3. Métricas SLA / latencias

```bash
python -m src.metrics stats
```

Salida orientativa:

```
Documentos totales: 10
Publicado: 7
Auto-post: 71.4%
Tiempo total medio (min): 1.80
P50 total (min): 1.10
P90 total (min): 3.20
OCR medio (s): 2.12
Validación medio (s): 0.42
Entrada->Post medio (s): 0.38
Duplicados detectados: 1
Reglas aprendidas: 2

Auto-post por tipo:
  - invoice: 6/7 → 85.7%
  - credit_note: 1/1 → 100.0%

Conciliación bancaria:
  - Facturas conciliadas: 6/7
  - Movimientos conciliados: 6/6

Cartera ventas:
  - Facturas AR: 10
  - Cobradas: 4 (40%)
  - Pendientes: 6 (de las cuales 2 vencidas)
```

> `% auto-post` excluye cualquier documento con trazas HITL (`HITL_*` en la tabla `audit`).

### 3.b Conciliación bancaria por CSV

Puedes importar movimientos bancarios desde CSV (sin PSD2) y conciliarlos contra las facturas ya procesadas. Ejemplo usando el fichero sintético `tests/bank_demo.csv`:

```bash
# 1) Importar CSV (usa `DEFAULT_TENANT` salvo que pases otro)
python -m src.bank_matcher import --tenant demo --file tests/bank_demo.csv

# 2) Ejecutar el matching heurístico (importe ±0,01 y ventana de fechas de ±10 días)
python -m src.bank_matcher match --tenant demo

# 3) Ver métricas de conciliación
python -m src.bank_matcher stats --tenant demo
```

Los resultados también se incluyen automáticamente en `python -m src.metrics stats` y en el dashboard web (`Conciliación bancaria`), de forma que puedes ver qué parte del ciclo factura→pago está ya cerrada.

Para facturas emitidas (AR) tienes un CSV de ejemplo en `tests/bank_ar_demo.csv`:

```bash
python -m src.bank_matcher import --tenant demo --file tests/bank_ar_demo.csv
python -m src.bank_matcher match --tenant demo
```

Durante el matching se marca cada factura de venta conciliada como `paid_flag=1`, y las métricas (`python -m src.metrics stats`) muestran la **cartera de ventas** (cobradas, pendientes y vencidas).

Comandos adicionales útiles:

```bash
# Ver docs y movimientos pendientes (puedes filtrar por tipo sales/...)
python -m src.bank_matcher list --tenant demo --doc-type sales

# Eliminar conciliaciones automáticas de un documento
python -m src.bank_matcher clear --doc <doc_id>

# Registrar un match manual (ej. si tienes un extracto conciliado a mano)
python -m src.bank_matcher override --tenant demo --doc <doc_id> --tx <tx_id> --amount 500.00
```

En la UI HITL, cada documento muestra su porcentaje conciliado y los movimientos asociados, y dispone de un botón para “Eliminar conciliación” (con opción de respetar o no los overrides manuales).

## Reporting y Analítica

CERTIVA genera informes financieros directamente desde `docs.sqlite` y los JSON contables:

```bash
# Pérdidas y ganancias del primer trimestre
python -m src.reports pnl --tenant demo --date-from 2025-01-01 --date-to 2025-03-31 --format text

# Informe de IVA del mismo periodo en CSV
python -m src.reports iva --tenant demo --date-from 2025-01-01 --date-to 2025-03-31 --format csv

# Antigüedad de saldos AR a una fecha
python -m src.reports aging --tenant demo --as-of 2025-03-31 --flow AR

# Cashflow previsto a 3 meses
python -m src.reports cashflow --tenant demo --date-from 2025-03-01 --months 3 --format text
```

Los ficheros generados se guardan en `OUT/reports/` (`.csv` o `.json` según formato). El dashboard web muestra resúmenes del mes en curso: resultado (ingresos/gastos), IVA (soportado/repercutido), aging AR y cashflow 3 meses, con las mismas cifras que la CLI.

### Asistente Normativo y Explicador IA

CERTIVA incluye un asistente RAG basado en normativa local (`data/normativa/*.md`) y un explicador IA para informes:

```bash
# Preguntar dudas normativas
python -m src.rag_normativo ask --question "¿Qué cuenta uso para alquiler de oficina?"

# Explicar issues de un documento concreto
python -m src.rag_normativo explain-doc --doc-id <doc_id>

# Obtener un resumen en lenguaje de negocio del P&L/IVA/Cashflow
python -m src.reports explain-pnl --tenant demo --date-from 2025-01-01 --date-to 2025-03-31
python -m src.reports explain-iva --tenant demo --date-from 2025-01-01 --date-to 2025-03-31
python -m src.reports explain-cashflow --tenant demo --date-from 2025-01-01 --months 3
```

En la UI (`/assistant`) se puede plantear una pregunta normativa o introducir un `doc_id` para que el asistente explique los issues o la lógica contable. Además, cada card del dashboard (P&L, IVA y Cashflow) ofrece un botón **“Explicar”** que abre un análisis en lenguaje natural para compartir con clientes o gestores.

## Export fiscal (SII, Facturae, FACe, Veri*Factu)

CERTIVA genera ficheros fiscales totalmente offline:

```bash
# Declaración SII de un periodo
python -m src.fiscal export-sii --tenant demo --date-from 2025-01-01 --date-to 2025-03-31

# Facturae XML para enviar a FACe/FACeB2B
python -m src.fiscal export-facturae --doc-id sales_doc_id

# Payloads FACe / Veri*Factu
python -m src.fiscal export-face-payload --doc-id sales_doc_id
python -m src.fiscal export-verifactu --doc-id sales_doc_id --action ALTA
```

Los ficheros se guardan en `OUT/sii/`, `OUT/facturae/` y `OUT/efactura/`. Desde la UI (`/admin/fiscal`) los administradores pueden lanzar estos exports para el mes actual o para un documento concreto.

El XML de Facturae se valida contra el esquema `data/xsd/facturae_3_2_2.xsd` antes de escribirse; si faltan campos o los totales no cuadran (`base + IVA = total`), se lanza una excepción y no se genera el fichero.

## Integraciones ERP y e-factura (modo offline)

El `HoldedAdapter` genera JSON compatibles con la API de Holded en `OUT/holded/<doc_id>.json`. El pipeline selecciona el adapter según `tenants.json`. Esto permite revisar los asientos antes de cablear los POST reales.
Cada JSON incluye `date`, `dueDate`, `documentNumber`, `contact`, `currency`, `lines` (cuenta/debe/haber) y los totales del asiento para que el payload sea plug&play cuando se conecte la API.

## Orquestación avanzada (IMAP/SFTP stubs)

`ImapSource` y `SftpSource` incluyen la configuración necesaria (host, usuario, mailbox, remote_path). Hasta que haya credenciales reales, funcionan en modo “copy-only” leyendo de los directorios locales (`IN/<tenant>/imap/` y `IN/<tenant>/sftp/`). Los jobs `scan_imap` y `scan_sftp` están listos y registran `SKIPPED` si el conector aún no está implementado.

## AI Governance & LLM logging

Cada llamada al router LLM queda registrada en la tabla `llm_calls` (task, modelo, tokens, latencia, error). El dashboard muestra las estadísticas agregadas y puede auditarse desde SQLite:

```sql
SELECT * FROM llm_calls ORDER BY created_at DESC LIMIT 20;
```

### 4. Procesar lotes / pruebas

```bash
# Modo batch (sin watcher):
python -m src.watcher --batch-dir tests/golden

# Golden set clean vs dirty
echo "Clean:" && python -m tests.run_golden --reset --force
echo "Dirty:" && python -m tests.run_golden --dirty --reset --force

# Regenerar PDFs sintéticos + dirty
python -m tests.generate_golden
python -m tests.augment
```

`tests/augment.py` genera las versiones sucias (imagen rotada + blur + JPEG) partiendo de los PDFs limpios.

`tests/golden_manifest.csv` etiqueta cada PDF con su categoría, tenant y flujo (AP/AR). `tests/run_golden.py` utiliza ese manifest para producir un informe por categoría y `doc_type`, mostrando nº de documentos, % auto-post, pendientes HITL y (si has importado CSV bancarios) la conciliación alcanzada.

Todas las ingestas pasan ahora por el concepto de **Source** (`src/sources.py`). `LocalFolderSource` está implementado al 100 % (usado por `watcher` y los jobs), mientras que `ImapSource` y `SftpSource` dejan preparado el interfaz para cuando conectemos credenciales reales (descarga de adjuntos en IMAP, SFTP de gestorías, etc.).
El dashboard (`/`) muestra también los jobs definidos (schedule, ON/OFF, último run) para tener visibilidad rápida de la orquestación.

## Export a3innuva

CSV generado en `OUT/csv/<doc_id>.csv` con cabecera:

```
Fecha,Diario,Documento,Cuenta,Debe,Haber,Concepto,NIF
```

Columnas soportadas nativamente por la importación a Diario de a3innuva. Pasos típicos:

1. En a3innuva Contabilidad → Diario → **Importar**.
2. Selecciona formato CSV con separador `,` y codificación UTF-8.
3. Mapea las columnas anteriores (`Fecha`, `Diario`, `Documento`, `Cuenta`, `Debe`, `Haber`, `Concepto`, `NIF`).
4. Ejecuta la importación; los asientos quedan como borrador y se pueden revisar antes de contabilizar.

Ejemplo de CSV generado:

```
2025-01-15,COMPRAS,IB-2025-001,628000,120.00,0.00,IB-2025-001,A12345678
2025-01-15,COMPRAS,IB-2025-001,472000,25.20,0.00,IVA SOPORTADO,
2025-01-15,COMPRAS,IB-2025-001,410000,0.00,145.20,IBERDROLA COMERCIALIZACION...,A12345678
```

## Reglas deterministas

- Importes controlados con `Decimal` (tolerancia `±0.02`); si `base + IVA != total` se genera issue `AMOUNT_MISMATCH`.
- Fechas se normalizan a ISO (`YYYY-MM-DD`) usando `dateutil` (admite `dd/mm/yyyy`). Fechas futuras (+3 días) levantan `FUTURE_DATE`.
- Validación NIF/NIE/CIF básica: se verifica estructura y dígito de control; si la evidencia es dudosa, baja la confianza o se crea `NIF_SUSPECT`.
- Anti-duplicados en SQLite (`dedupe`) vía `(NIF + número)` o `(NIF + gross ± 0.01)` en una ventana de 180 días.
- Mapping proveedor→cuenta/IVA desde `rules/vendor_map.csv` (por NIF, luego similitud nombre) y fallback `llm_suggest` (sólo sugerencia, pasa por HITL).
- Se calcula `confidence_entry`, `confidence_ocr` y `confidence_global = min(ocr, entry)`. Si `confidence_global < MIN_CONF_ENTRY`, hay duplicados o issues críticos → HITL.
- Informe pre-SII (`python -m src.metrics preflight`) que cuenta issues (`NIF`, importes, duplicados, etc.) antes de lanzar un lote.
- `classifier.py` etiqueta cada documento con `doc_type` (`invoice`, `credit_note`, `intracom_invoice`, `expense_ticket`, …). Este dato alimenta las métricas, la UI HITL y los informes `tests.run_golden`.

## Módulo AR (facturas emitidas)

- El manifest (`tests/golden_manifest.csv`) y el `classifier` distinguen flujos `flow=AR` con doc_types `sales_invoice`, `sales_credit_note`, `sales_intracom_invoice`, etc.
- `rules_engine.generate_entry` genera asientos de ventas (ingresos 7xx, IVA repercutido 477x y clientes 430x) reutilizando las reglas existentes. Los abonos invierten los Debe/Haber automáticamente.
- Las categorías `ventas_*` traen cuentas por defecto (`SALES_CATEGORY_ACCOUNT_MAP`) y puedes registrar clientes en `rules/vendor_map.csv` igual que a los proveedores.
- `tenants.json` puede definir `sales_journal` y `customer_account` para personalizar el diario y la cuenta 43xx por tenant.
- `bank_matcher` marca los cobros como conciliados y actualiza `docs.paid_flag/paid_ts`. El dashboard y `python -m src.metrics stats` muestran la cartera de ventas (cobradas, pendientes y vencidas).

## Multi-tenant a3innuva

- Configura `tenants.json` para describir el diario y la cuenta de proveedores por tenant:

```json
{
  "default": {
    "default_journal": "COMPRAS",
    "supplier_account": "410000",
    "sales_journal": "VENTAS",
    "customer_account": "430000"
  },
  "cliente_b": {
    "default_journal": "GASTOS",
    "supplier_account": "410200",
    "sales_journal": "VENTAS",
    "customer_account": "430200"
  }
}
```

- El exportador (`src/exporter.py`) aplica estas preferencias automáticamente cuando genera el CSV, tanto para compras (410/472/6xx) como para ventas (430/477/7xx).
- El `exporter` usa un patrón de **adapters**. Hoy `A3InnuvaAdapter` es el único implementado, pero ya existen stubs para `contasol` o `holded` (configura el campo `"erp"` por tenant para seleccionar el adaptador). Añadir un ERP nuevo implica implementar la clase `ERPAdapter` correspondiente sin tocar el pipeline.

## HITL y aprendizaje

- `review_queue` almacena motivos + sugerencias JSON.
- CLI permite aceptar, editar (cambia cuenta/IVA), marcar duplicado o reprocesar con atajos rápidos.
- Cada aceptación puede persistir una nueva regla (`rules/vendor_map.csv`). También se puede aplicar la regla recién creada a todos los pendientes del mismo NIF.
- Auditoría (`db/audit`) guarda quién, cuándo, antes/después.

### Informe de aprendizaje / cobertura de reglas

```bash
python -m src.learning report --tenant demo --limit 5
```

El informe resume cuántas reglas de proveedor hay por tenant, cuántas veces se están usando (por `mapping_source`: regla por NIF, por nombre, sugerencia LLM, fallback de categoría, etc.) y qué proveedores siguen provocando `NO_RULE`. Sirve para priorizar sesiones HITL y para mostrar mejoras “antes/después” en demos.

## Golden set

- `tests/golden/` contiene 34 PDFs limpios: 24 de compras (AP) y 10 de ventas (AR) con categorías como suministros, alquiler, software, hostelería, intracomunitarias, abonos, `ventas_servicios`, `ventas_productos`, `ventas_intracom`, etc.
- `tests/golden_dirty/` contiene las versiones degradadas (rotación, blur y compresión JPEG intensiva).
- Usa `python -m tests.run_golden [--dirty] [--force]` para comparar % auto-post y `P90` entre ambos lotes.
- `python -m tests.generate_golden` regenera 24 facturas limpias (suministros, alquiler, software, intracomunitarias, abonos, etc.). El manifest enlazado permite agrupar métricas por categoría.
- El informe del comando muestra tanto el resumen por categoría como el resumen por `doc_type`, además de las métricas de conciliación bancaria si se han importado movimientos.

## Logs y trazabilidad

- `OUT/logs/certiva.log` con rotación 5×5 MB.
- Tabla `audit` registra `EXPORT_CSV`, acciones HITL y aprendizaje de reglas.
- Errores bajan el status a `ERROR` pero no detienen el watcher.

## Dependencias clave

- `watchdog` (file watcher)
- `azure-ai-formrecognizer` (prebuilt invoice)
- `pandas`, `pydantic`, `python-dotenv`, `python-dateutil`, `tqdm`
- `pdfminer.six`, `reportlab`, `pdf2image`, `Pillow` (golden set / dummy OCR)
- `openai` (opcional para `llm_suggest`)

## Próximos pasos sugeridos

1. Conectar clave real de Azure / Google y ajustar `OCR_PROVIDER`.
2. Completar `llm_suggest` con un endpoint propio y políticas de uso.
3. Añadir pruebas automatizadas sobre `tests/golden*` + integración continua.
4. (Opcional) Integrar segundo OCR provider y escoger por mejor confianza.

## Demo rápida

```bash
# Procesa tests/golden desde cero y abre métricas
python -m src.demo --reset

# Idéntico pero abre flujo HITL para el primer documento pendiente
python -m src.demo --reset --hitl
```

El comando imprime métricas antes/después y, si se usa `--hitl`, lanza el flujo interactivo sobre el primer documento pendiente (ideal para mostrar aprendizaje de reglas durante la demo).

## Wizard de onboarding

```bash
# Modo general (AP)
python -m src.wizard --reset --limit 3

# Modo AR → sólo ventas
python -m src.wizard --reset --limit 3 --focus sales
```

Descripción:

1. Resetea el estado (opcional) y procesa la carpeta indicada (`tests/golden` por defecto).
2. Muestra métricas “antes” (auto-post, P50/P90, duplicados y, si usas `--focus sales`, la cartera AR).
3. Guía la revisión HITL de los principales `NO_RULE`, aprendiendo reglas y aplicándolas a NIF similares si quieres.
4. Reprocesa el lote y muestra las métricas “después”, resaltando la mejora en auto-post, la reducción de la cola HITL y (en modo ventas) la mejora en cobradas vs pendientes.

## Simulador de políticas de auto-post / riesgo

Explora distintos perfiles de publicación automática sin tocar la base de datos:

```bash
# Política conservadora sobre todos los docs
python -m src.policy_sim --policy conservative

# Política agresiva sólo sobre ventas usando el manifest de golden
python -m src.policy_sim --policy aggressive --doc-type sales --manifest tests/golden_manifest.csv

# Ajustar el umbral de confianza mínimo
python -m src.policy_sim --policy balanced --min-conf 0.8
```

El informe muestra `% auto-post`, tamaño estimado de la cola HITL, desglose por `doc_type`, issues que pasarían sin revisión y un score de riesgo acumulado. Ideal para preparar demos con distintos niveles de tolerancia o para justificar políticas de control internas.

## Tests automatizados

Hay una batería mínima de `pytest` que cubre los módulos clave:

- `rules_engine`: generación de asientos AP/AR y cuentas esperadas.
- `bank_matcher`: pagos parciales (1 factura ↔ N movimientos) y actualización de `reconciled_pct`.
- `reports`: P&L, IVA, aging y cashflow construidos desde docs sintéticos.
- `policy_sim`: coherencia del simulador frente a los docs creados en los tests.

Cada test crea un entorno aislado (SQLite + `OUT/json` temporales), por lo que puedes ejecutarlos en cualquier máquina sin tocar tu base real:

```bash
pytest
```

## Seguridad avanzada

- Tabla `login_attempts` con rate-limit configurable (`AUTH_MAX_FAILS` + `AUTH_LOCK_MINUTES`) y registro de IP.
- Validación de `Origin/Referer`, rotación de sesión en el login y CSRF obligatorio en todos los formularios críticos.
- Gestión completa de usuarios desde CLI (`create-admin`, `list`, `change-password`, `activate`, `deactivate`) y desde la UI `/admin/users` (roles + activar/desactivar).
- `/healthz` y `/readyz` siguen expuestos y el `docker-compose` incorpora `healthcheck` para orquestadores.
- Los logs pasan por un filtro PII reutilizando las mismas reglas del router LLM, evitando que aparezcan NIF/IBAN/nombres en texto claro.

## Asistente IA & PII

- El router multi-LLM centraliza proveedores y cae automáticamente a `dummy` si faltan credenciales, registrando cada llamada en `llm_calls`.
- Configura `LLM_ENABLE_PII` y `LLM_PII_SCRUB_STRICT` para controlar el scrub de NIF, IBAN, nombres de contrapartes y numeraciones sensibles antes de llamar al modelo.
- El dashboard muestra un bloque de “LLM Governance” con nº de llamadas, errores y latencias; puedes auditar el detalle en SQLite.
- `/assistant` y los explicadores (`/reports/explain`) usan esta capa, por lo que están preparados para conectar Grok/Groq/OpenAI cuando tengas API keys reales.
- `LLM_MAX_CALLS_TENANT_DAILY` y `LLM_MAX_CALLS_USER_DAILY` definen soft-limits diarios; si se superan, el router responde con una simulación y deja constancia del evento en `llm_calls`.
