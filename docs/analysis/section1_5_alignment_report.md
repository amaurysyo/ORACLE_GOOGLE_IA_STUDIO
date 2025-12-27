# Informe de alineación — Secciones 1 a 5 (DOC vs Repo)  
**Proyecto:** Oráculo BTCUSDT  
**Base analizada:** repo `ORACLE_GOOGLE_IA_STUDIO-master` (zip aportado) + DOC `📘 Proyecto — Oráculo Btcusdt V1 — ACTUALIZADO.docx`  
**Fecha:** 2025-12-27

## Contexto mínimo (para no perder continuidad)

En el ciclo anterior cerramos **Sección 6 (métricas)** y **Sección 7 (reglas, Opción A)** con un enfoque **DOC-first** y **fallback a legacy** para no romper semántica histórica. Eso dejó el proyecto en un estado “operable” (feature flags por detector, macro-detectores separados, runbooks completos y runbook global de cierre).

### Qué significa “DOC” vs “Legacy”
- **Legacy:** la métrica/detector tal y como venía funcionando en el repo (semántica vigente, aunque no sea idéntica al DOC original).
- **DOC:** la métrica/detector calculada según definiciones del documento “Proyecto…V1”, con unidades/normalizaciones explícitas y calibración documentada.
- **Auto (cuando aplica):** preferir DOC si existe y es coherente; si no, usar legacy.  
En todos los casos, el pipeline deja **traza auditable** de qué fuente se usó.

---

## Resumen ejecutivo por sección (1–5)

| Sección | Lo que promete el DOC | Lo que existe hoy en el repo | Gap principal | Decisión recomendada |
|---|---|---|---|---|
| 1. Objetivo | Sistema RT, robusto, baja latencia, métricas + reglas + alertas + operación 24/7 | Ya es un sistema RT (ingesta + pipeline de detección + rules + Telegram + observabilidad y runbooks) | Falta “API” de consumo, y falta formalizar despliegue 24/7 (systemd/operación), además de backtesting/modelos | Mantener arquitectura actual; completar *operación 24/7* y añadir API mínima si aporta valor |
| 2. Alcance | WS (depth/trade/mark/liq), REST (OI/TopRatios), TSDB con compresión/retención, detectores (slice/absorption/breakwall/spoof), reglas→Telegram/API, backtesting | Ingesta WS/REST implementada + TSDB + detectores micro y macro + Telegram. Existe esquema BT pero no pipeline | Backtesting/modelos no existen. Compresión/retención y políticas no están “codificadas” en migrations del repo | Tratar “backtesting/modelos” como fase futura; codificar políticas TSDB en SQL/migrations |
| 3. Requisitos técnicos | Python 3.11 async, Timescale, libs, resiliencia | Se cumple en gran parte (asyncpg, pydantic v2, loguru, prometheus, reconexiones, watchdogs) | `requirements.txt` incluye una línea inválida y faltan artefactos de entorno (`.env.example`) | Higiene inmediata: corregir requirements, plantillas de entorno, y secret management |
| 4. Fases | F0…F7 (diseño → ingesta → contexto → detectores → modelos → backtesting → monitorización/alertas) | Sprints 1–3 cubren ingesta + detectores + reglas + operación (runbooks/observabilidad) | “Modelos (Logit)” y “Backtesting” no desarrollados | Re-escribir el roadmap real (Sprint 4+) y re-etiquetar fases en docs |
| 5. Modularidad | separación ingesta/cálculo/reglas/entrega; colas/backpressure; TSDB | Modularidad existe (ingest vs alerts; detect vs rules; macros; router) y hay auditoría extra (orderbook snapshots) | El DOC menciona “spill-to-disk” que no existe; falta formalizar interfaces (eventos canónicos) y coherencia README↔CLI | Formalizar contratos (event schemas), y reconciliar README/CLI; decidir si hace falta spill-to-disk |

---

## Sección 1 — Objetivo general (DOC) vs realidad

### DOC (resumen)
El DOC define un sistema de **tiempo real** y **baja latencia**, capaz de ingerir streams, calcular métricas microestructurales, evaluar reglas y operar 24/7 con tuning y observabilidad.

### Repo (lo que hay)
- Servicios claros (aunque “en un solo repo”):  
  - **Ingesta:** `scripts/cli.py ingest run` → Binance Futures WS + REST (OI/Top Traders) + Spot WS opcional + Deribit WS opcional.  
  - **Alertas/detección:** `scripts/cli.py alerts run` → detectores micro + macro + motor de reglas + router Telegram.  
- Observabilidad: Prometheus exporter (ingest :9000 / alerts :9001), métricas de lag y watchdogs, logs rotativos, runbooks y runbook global de cierre.

### Diferencias / gaps
- **API de consumo:** el DOC menciona Telegram/**API**; el repo actual no expone API HTTP/WebSocket para consumo externo (más allá de Prometheus).
- **Operación 24/7 “paquetizada”:** existe plan operativo (runbooks) pero faltan artefactos típicos de despliegue no-Docker: `systemd` units, user/service, healthchecks, rotación fuera de python, etc.
- **Tuning/BD:** existe base Timescale y vistas/CAGGs, pero faltan scripts “migrations/policies” en repo para reproducir la BD desde cero con garantías.

**Decisión recomendada:** mantener el objetivo del DOC, pero tratarlo como *objetivo operacional*: cerrar “operación 24/7” con artefactos de despliegue y (si realmente hace falta) una **API mínima** (por ejemplo, FastAPI read-only sobre `oraculo.v_alerts_recent`).

---

## Sección 2 — Alcance

### DOC (lo prometido)
- Ingesta: `depth20@100ms`, trades, markPrice (1s), liquidations, y REST para OI + Top Ratios.
- Persistencia en Timescale con compresión/retención.
- Detectores: slicing (agresivo/pasivo), absorption, breakwalls, spoofing.
- Reglas configurables → Telegram/API.
- Backtesting y caracterización empírica.

### Repo (estado actual)
- **Ingesta WS/REST:** implementada como describe el DOC (y ampliada: Spot WS opcional; Deribit WS opcional).  
- **Persistencia:** tablas en `binance_futures`, `binance_spot`, `deribit`, `oraculo`.  
  - Tablas clave (según el backup SQL):  
    - `binance_futures`: `depth`, `trades`, `mark_funding`, `liquidations`, `open_interest`, `top_trader_*_ratio`  
    - `binance_spot`: `depth`, `trades`  
    - `deribit`: `options_*` (incluye `options_iv_surface`)  
    - `oraculo`: `metrics_series`, `slice_events`, `rule_alerts`, `rule_telemetry`, `instrument_catalog`, etc.  
- **Detectores:** los micro detectores del DOC están, y además hay **macro detectores** (oi_spike, liq_cluster, top_traders, gamma_flip, skew_shock, term_structure, etc.) con feature flags.
- **Alertas:** Telegram (router). “API” no.
- **Backtesting:** hay esquema `oraculo_bt` (tablas `bt_*`), pero el repo no incluye runner/pipeline de backtesting.

### Diferencias / gaps
- **Backtesting y modelos**: no existen (por ahora) como software operativo, aunque parte del esquema está presente.
- **Compresión/retención:** el backup SQL muestra presencia de estructuras de Timescale y vistas tipo CAGG (por ejemplo `oraculo.trades_futures_1s_base`, `oraculo.oi_1m_base`, `oraculo.iv_surface_1m`), pero el repo no “declara” las políticas. Esto impide reproducibilidad completa.

**Decisión recomendada:** tratar “Backtesting + modelos” como Sprint futuro (no bloqueante) y **codificar** la parte TSDB (schemas, CAGGs, políticas) como *migrations* versionadas en repo.

---

## Sección 3 — Requisitos técnicos

### Alineado
- Python async + `asyncpg`, `aiohttp`, `python-telegram-bot`, `prometheus-client`.
- Estilo/typing: razonablemente alineado, y el proyecto ya adoptó Pydantic v2 (`pydantic>=2,<3`).

### No alineado / riesgos inmediatos
- `requirements.txt` contiene una línea inválida:  
  - Actual: `pip install nest_asyncio==1.6.0`  
  - Correcto: `nest_asyncio==1.6.0`
- Falta `.env.example` (README lo menciona, pero no existe en el repo actual).
- Gestión de secretos: `config/config.yaml` contiene campos `auth.client_id/client_secret` para Deribit; aunque exista un comentario de “sin credenciales reales”, el patrón recomendado es **ENV-only**.

**Decisión recomendada (higiene S1–S3):** arreglar requirements, añadir `.env.example`, y mover secretos a variables de entorno (config solo con placeholders).

---

## Sección 4 — Fases

El DOC propone F0–F7; el repo real (Sprints 1–3 + ampliaciones) se puede mapear así:

- **F1 Ingesta:** completada (Binance WS/REST; Deribit WS; Spot opcional).
- **F2 Contexto de mercado:** completada en parte (mark/index/basis, OI, funding; + métricas doc/legacy).
- **F3 Detectores + reglas:** completada en gran parte (micro + macro con feature flags, runbooks).
- **F7 Monitorización/alertas:** completada en gran parte (Prometheus, watchdogs, runbooks, cierre).
- **F5 Modelos / F6 Backtesting:** **pendiente**.

**Decisión recomendada:** actualizar docs de roadmap para reflejar “Sprints reales”, y mover F5/F6 a backlog explícito.

---

## Sección 5 — Organización modular

### DOC: contratos e interfaces
El DOC define contratos (eventos canónicos, métricas por clave `{symbol, window, ts}`, alertas con `dedup_key`, etc.) y sugiere backpressure con colas bounded, además de “spill-to-disk” para críticos.

### Repo: implementación real
- **Entry points**
  - `scripts/cli.py ingest run`
  - `scripts/cli.py alerts run`
- **Módulos (mapa rápido)**
  - Ingesta: `oraculo/ingest/*` (Binance WS/REST, Spot WS, Deribit WS, batcher)
  - Detección: `oraculo/detect/*` (orderbook, métricas, micro detectores, macro detectores)
  - Reglas: `oraculo/rules/*` (engine + router)
  - Observabilidad: `oraculo/obs/*`
  - Config hot-reload: `oraculo/config_hot.py`
  - Deribit surface builder: `oraculo/deribit/surface_builder.py`
  - Auditoría orderbook snapshot: `oraculo/ingest/binance_orderbook_audit.py` (mejora respecto al DOC)
- **Backpressure real**
  - Hay colas y políticas de drop/lag monitor, pero **no existe spill-to-disk**.

### Desalineaciones relevantes
1. **README↔CLI:** el README menciona comandos `db:migrate` y `db:refresh-caggs`, pero no aparecen en `scripts/cli.py` actual.  
2. **Contratos de evento:** existen, pero están implícitos; el DOC propone eventos canónicos explícitos.  
3. **Spill-to-disk:** no implementado.

**Decisión recomendada (S5):**
- Formalizar *event schemas* (Pydantic o dataclasses) para los eventos críticos que cruzan módulos (depth/trades/mark/macro-events).
- Reconciliar README con la CLI real (o reintroducir comandos DB si los necesitas para operar).
- Mantener “spill-to-disk” como opción futura (solo si realmente lo pide la operación; hoy el watchdog + dedupe + drop policy suele ser suficiente).

---

## Recomendación de plan (próximos pasos sin reabrir Sección 6/7)

### S1–S5-A — Documento de auditoría (este informe)
- Añadir este archivo como `docs/analysis/section1_5_alignment_report.md`.

### S1–S5-B — Higiene y coherencia operativa (alto impacto, bajo coste)
1. Corregir `requirements.txt` (nest_asyncio).
2. Añadir `.env.example` mínimo.
3. Quitar secretos de `config/config.yaml` (Deribit auth por ENV).
4. Ajustar README para reflejar comandos reales (o reintroducir comandos DB).

### S1–S5-C — Reproducibilidad BD (para operación 24/7)
- Versionar SQL “core” (schemas, tipos, tablas base) y un bloque separado para Timescale (hypertables, CAGGs, políticas).
- (Opcional) añadir una CLI `db:bootstrap` / `db:policies` si quieres automatizar.

### S1–S5-D — API mínima (solo si la necesitas de verdad)
- FastAPI read-only:  
  - `/alerts/recent` → `oraculo.v_alerts_recent`  
  - `/healthz` → checks de DB + lag (métricas internas)  
Esto te da consumo web sin tocar Telegram ni el pipeline.

### S1–S5-E — Backtesting (futuro, pero ya encarrilado)
- Aprovechar `oraculo_bt.bt_*` y definir:  
  - un runner que lea series históricas (trades/depth/mark)  
  - un “replayer” que emule eventos hacia el engine de detectores/reglas  
  - comparativa de señales vs PnL y métricas de precisión/recall.

---

## Apéndice A — Inventario BD (según `SQL_ORACULO_BACKUP.sql`)

### Tablas (por esquema)
- **binance_futures**: `depth`, `trades`, `mark_funding`, `liquidations`, `open_interest`, `top_trader_account_ratio`, `top_trader_position_ratio`
- **binance_spot**: `depth`, `trades`
- **deribit**: `options_trades`, `options_book_changes`, `options_ticker`, `options_mark_price`, `options_greeks`, `options_instruments`, `options_iv_surface`, `options_signals`
- **oraculo**: `metrics_series`, `slice_events`, `rule_alerts`, `rule_telemetry`, `instrument_catalog`, `telegram_bots`, `alert_dispatch_log`
- **oraculo_bt**: `bt_runs`, `bt_trades`, `bt_equity`, `bt_metrics`

### Vistas relevantes (CAGGs / observabilidad)
- `oraculo.trades_futures_1s`, `oraculo.trades_spot_1s`
- `oraculo.oi_1m`
- `oraculo.mark_basis_1s`
- `oraculo.iv_surface_1m`
- `oraculo.v_alerts_recent`, `oraculo.v_events_throughput_1m`, `oraculo.v_slice_recent`

---

## Apéndice B — Observaciones puntuales (para no perderlas)
- El repo actual incluye **auditoría de orderbook** (`oraculo_audit.orderbook_snapshots`) que no aparece en el DOC, y es una mejora fuerte para verificar integridad de depth y para diagnósticos de “niveles >20”.
- Se detecta una inconsistencia documental: README menciona comandos DB que no existen en la CLI actual. Conviene resolverlo pronto para evitar fricción operativa.
