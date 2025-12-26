# dominance runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta dominancia de un lado del libro cuando un lado concentra ≥80% de profundidad.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Depth agregado a N niveles (levels=1000 por defecto).
- Spread para gating (`max_spread_usd`).

## 3) Configuración (knobs YAML + valores recomendados iniciales)
- `dom_pct` (0.80) y `dom_pct_doc` (0.60) como thresholds.
- `metric_source` (legacy|doc|auto).
- `levels`, `hold_ms`, `retrigger_s`, `max_spread_usd`.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Calcular percentiles de ratio de profundidad por lado para fijar `dom_pct`.
- Alinear `hold_ms` con ruido intrínseco del book.

## 5) Validación (queries de “health”, ejemplos de señales recientes)
- Health: % de ventanas con dominancia por símbolo.
- Ejemplos recientes con snapshot de depth y spread.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Métrica `detector_dominance_active` y contadores por side.
- Logs con `metric_source` efectivo y niveles usados.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Limitar agregación a `levels` configurado.
- Indexar depth por `ts` y `side`.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Si sin señales: revisar ingestión de depth o `metric_source`=doc sin datos.
- Si ruido: subir `dom_pct` o `hold_ms`.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Subir `dom_pct` temporalmente o desactivar con `enabled=false`.
- Reducir `hold_ms` a 0 para modo observación sin alerts.
