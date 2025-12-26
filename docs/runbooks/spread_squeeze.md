# spread_squeeze runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta compresión anómala del spread cuando se reduce por debajo de umbral con profundidad mínima en ambos lados.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Depth con spread calculado y profundidad en niveles top (levels=50).

## 3) Configuración (knobs YAML + valores recomendados iniciales)
- `max_spread_usd` (0.5) como umbral de squeeze.
- `levels` (50) para medir profundidad.
- `min_depth_bid_btc`/`min_depth_ask_btc` (200) mínimos por lado.
- Timing: `hold_ms` (1500), `retrigger_s` (30).

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Percentiles de spread histórico para validar `max_spread_usd`.
- Ajustar `min_depth_*` según liquidez típica.

## 5) Validación (queries de “health”, ejemplos de señales recientes)
- Health: nº de squeezes y relación con volatilidad.
- Ejemplos recientes con spread y depth en payload.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Métricas de spread actual y duración del squeeze.
- Logs con niveles usados y profundidades medidas.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Limitar análisis a `levels` configurados.
- Indexar depth por `ts` y side.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Si no detecta: revisar cálculo de spread y profundidad mínima.
- Si ruido: bajar `max_spread_usd` o subir `min_depth_*`.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Desactivar con `enabled=false` o subir `max_spread_usd` temporalmente.
- Extender `hold_ms` para exigir persistencia.
