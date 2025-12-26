# liq_cluster runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta clusters de liquidaciones grandes en ventanas cortas, confirmando con movimiento de precio.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Feed de liquidaciones con qty en USD o base.
- Precio de referencia para validar movimiento (`min_move_usd`).

## 3) Configuración (knobs YAML + valores recomendados iniciales)
- `poll_s` (5) y `window_s` (60).
- Umbrales `warn_usd` (1_000_000) y `strong_usd` (3_000_000).
- Confirmación de momentum: `confirm_s` (10), `momentum_window_s` (30), `min_move_usd` (25), `max_rebound_usd` (10).
- Clamps: `use_usd`, `clamp_usd`.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Analizar distribución de tamaños de liquidación por mercado para ajustar umbrales.
- Backtest con eventos de alta volatilidad para validar confirm_s/momentum.

## 5) Validación (queries de “health”, ejemplos de señales recientes)
- Health: conteo de clusters y proporción con confirmación de precio.
- Ejemplos recientes con lista de liquidaciones y movimiento de precio.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Métricas de severidad (warn/strong) y uso de `use_usd`.
- Logs con lista agregada de liquidaciones por ventana.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Indexar liquidaciones por `ts` y símbolo.
- Usar clamps para evitar outliers que rompan agregaciones.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Si no detecta: validar feed de liquidaciones o que `use_usd` esté correcto.
- Si ruido: subir `warn_usd` o `min_move_usd`.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Desactivar con `enabled=false` o aumentar umbrales.
- Deshabilitar confirmación de precio si bloquea falsamente.
