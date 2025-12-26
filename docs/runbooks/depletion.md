# depletion runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta caídas abruptas de liquidez en bid/ask con ventanas cortas (depletion masivo).

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Depth agregado a `n_levels` (50).
- DOC opcional para métricas derivadas.

## 3) Configuración (knobs YAML + valores recomendados iniciales)
- `window_s` (2s) y `hold_ms` (800).
- Umbrales `pct_drop_bid`/`pct_drop_ask` (0.35).
- `metric_source` y bloque `doc` (`dv_warn`, `dv_strong`, `clamp_abs_dv`).
- `retrigger_s` (30).

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Obtener percentiles de variación de profundidad para ajustar `pct_drop`.
- Usar distribuciones DOC para `dv_warn`/`dv_strong`.

## 5) Validación (queries de “health”, ejemplos de señales recientes)
- Health: número de depletions por lado y ratio de refill posterior.
- Ejemplos recientes con before/after depth.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Métricas por side, severidad y fuente (`metric_source`).
- Logs con valores clamp y ventana efectiva.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Limitar cómputo a `n_levels` configurados.
- Reutilizar agregados DOC para evitar recomputes.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Si no hay detecciones: revisar ingestión de depth o clamps muy bajos.
- Si ruido: subir `pct_drop_*` o `dv_warn`.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Desactivar o aumentar umbrales hasta re-calibrar.
- Extender `retrigger_s` para reducir spam.
