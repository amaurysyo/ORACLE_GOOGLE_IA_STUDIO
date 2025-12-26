# slicing_hit runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta slicing de órdenes agresivas que barren liquidez (hitting) sin requerir igualdad exacta de qty.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Trades agresivos con marca de side y precio ejecutado.
- Depth para validar distancia al best y continuidad.

## 3) Configuración (knobs YAML + valores recomendados iniciales)
- `gap_ms` (80 ms) para agrupar ejecuciones.
- `k_min` (1) como mínimo de fills en la ráfaga.
- `qty_min` (150) para tamaño mínimo acumulado.
- `require_equal` forzado a false en código (no chequear igualdad).

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Usar percentiles de tamaño de trade para ajustar `qty_min` por mercado.
- Revisar distribución de tiempo entre trades para validar `gap_ms`.

## 5) Validación (queries de “health”, ejemplos de señales recientes)
- Query de health: ráfagas detectadas por hora y ratio de confirmación en precios.
- Revisar 3 ejemplos recientes con trayectoria de precio.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Métrica `detector_slicing_hit_hits_total` por símbolo.
- Log de ráfagas con acumulado de qty y duración.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Indices por `symbol, ts` en eventos de trade.
- Limitar análisis a últimas n ventanas para evitar scans largos.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Si hay sobre-detección: subir `qty_min` o `k_min`.
- Si falta detección: revisar lag de trades o configuración de side.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Setear `enabled=false` o aumentar `qty_min` hasta estabilizar.
- Incrementar `gap_ms` para reducir sensibilidad a microburst.
