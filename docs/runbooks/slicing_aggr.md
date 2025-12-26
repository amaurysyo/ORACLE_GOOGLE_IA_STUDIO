# slicing_aggr runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta slicing agresivo/iceberg cuando múltiples ejecuciones pequeñas ocurren en ventanas cortas con tamaños similares (patrón iceberg).

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Trades en vivo con timestamp en ms para detectar ráfagas.
- Depth opcional para validar best price y consistencia de qty.

## 3) Configuración (knobs YAML + valores recomendados iniciales)
- `gap_ms` (80 ms) para agrupar ejecuciones.
- `k_min` (5) como mínimo de fills consecutivos.
- `qty_min` (1) como tamaño mínimo por fill.
- `require_equal` y tolerancias `equal_tol_pct`/`equal_tol_abs` para validar igualdad.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Ajustar `gap_ms` a percentiles de latencia observada en trades.
- Usar distribuciones de tamaño medio por mercado para definir `qty_min`.

## 5) Validación (queries de “health”, ejemplos de señales recientes)
- Contar secuencias que cumplen los criterios vs total de trades en la última hora.
- Revisar 5 ejemplos recientes con timestamps y qty para validar patrón.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Métrica Prometheus: `detector_slicing_aggr_hits_total` y tasa por instrumento.
- Logs de secuencias con ids de trade y latencia inter-fill.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Indexar tablas de trades por `ts` y `instrument` para consultas rápidas.
- Limitar lookback a ventanas cortas (<5 min) al calcular ráfagas.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Si no hay señales: verificar ingesta de trades y reloj del collector.
- Si hay falsos positivos: subir `qty_min` o `k_min`.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Poner `enabled=false` en el bloque de YAML si hay ruido.
- Subir `qty_min` temporalmente para reducir sensibilidad.
