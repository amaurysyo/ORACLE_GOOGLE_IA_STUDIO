# slicing_pass runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta slicing pasivo en libro (iceberg) cuando aparecen múltiples órdenes pasivas similares en ventanas breves.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Depth incremental para ver colocación de órdenes pasivas.
- Opcional: trades para confirmar ejecución parcial.

## 3) Configuración (knobs YAML + valores recomendados iniciales)
- `gap_ms` (120 ms) como ventana para agrupar colocaciones.
- `k_min` (6) mínimo de niveles agregados.
- `qty_min` (5) tamaño mínimo por evento.
- `require_equal` con tolerancias `equal_tol_pct`/`equal_tol_abs` para validar consistencia.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Alinear `gap_ms` con latencia observada en mensajes de depth.
- Ajustar `k_min` según frecuencia de reposteo legítimo.

## 5) Validación (queries de “health”, ejemplos de señales recientes)
- Contar lotes iceberg detectados vs total de mensajes en la última hora.
- Revisar ejemplos con path de precios y tamaños.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Métrica `detector_slicing_pass_hits_total`.
- Logs con snapshots inicial/final para auditoría.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Indexar depth por `ts, level` si se persiste histórico.
- Filtrar por los primeros niveles para reducir carga.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Si tabla vacía: revisar ingesta de depth y reorder de mensajes.
- Si falsos positivos: subir `k_min` o `qty_min`.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Deshabilitar detector o aumentar `qty_min` mientras se recalibra.
- Reducir `require_equal` para modo seguro.
