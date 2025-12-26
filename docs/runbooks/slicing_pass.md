# slicing_pass runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta **slicing pasivo** (inserciones repetidas en book) cuando se publican órdenes limit consecutivas con qty similares y gaps cortos (`gap_ms`).
- No debe disparar cuando las inserciones se cancelan rápido sin acumular `k_min` ni cuando la qty por inserción está debajo de `qty_min`.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Stream de depth con acciones `insert/update` y secuencia (`seq`).
- Persistencia de eventos en `oraculo.slice_events` (`event_type='slicing_pass'`, `fields.mode='passive'`).
- Función de verificación `SQL/f_slicing_passive_blocks.sql` para reconstruir bloques desde depth.
- Lag esperado <300 ms; revisar `alerts_queue_time_latest_seconds{stream="depth"}` y `ws_msg_lag_ms`.

## 3) Configuración (knobs YAML + valores recomendados iniciales)

```yaml
detectors:
  slicing_pass:
    gap_ms: 120
    k_min: 6
    qty_min: 5.0
    require_equal: true
    equal_tol_pct: 0
    equal_tol_abs: 0
```
- `gap_ms`: ventana máxima entre inserts.
- `k_min`: nº mínimo de inserts consecutivos.
- `qty_min`: tamaño mínimo por insert.
- `require_equal` + tolerancias: homogeneidad de qty.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Métrica primaria: qty promedio por inserción y longitud del bloque (`k`).

```sql
WITH blocks AS (
  SELECT
    (fields->>'avg_qty')::numeric AS avg_qty,
    (fields->>'k')::int AS k,
    duration_ms
  FROM oraculo.slice_events
  WHERE event_type = 'slicing_pass'
    AND event_time >= now() - interval '30 days'
)
SELECT
  percentile_cont(0.90) WITHIN GROUP (ORDER BY avg_qty) AS qty_p90,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY avg_qty) AS qty_p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY avg_qty) AS qty_p99,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY k)        AS k_p95
FROM blocks;
```
- Regla: warn≈p95, strong≈p99. Si hay outliers por rellenos parciales, usar `equal_tol_pct=0.01` y recalcular.
- Validar `gap_ms` comparando con `f_slicing_passive_blocks` para cada venue; fijar `gap_ms` ≥ p95 de `ts` diff observado para evitar cortes prematuros.

## 5) Validación (queries de “health”, ejemplos de señales recientes)

```sql
SELECT event_time, side, intensity,
       fields->>'avg_qty' AS avg_qty,
       fields->>'k' AS k,
       duration_ms
FROM oraculo.slice_events
WHERE event_type = 'slicing_pass'
  AND event_time >= now() - interval '48 hours'
ORDER BY event_time DESC
LIMIT 50;
```
- Reconstrucción: ejecutar `SQL/f_slicing_passive_blocks.sql` (usa mismo `gap_ms/k_min/qty_min`) y comparar conteos vs eventos emitidos.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Métricas de pipeline depth: `oraculo_alerts_queue_depth{stream="depth"}`, `alerts_stage_duration_ms{stage="depth"}`.
- Logs por evento incluyen `mode=passive`, `k`, `avg_qty`, `duration_ms`, `latency_ms` y side.
- Si se usa DOC para validar precio, revisar `ws_last_msg_age_seconds{stream="doc_depth"}`.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Mantener consultas acotadas por `event_time` usando `idx_slice_by_time`.
- Filtros `k_min` y `qty_min` son el throttle principal; evitar valores demasiado bajos que generen millones de snapshots.
- `gap_ms` alto incrementa combinaciones O(N²) de depth; recalibrar tras cambios de latencia del exchange.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Tabla vacía: validar ingestión depth (triggers `insert_depth_futures` activos) y reloj.
- Falsos positivos por cancelaciones rápidas: subir `k_min` o exigir `hold_ms` en la regla de consumo.
- Si hay lag, revisar `alerts_db_queue_depth` y `alerts_queue_dropped_total`.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Elevar `qty_min` a p99 y `k_min` +2 para modo conservador.
- Desactivar bloque en YAML (`enabled=false` en reglas) o setear `require_equal=false` para convertirlo en hitting temporal.
