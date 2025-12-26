# slicing_aggr runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta patrones de **slicing agresivo/iceberg** cuando llegan ráfagas de trades con tamaños similares y brechas cortas (modo `require_equal=true`).
- No debe disparar si el gap entre fills excede `gap_ms`, si las qty caen fuera de las tolerancias `equal_tol_pct/abs`, o si la ráfaga no alcanza `k_min` ejecuciones.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Stream de **trades** en tiempo real (timestamp en ms) y best bid/ask para validar precios.
- Serie de auditoría `oraculo.slice_events` (TimescaleDB) donde se escriben los eventos agregados de slicing (`event_type='slicing_aggr'`).
- Lag esperado: <200 ms desde ingestión hasta escritura; si `ws_msg_lag_ms` o `alerts_queue_time_latest_seconds` >0.5 s revisar pipeline.

## 3) Configuración (knobs YAML + valores recomendados iniciales)
Bloque en `config/rules.yaml`:

```yaml
detectors:
  slicing_aggr:
    gap_ms: 80
    k_min: 5
    qty_min: 1
    require_equal: true
    equal_tol_pct: 0
    equal_tol_abs: 0
```
- `gap_ms`: máximo gap entre fills de la ráfaga.
- `k_min`: mínimo de fills consecutivos.
- `qty_min`: tamaño mínimo por fill (BTC/contrato).
- `require_equal` + tolerancias: validan que las qty sean similares (iceberg). Ajustar tolerancias antes de mover `k_min`.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Métrica primaria: `fields->>'avg_qty'` y `fields->>'k'` en `oraculo.slice_events`.
- Regla base: warn ≈ p95, strong ≈ p99 sobre la distribución reciente.

```sql
WITH ev AS (
  SELECT
    (fields->>'avg_qty')::numeric AS avg_qty,
    (fields->>'k')::int AS k,
    duration_ms
  FROM oraculo.slice_events
  WHERE event_type = 'slicing_aggr'
    AND event_time >= now() - interval '14 days'
    AND fields ? 'avg_qty'
)
SELECT
  percentile_cont(0.90) WITHIN GROUP (ORDER BY avg_qty) AS qty_p90,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY avg_qty) AS qty_p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY avg_qty) AS qty_p99,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY k)        AS k_p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY k)        AS k_p99
FROM ev;
```
- Ajustar `qty_min` a p90 si se quieren más señales; a p95/p99 si hay ruido.
- Revisar `gap_ms` con `SQL/f_slicing_blocks_icebering.sql` para ver latencias reales por venue y setear `gap_ms` ≥ p95 de latencia intra-rafaga.

## 5) Validación (queries de “health”, ejemplos de señales recientes)
- Últimos eventos:

```sql
SELECT event_time, side, intensity, duration_ms,
       fields->>'avg_qty' AS avg_qty,
       fields->>'k' AS k,
       fields->>'mode' AS mode
FROM oraculo.slice_events
WHERE event_type = 'slicing_aggr'
  AND event_time >= now() - interval '1 day'
ORDER BY event_time DESC
LIMIT 50;
```
- Reconstrucción offline: ejecutar `SQL/f_slicing_blocks_icebering.sql` para comparar bloques detectados vs `slice_events` y validar `k_min`/`qty_min`.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Backpressure y lag: `oraculo_alerts_queue_depth{stream="trades"}`, `oraculo_alerts_queue_time_latest_seconds{stream="trades"}`.
- Ingesta: `ws_msg_lag_ms{stream="trades"}`.
- Logs de auditoría incluyen `event_type=slicing_aggr`, `mode=iceberg`, `k`, `avg_qty`, `gap_ms`, `latency_ms`.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Índices existentes: `idx_slice_by_time` y `idx_slice_by_type` en `oraculo.slice_events`; mantener consultas acotadas por `event_time`.
- Limitar lookback de auditorías a ≤14 días para no descomprimir chunks antiguos.
- `gap_ms` bajo reduce cardinalidad; evitar `k_min` demasiado pequeño para no generar millones de eventos.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Tabla vacía: validar ingestión de trades y que `require_equal=true` no esté filtrando (probar `equal_tol_pct=0.01`).
- Falsos positivos: subir `qty_min` o `k_min`, o exigir `gap_ms` menor.
- Latencia alta: revisar `alerts_queue_time_latest_seconds` y posibles bloqueos de DB (`alerts_db_queue_depth`).

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Subir `qty_min` a p99 y `k_min` +1 para modo solo-señales fuertes.
- Si ruido persiste, marcar `require_equal=false` para re-etiquetar como `slicing_hit` o desactivar con `enabled=false` en reglas.
