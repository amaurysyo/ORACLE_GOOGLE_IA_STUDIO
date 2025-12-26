# slicing_hit runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Identifica **slicing hitting**: ráfagas de ejecuciones agresivas (market/IOC) sin requerir igualdad de tamaños.
- No debe disparar cuando las qty son <`qty_min` o cuando el gap entre trades supera `gap_ms` (patrón no compacto).

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Stream de trades con timestamp ms y side ejecutado.
- Persistencia en `oraculo.slice_events` con `event_type='slicing_hit'` y `fields.mode='hitting'`.
- Lag objetivo <200 ms; monitorear `ws_msg_lag_ms` y `alerts_queue_time_latest_seconds{stream="trades"}`.

## 3) Configuración (knobs YAML + valores recomendados iniciales)
Bloque YAML:

```yaml
detectors:
  slicing_hit:
    gap_ms: 80
    k_min: 1
    qty_min: 150.0
    # require_equal forzado a false en código
```
- `gap_ms`: máximo gap entre fills.
- `k_min`: nº mínimo de trades consecutivos (1 permite singles grandes).
- `qty_min`: filtro principal de severidad.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Métrica primaria: `fields->>'qty_sum'` o `fields->>'avg_qty'` en `oraculo.slice_events`.
- Regla: warn=p95, strong=p99 de la qty por ráfaga; ajustar `k_min` a p90 del conteo.

```sql
WITH ev AS (
  SELECT
    COALESCE((fields->>'qty_sum')::numeric, (fields->>'avg_qty')::numeric) AS qty,
    (fields->>'k')::int AS k
  FROM oraculo.slice_events
  WHERE event_type = 'slicing_hit'
    AND event_time >= now() - interval '14 days'
)
SELECT
  percentile_cont(0.90) WITHIN GROUP (ORDER BY qty) AS qty_p90,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY qty) AS qty_p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY qty) AS qty_p99,
  percentile_cont(0.90) WITHIN GROUP (ORDER BY k)   AS k_p90,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY k)   AS k_p95
FROM ev;
```
- Si hay muchas señales pequeñas, eleve `qty_min` hacia p95/p99. Si faltan disparos grandes, considere `k_min=2` para filtrar single prints accidentales.

## 5) Validación (queries de “health”, ejemplos de señales recientes)

```sql
SELECT event_time, side, intensity,
       fields->>'qty_sum' AS qty_sum,
       fields->>'k' AS k,
       duration_ms
FROM oraculo.slice_events
WHERE event_type = 'slicing_hit'
  AND event_time >= now() - interval '24 hours'
ORDER BY event_time DESC
LIMIT 50;
```
- Auditoría offline: `SQL/f_slicing_blocks.sql` genera bloques hitting por ventana y permite comparar contra eventos grabados.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Pipeline: `oraculo_alerts_queue_depth{stream="trades"}`, `alerts_stage_duration_ms{stage="slicing"}`.
- Ingesta: `ws_msg_lag_ms{stream="trades"}`.
- Logs incluyen payload con `k`, `qty_sum`, `duration_ms`, `latency_ms` y `instrument_id`.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Usar el índice `idx_slice_by_type` y limitar consultas por `event_time` para evitar full scans.
- Mantener `gap_ms` cercano a latencias observadas; valores altos multiplican combinaciones y CPU.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Sin señales: confirmar ingestión de trades y reloj (`ws_last_msg_age_seconds`) y que `qty_min` no esté por encima del p99 real.
- Señales ruidosas: subir `qty_min` o `k_min`; revisar duplicados por reenvío WS (resyncs en logs `ws_resync_total`).

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Incrementar `qty_min` a p99 y `gap_ms` a 0 para pausar sin desactivar código.
- Para rollback completo, deshabilitar la regla que consume `slicing_hit` o comentar el bloque en `rules.yaml` (manteniendo otros detectores intactos).
