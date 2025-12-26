# spread_squeeze runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta **compresión extrema del spread** cuando el spread en USD cae por debajo de `max_spread_usd` y existe profundidad mínima en ambos lados (`min_depth_*`).
- No debe disparar si el depth disponible es bajo o si el spread rebota antes de `hold_ms`.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Depth consolidado con precios y qty.
- Serie `spread_usd` en `oraculo.metrics_series` (window_s=1) y depth agregado top `levels`.
- Lag esperado <200 ms; vigilar `ws_msg_lag_ms{stream="depth"}` y `alerts_queue_time_latest_seconds{stream="depth"}`.

## 3) Configuración (knobs YAML + valores recomendados iniciales)

```yaml
detectors:
  spread_squeeze:
    enabled: true
    max_spread_usd: 0.5
    levels: 50
    min_depth_bid_btc: 200.0
    min_depth_ask_btc: 200.0
    hold_ms: 1500
    retrigger_s: 30
```
- `max_spread_usd`: umbral principal.
- `levels`: head del libro considerado.
- `hold_ms`: tiempo que debe mantenerse el squeeze.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Métrica primaria: `spread_usd` y depth acumulado top `levels`.

```sql
WITH spr AS (
  SELECT value AS spread
  FROM oraculo.metrics_series
  WHERE metric = 'spread_usd'
    AND instrument_id = :instrument
    AND event_time >= now() - interval '14 days'
)
SELECT
  percentile_cont(0.50) WITHIN GROUP (ORDER BY spread) AS p50,
  percentile_cont(0.90) WITHIN GROUP (ORDER BY spread) AS p90,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY spread) AS p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY spread) AS p99
FROM spr;
```
- Regla: fijar `max_spread_usd` cerca de p10–p20 para capturar compresiones raras; warn en p95 de profundidad y strong en p99 de profundidad acumulada.
- Validar profundidad usando snapshots agregados en CPU (`metrics_series` si persiste `depth_top_levels`).

## 5) Validación (queries de “health”, ejemplos de señales recientes)

```sql
SELECT event_time, intensity,
       fields->>'spread_usd' AS spread,
       fields->>'depth_bid' AS depth_bid,
       fields->>'depth_ask' AS depth_ask
FROM oraculo.slice_events
WHERE event_type = 'spread_squeeze'
  AND event_time >= now() - interval '24 hours'
ORDER BY event_time DESC
LIMIT 50;
```
- Health por hora: conteo de señales y promedio de spread en `metrics_series` para el mismo período.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Pipeline depth: `alerts_queue_depth`, `alerts_stage_duration_ms{stage="depth"}`.
- Logs incluyen `spread_usd`, depth por lado, `levels`, `hold_ms`, `latency_ms`.
- Revisar `ws_last_msg_age_seconds` si hay huecos en depth.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Consultas a `metrics_series` con filtros de tiempo/metric para usar compresión Timescale.
- `levels` altos aumentan CPU; 50 suele ser suficiente.
- `hold_ms` largo reduce spam pero requiere buffering; asegurar colas (`alerts_queue_depth`) no crecen.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Sin señales: revisar `max_spread_usd` (puede estar demasiado bajo) y precisión de `spread_usd` (tick_size correcto).
- Señales erráticas: subir `min_depth_*` y `hold_ms`; verificar que `spread_usd` no esté contaminado por latencias (lag >200 ms).
- Clock skew: revisar `ws_msg_lag_ms` y sincronización NTP.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Subir `max_spread_usd` a p50 y `hold_ms` a 0 para modo observación.
- Desactivar con `enabled=false` en reglas si sigue generando ruido.
