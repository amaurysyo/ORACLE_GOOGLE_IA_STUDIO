# absorption runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta **absorción** cuando una pared de órdenes pasivas sostiene agresiones durante `dur_s` sin que el precio se mueva más de `max_price_drift_ticks`.
- No debe disparar si el volumen ejecutado < `vol_btc` o si el drift supera el límite (signo de rompimiento, no absorción).

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Trades en vivo con side y qty.
- Depth/top of book para calcular ticks de drift (`tick_size`).
- Auditoría: función `oraculo_audit.audit_absorption_exact` (ver `SQL/audit_absorption_exact.sql`) que reconstruye absorciones desde raw depth/trades.
- Lag objetivo <300 ms; monitorear `ws_msg_lag_ms` y `alerts_queue_time_latest_seconds{stream="trades"}`.

## 3) Configuración (knobs YAML + valores recomendados iniciales)

```yaml
detectors:
  absorption:
    dur_s: 10.0
    vol_btc: 450.0
    max_price_drift_ticks: 2
    tick_size: 0.1
```
- `dur_s`: ventana para acumular agresiones.
- `vol_btc`: volumen total mínimo absorbido.
- `max_price_drift_ticks`: tolerancia al desplazamiento de precio (en ticks `tick_size`).

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Métrica primaria: volumen ejecutado en ventanas `dur_s` con drift ≤ `max_price_drift_ticks`.
- Regla: warn=p95, strong=p99 de `vol_exec`.

```sql
WITH abs AS (
  SELECT *
  FROM oraculo_audit.audit_absorption_exact(
    p_instrument_id := :instrument,
    p_from := now() - interval '14 days',
    p_to := now(),
    p_dur_s := 10.0,
    p_vol_btc := 0.0,
    p_max_price_drift_ticks := 2,
    p_tick_size := 0.1
  )
  WHERE price_drift_ticks <= 2
)
SELECT
  percentile_cont(0.90) WITHIN GROUP (ORDER BY vol_exec) AS p90,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY vol_exec) AS p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY vol_exec) AS p99
FROM abs;
```
- Ajustar `vol_btc` a p95 y recalibrar cada venue. Si el drift típico es <1 tick, reducir `max_price_drift_ticks` a 1 para mejorar precisión.

## 5) Validación (queries de “health”, ejemplos de señales recientes)

```sql
SELECT event_time, side, intensity,
       fields->>'vol_exec' AS vol_exec,
       fields->>'price_drift_ticks' AS drift
FROM oraculo.slice_events
WHERE event_type = 'absorption'
  AND event_time >= now() - interval '24 hours'
ORDER BY event_time DESC
LIMIT 50;
```
- Auditoría exacta: ejecutar `SQL/audit_absorption_exact.sql` con la ventana del evento para revisar fills y drift.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Backpressure: `oraculo_alerts_queue_depth{stream="trades"}`.
- Logs incluyen `vol_exec`, `dur_s`, `price_drift_ticks`, `latency_ms` y side.
- Usar `ws_last_msg_age_seconds{stream="depth"}` para verificar que el snapshot de tick sea fresco.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Mantener consultas en `audit_absorption_exact` acotadas por `p_from/p_to` (≤2 h) para evitar scans en depth.
- Indexar trades por `event_time` y `instrument_id`; depth ya usa clave compuesta (`instrument_id,event_time,seq,side,price`).

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Si no hay eventos: revisar que `vol_btc` no esté por encima de p99 y que `tick_size` coincida con el símbolo.
- Si hay falsos positivos por drift: bajar `max_price_drift_ticks` o filtrar trades cross-venue.
- Lag: revisar `alerts_queue_time_latest_seconds` y `alerts_db_queue_depth`.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Subir `vol_btc` a p99 y `dur_s` a 5s para modo estricto.
- Desactivar la regla de absorción o setear `enabled=false`; conservar auditoría offline para análisis.
