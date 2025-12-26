# term_structure_invert runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta **inversión de la curva de IV** entre un tenor corto (`short_tenor_bucket`) y uno largo (`long_tenor_bucket`). Calcula spread y opcionalmente su velocidad.
- No debe disparar si falta alguno de los buckets (`require_both_present=true`) o si se requiere inversión positiva (`require_positive_inversion=true`) y el spread es negativo.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Superficie `options_iv_surface` para los buckets configurados.
- Eventos macro en `oraculo.slice_events` (`event_type='term_structure_invert'`).
- Lag esperado <1 s; monitorear `alerts_queue_time_latest_seconds{stream="macro"}` y freshness de surface.

## 3) Configuración (knobs YAML + valores recomendados iniciales)

```yaml
detectors:
  term_structure_invert:
    enabled: false
    poll_s: 30
    retrigger_s: 600
    underlying: "BTC"
    lookback_s: 600
    short_tenor_bucket: "0_7d"
    long_tenor_bucket: "30_90d"
    moneyness_bucket: "NA"
    spread_warn: 0.05
    spread_strong: 0.10
    vel_warn_per_s: 0.0002
    vel_strong_per_s: 0.0004
    use_velocity_gate: false
    clamp_abs_iv: 5.0
    clamp_abs_spread: 1.0
    clamp_abs_vel_per_s: 0.01
    require_both_present: true
    require_positive_inversion: true
```
- `spread_*`: diferencia IV_short - IV_long.
- `use_velocity_gate`: habilita gates de velocidad.
- Clamps para prevenir outliers.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Guía detallada en `docs/runbooks/term_structure_invert_calibration.md`.

```sql
WITH iv AS (
  SELECT event_time,
         (fields->>'iv_short')::numeric AS iv_short,
         (fields->>'iv_long')::numeric AS iv_long
  FROM oraculo.slice_events
  WHERE event_type = 'term_structure_invert'
    AND event_time >= now() - interval '90 days'
), spread AS (
  SELECT event_time, (iv_short - iv_long) AS spread
  FROM iv
)
SELECT
  percentile_cont(0.95) WITHIN GROUP (ORDER BY spread) AS spread_p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY spread) AS spread_p99
FROM spread;
```
- Velocidad:

```sql
WITH ts AS (
  SELECT event_time, value AS iv_short
  FROM oraculo.metrics_series
  WHERE metric = 'iv_0_7d'
    AND instrument_id = :underlying
    AND event_time >= now() - interval '30 days'
), tl AS (
  SELECT event_time, value AS iv_long
  FROM oraculo.metrics_series
  WHERE metric = 'iv_30_90d'
    AND instrument_id = :underlying
    AND event_time >= now() - interval '30 days'
), merged AS (
  SELECT ts.event_time,
         ts.iv_short,
         tl.iv_long
  FROM ts JOIN tl USING (event_time)
), vel AS (
  SELECT m1.event_time,
         (m1.iv_short - m1.iv_long) - (m0.iv_short - m0.iv_long)
           AS spread_delta,
         ((m1.iv_short - m1.iv_long) - (m0.iv_short - m0.iv_long)) /
           EXTRACT(EPOCH FROM (m1.event_time - m0.event_time)) AS vel
  FROM merged m1
  JOIN LATERAL (
    SELECT iv_short, iv_long, event_time FROM merged
    WHERE event_time <= m1.event_time - interval '600 seconds'
    ORDER BY event_time DESC LIMIT 1
  ) m0 ON true
)
SELECT
  percentile_cont(0.95) WITHIN GROUP (ORDER BY vel) AS vel_p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY vel) AS vel_p99
FROM vel;
```
- Regla: `spread_warn/strong`≈p95/p99; `vel_warn/strong`≈p95/p99 si se usa el gate.

## 5) Validación (queries de “health”, ejemplos de señales recientes)

```sql
SELECT event_time, intensity,
       fields->>'iv_short' AS iv_short,
       fields->>'iv_long' AS iv_long,
       fields->>'spread' AS spread,
       fields->>'source' AS source
FROM oraculo.slice_events
WHERE event_type = 'term_structure_invert'
  AND event_time >= now() - interval '30 days'
ORDER BY event_time DESC
LIMIT 50;
```
- Confirmar que ambos buckets estén presentes (`require_both_present`) y clamps no recorten valores.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Pipeline macro: `alerts_queue_depth{stream="macro"}`.
- Logs incluyen `spread`, `vel_per_s` (si aplica), clamps, `source`, `latency_ms`.
- Vigilar `ws_last_msg_age_seconds{stream="options"}`.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Filtrar `metrics_series` por metric/time; usar compresión para histórico.
- `lookback_s`=600 implica comparar puntos a 10 min; mantener `poll_s`≥30.
- Clamps reducen riesgo numérico; mantenerlos cerca de p99.5.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Sin eventos: verificar surface disponible para ambos buckets y `enabled=false` por defecto.
- Ruido: subir `spread_warn/strong` o habilitar `use_velocity_gate`; aumentar `retrigger_s`.
- Clock skew: revisar latencias surface vs ticker; auditar `ws_msg_lag_ms`.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Subir umbrales a p99 y `retrigger_s` a 1200.
- Desactivar con `enabled=false`; mantener queries de calibración para monitoreo pasivo.
