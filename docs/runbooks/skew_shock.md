# skew_shock runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta **shocks de skew 25Δ** midiendo cambios absolutos (`delta_warn/strong`) y velocidades (`vel_warn/strong`) del RR 25d en ventana `window_s`.
- No debe disparar si faltan suficientes instrumentos (`min_instruments`) o si la serie DOC no está fresca.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Superficie de IV por delta/tenor (`options_iv_surface`).
- Series persistidas en `oraculo.metrics_series` para RR 25d y derivados (si materializadas).
- Eventos macro en `oraculo.slice_events` (`event_type='skew_shock'`).
- Lag esperado <1 s; revisar `alerts_queue_time_latest_seconds{stream="macro"}` y freshness de surface.

## 3) Configuración (knobs YAML + valores recomendados iniciales)

```yaml
detectors:
  skew_shock:
    enabled: false
    poll_s: 15
    retrigger_s: 300
    underlying: "BTC"
    window_s: 300
    delta_warn: 0.010
    delta_strong: 0.020
    vel_warn_per_s: 0.00003
    vel_strong_per_s: 0.00006
    clamp_abs_rr: 1.0
    clamp_abs_delta: 0.2
    clamp_abs_vel_per_s: 0.01
    require_recent_past: true
```
- `delta_*`: cambio absoluto de RR.
- `vel_*`: derivada temporal.
- Clamps para proteger contra outliers.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Guía detallada en `docs/runbooks/skew_shock_calibration.md`.

```sql
WITH rr AS (
  SELECT event_time, value AS rr
  FROM oraculo.metrics_series
  WHERE metric = 'rr_25d'
    AND instrument_id = :underlying
    AND event_time >= now() - interval '60 days'
), delta AS (
  SELECT r1.event_time,
         (r1.rr - r0.rr) AS d_rr,
         (r1.rr - r0.rr) / EXTRACT(EPOCH FROM (r1.event_time - r0.event_time)) AS vel
  FROM rr r1
  JOIN LATERAL (
    SELECT rr, event_time FROM rr
    WHERE event_time <= r1.event_time - interval '300 seconds'
    ORDER BY event_time DESC LIMIT 1
  ) r0 ON true
)
SELECT
  percentile_cont(0.95) WITHIN GROUP (ORDER BY abs(d_rr)) AS delta_p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY abs(d_rr)) AS delta_p99,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY abs(vel))  AS vel_p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY abs(vel))  AS vel_p99
FROM delta;
```
- Regla: `delta_warn/strong`≈p95/p99; `vel_warn/strong`≈p95/p99. Ajustar clamps si la distribución tiene colas pesadas.

## 5) Validación (queries de “health”, ejemplos de señales recientes)

```sql
SELECT event_time, intensity,
       fields->>'delta_rr' AS delta_rr,
       fields->>'vel_per_s' AS vel_per_s,
       fields->>'n_instruments' AS n_instr
FROM oraculo.slice_events
WHERE event_type = 'skew_shock'
  AND event_time >= now() - interval '14 days'
ORDER BY event_time DESC
LIMIT 50;
```
- Confirmar número de instrumentos y clamps aplicados en logs.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Pipeline macro: `alerts_queue_depth{stream="macro"}`.
- Logs incluyen `delta_rr`, `vel_per_s`, `clamp_abs_rr/delta/vel`, `latency_ms`.
- Monitorizar `ws_last_msg_age_seconds{stream="options"}` para freshness de surface.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Filtrar `metrics_series` por `metric/instrument/time`; usar compresión para histórico.
- `window_s`=300 implica buscar puntos separados 5 min; asegurar índice por tiempo.
- `poll_s`=15 limita carga; evitar valores <10 sin necesidad.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Sin eventos: revisar surface y clamps (`clamp_abs_rr`) que podrían recortar datos; validar `require_recent_past`.
- Ruido: subir `delta_warn/strong` o `vel_warn/strong`; incrementar `retrigger_s`.
- Clock skew: confirmar sincronización entre surface y precios (mark/spot).

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Subir umbrales a p99 y `retrigger_s` a 900 para modo observación.
- Desactivar con `enabled=false`; mantener queries de calibración para monitoreo pasivo.
