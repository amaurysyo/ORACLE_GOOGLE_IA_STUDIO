# Basis dislocation — Calibración de umbrales (DOC)

Objetivo: estimar umbrales `warn/strong` para basis y velocidad (bps) y para la tendencia de funding usando percentiles históricos, siguiendo la recomendación de p95 (warn) y p99 (strong).

## Datos necesarios
- Series DOC en `oraculo.metrics_series`: `basis_bps_doc`, `basis_vel_bps_s_doc` (usar la misma `window_s` que el pipeline, típicamente 120s).
- Funding rate en `binance_futures.mark_funding` (columna `funding_rate`, `event_time`).

## Consultas sugeridas

### 1) Distribución de |basis_bps_doc|
```sql
WITH s AS (
  SELECT ABS(value) AS v
  FROM oraculo.metrics_series
  WHERE metric = 'basis_bps_doc'
    AND window_s = 120    -- ajustar si la ventana difiere
    AND instrument_id = $1
    AND event_time >= NOW() - INTERVAL '30 days'
)
SELECT
  percentile_cont(0.95) WITHIN GROUP (ORDER BY v) AS p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY v) AS p99
FROM s;
```

### 2) Distribución de |basis_vel_bps_s_doc|
```sql
WITH s AS (
  SELECT ABS(value) AS v
  FROM oraculo.metrics_series
  WHERE metric = 'basis_vel_bps_s_doc'
    AND window_s = 120
    AND instrument_id = $1
    AND event_time >= NOW() - INTERVAL '30 days'
)
SELECT
  percentile_cont(0.95) WITHIN GROUP (ORDER BY v) AS p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY v) AS p99
FROM s;
```

### 3) Distribución de |funding_trend| (derivada)
```sql
WITH fr AS (
  SELECT event_time, funding_rate
  FROM binance_futures.mark_funding
  WHERE instrument_id = $1
    AND event_time >= NOW() - INTERVAL '60 days'
  ORDER BY event_time
), deltas AS (
  SELECT
    (fr2.funding_rate - fr1.funding_rate) / GREATEST(EXTRACT(EPOCH FROM (fr2.event_time - fr1.event_time)), 1) AS funding_trend
  FROM fr fr1
  JOIN fr fr2
    ON fr2.event_time >= fr1.event_time + ($2::text || ' seconds')::interval   -- $2 = funding_window_s
   AND fr2.event_time = (
        SELECT fr3.event_time
        FROM fr fr3
        WHERE fr3.event_time >= fr1.event_time + ($2::text || ' seconds')::interval
        ORDER BY fr3.event_time
        LIMIT 1
   )
)
SELECT
  percentile_cont(0.95) WITHIN GROUP (ORDER BY ABS(funding_trend)) AS p95_abs,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY ABS(funding_trend)) AS p99_abs
FROM deltas;
```

> Recomendación: fijar `basis_warn_bps`/`basis_strong_bps`, `vel_warn_bps_s`/`vel_strong_bps_s` y `funding_trend_warn`/`funding_trend_strong` en p95/p99 de las distribuciones absolutas. Documentar fecha y consultas usadas en el PR de calibración.

## Notas operativas
- Mantener `detectors.basis_dislocation.enabled=false` hasta completar calibración y validación en dashboards.
- Registrar en auditoría los valores actuales de `basis_bps_doc`, `basis_vel_bps_s_doc`, métrica usada (doc vs legacy) y `funding_trend` para comparar contra percentiles propuestos.
- Si no hay funding suficiente en la ventana (`funding_window_s`), habilitar temporalmente `allow_emit_without_funding=true` sólo para pruebas controladas; mantener en `false` en producción.
