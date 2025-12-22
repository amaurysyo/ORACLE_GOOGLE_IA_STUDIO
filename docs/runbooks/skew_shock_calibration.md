# Runbook — Calibración `skew_shock` (RR 25Δ)

## Objetivo
Ajustar los umbrales de `detectors.skew_shock` (ΔRR y velocidad) a percentiles observados en la serie `rr_25d_avg` de `oraculo.iv_surface_1m`, preservando el gating por `enabled=false` hasta completar la calibración.

## Fuente de datos
- Vista: `oraculo.iv_surface_1m`
- Campos usados: `bucket` (timestamp), `rr_25d_avg`, `bf_25d_avg`, `iv_avg`
- Granularidad: 1 minuto por `underlying` (suficiente para macro shocks)

## Consultas sugeridas
Reemplaza `$UNDERLYING`, `$WINDOW_S` y `$BUCKET_FROM` según el rango deseado.

### 1) Distribución de saltos absolutos en ventana `window_s`
```sql
WITH points AS (
    SELECT
        bucket,
        rr_25d_avg,
        LAG(rr_25d_avg) OVER (ORDER BY bucket) AS rr_prev
    FROM oraculo.iv_surface_1m
    WHERE underlying = $1
      AND bucket >= $2
)
SELECT
    percentile_cont(0.50) WITHIN GROUP (ORDER BY ABS(rr_25d_avg - rr_prev)) AS p50_abs_delta,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY ABS(rr_25d_avg - rr_prev)) AS p95_abs_delta,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY ABS(rr_25d_avg - rr_prev)) AS p99_abs_delta
FROM points
WHERE rr_prev IS NOT NULL;
-- params: $1=UNDERLYING (p.ej. 'BTC'), $2=BUCKET_FROM (timestamp)
```

### 2) Distribución de velocidad |ΔRR|/Δt
```sql
WITH points AS (
    SELECT
        bucket,
        rr_25d_avg,
        LAG(rr_25d_avg) OVER (ORDER BY bucket) AS rr_prev,
        EXTRACT(EPOCH FROM (bucket - LAG(bucket) OVER (ORDER BY bucket))) AS dt_s
    FROM oraculo.iv_surface_1m
    WHERE underlying = $1
      AND bucket >= $2
)
SELECT
    percentile_cont(0.50) WITHIN GROUP (ORDER BY ABS(rr_25d_avg - rr_prev)/NULLIF(dt_s,0)) AS p50_abs_vel,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY ABS(rr_25d_avg - rr_prev)/NULLIF(dt_s,0)) AS p95_abs_vel,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY ABS(rr_25d_avg - rr_prev)/NULLIF(dt_s,0)) AS p99_abs_vel
FROM points
WHERE rr_prev IS NOT NULL
  AND dt_s IS NOT NULL;
-- params: $1=UNDERLYING, $2=BUCKET_FROM
```

## Recomendaciones iniciales
- **Umbral warn**: usar percentil ~p95 de |ΔRR| y |vel|.
- **Umbral strong**: usar percentil ~p99 de |ΔRR| y |vel|.
- Ajustar `window_s` si se requiere mayor sensibilidad (p. ej., 180s) y recalcular percentiles.
- Mantener clamps (`clamp_abs_rr`, `clamp_abs_delta`, `clamp_abs_vel_per_s`) para filtrar outliers y datos corruptos.
- `require_recent_past=true` evita emitir si no existe un punto previo cercano a `window_s`.

## Validaciones
- Correr `python -m pytest test/test_skew_shock_rule.py` para validar lógica y retriggers.
- Confirmar que `enabled` se mantiene en `false` hasta terminar la calibración en pre-prod/QA.
