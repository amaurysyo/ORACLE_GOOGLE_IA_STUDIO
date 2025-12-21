# Runbook: Calibración de `liq_cluster`

## 1) Percentiles de volumen en ventana 60s (USD)

Usar `quote_qty_usd` si está disponible; si no, emplear `qty`.

```sql
-- Percentiles P90/P95/P99 de volumen liquidado por ventana 60s
WITH win AS (
  SELECT
    date_trunc('minute', event_time) AS bucket,
    side,
    COALESCE(SUM(quote_qty_usd), SUM(qty)) AS vol_usd
  FROM binance_futures.liquidations
  WHERE instrument_id = $1
    AND event_time >= now() - interval '14 days'
  GROUP BY 1, 2
)
SELECT
  side,
  percentile_cont(0.90) WITHIN GROUP (ORDER BY vol_usd) AS p90,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY vol_usd) AS p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY vol_usd) AS p99
FROM win
GROUP BY side;
```

## 2) Recomendación de umbrales

- `warn_usd` ≈ P95 de la serie anterior.
- `strong_usd` ≈ P99 de la serie anterior.

## 3) Guía para `min_move_usd` y `max_rebound_usd`

Calcular percentiles de |Δwmid| en 30s para el instrumento:

```sql
WITH ticks AS (
  SELECT
    event_time,
    value AS wmid
  FROM oraculo.metrics_series
  WHERE metric = 'wmid'
    AND window_s = 1
    AND instrument_id = $1
    AND event_time >= now() - interval '7 days'
),
delta AS (
  SELECT
    t1.event_time,
    abs(t1.wmid - t0.wmid) AS delta_wmid
  FROM ticks t1
  JOIN LATERAL (
    SELECT wmid
    FROM ticks t0
    WHERE t0.event_time <= t1.event_time - interval '30 seconds'
    ORDER BY t0.event_time DESC
    LIMIT 1
  ) t0 ON true
)
SELECT
  percentile_cont(0.50) WITHIN GROUP (ORDER BY delta_wmid) AS p50,
  percentile_cont(0.75) WITHIN GROUP (ORDER BY delta_wmid) AS p75,
  percentile_cont(0.25) WITHIN GROUP (ORDER BY delta_wmid) AS p25
FROM delta;
```

- Sugerencia: `min_move_usd` ≈ P75.
- `max_rebound_usd` ≈ P25 (tolerancia al rebote contra la dirección).

## 4) Notas de régimen

- **Alta volatilidad:** esperar colas más pesadas; considerar subir `warn_usd`/`strong_usd` y `min_move_usd`.
- **Mercado lateral:** umbrales más bajos pueden capturar clusters moderados; monitorear ratio de falsos positivos.
- Revisar periódicamente (semanal/mensual) para adaptar a cambios de liquidez y horario.
