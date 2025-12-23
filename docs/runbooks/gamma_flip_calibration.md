# Gamma flip (DOC-R21) — Runbook de calibración

## Objetivo
Calibrar umbrales `intensity_warn`/`intensity_strong` y gates (`min_abs_net_gex`, `min_instruments`) del detector `gamma_flip` (R36) usando la métrica proxy:

```
gex_i = gamma * open_interest * spot^2
net_gex = Σ gex_i * sign(call=+1, put=−1)
```

Es un **proxy de GEX**, no un valor USD exacto; la calibración debe ser relativa (percentiles históricos).

## Datos y filtros recomendados
- lookback_s: 300–900s.
- expiries: `[now, now + 45d]` (usa `expiry_max_days` del YAML).
- Moneyness ATM: `strike ∈ [spot*(1−0.10), spot*(1+0.10)]` (`moneyness_abs`).  
- Gate delta opcional: `abs(delta) BETWEEN 0.35 AND 0.65`.
- Opcional `min_oi` para limpiar ruido.
- Clamps de seguridad: `spot∈[1000,1_000_000]`, `gamma∈[0,1e3]`, `oi∈[0,1e9]`.

## Query base (net_gex por minuto, últimas 48h)
Limita el horizonte (24–72h) para evitar scans grandes; conserva los filtros ATM/expiry/delta.

```sql
WITH spot AS (
    SELECT underlying_price
    FROM deribit.options_ticker
    WHERE event_time >= now() - interval '10 minutes'
    ORDER BY event_time DESC
    LIMIT 1
), bucketed AS (
    SELECT
        date_trunc('minute', t.event_time) AS bucket_minute,
        COUNT(*) AS n_rows,
        SUM(
            LEAST(GREATEST(t.gamma, 0), 1e3)
            * LEAST(GREATEST(t.open_interest, 0), 1e9)
            * POWER(LEAST(GREATEST(s.underlying_price, 1000), 1e6), 2)
            * CASE WHEN i.option_type = 'call' THEN 1 ELSE -1 END
        ) AS net_gex
    FROM deribit.options_ticker t
    JOIN deribit.options_instruments i ON i.instrument_id = t.instrument_id
    JOIN spot s ON TRUE
    WHERE i.underlying = 'BTC'
      AND t.event_time >= now() - interval '48 hours'
      AND i.expiration BETWEEN now() AND now() + interval '45 days'
      AND i.strike BETWEEN s.underlying_price * (1 - 0.10) AND s.underlying_price * (1 + 0.10)
      AND t.open_interest IS NOT NULL AND t.gamma IS NOT NULL
      AND abs(t.delta) BETWEEN 0.35 AND 0.65
    GROUP BY 1
)
SELECT *
FROM bucketed
ORDER BY bucket_minute DESC
LIMIT 500;
```

## Percentiles sugeridos
Usa los buckets anteriores para fijar umbrales relativos. Ejemplo sobre 72h:

```sql
WITH data AS (
    -- Reutiliza el CTE bucketed anterior (o materialízalo) filtrando a 72h
    SELECT * FROM bucketed
)
SELECT
    percentile_cont(0.95) WITHIN GROUP (ORDER BY abs(net_gex)) AS p95_abs_net_gex,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY abs(net_gex)) AS p99_abs_net_gex
FROM data;
```

- Recomendar `intensity_warn ≈ p95_abs_net_gex`, `intensity_strong ≈ p99_abs_net_gex`.
- Ajustar `min_abs_net_gex` al percentil bajo que aún sea estable (ej., p80) y `min_instruments` al bucket mínimo con señal fiable.

## Notas operativas
- Mantén `enabled=false` hasta completar calibración y validar que el flip sólo dispara tras cambios de signo persistentes (ver `require_stable_samples`, `flip_hysteresis`, `retrigger_s`).
- Documenta siempre la versión de filtros usados (lookback, moneyness, delta gate, `min_oi`) junto con los percentiles calculados.
- El proxy no está en USD/contrato; usa sólo comparativos relativos e inspecciona `call_gex`/`put_gex` por separado si se requiere diagnóstico adicional.
