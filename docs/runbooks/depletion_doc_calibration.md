# Depletion DOC: recalibración y percentiles

## Objetivo

Calibrar los umbrales de `depletion_*_doc` (Δvolumen top-`n` negativo) para R13/R14 cuando se usa la fuente `doc|auto`. Los umbrales se expresan en unidades de volumen (BTC/contratos) y se traducen a `intensity` normalizada 0..1 para compatibilidad con RulesEngine (severidad 0.40–0.60).

## Queries de percentiles

### BUY / R13 (`depletion_bid_doc`)

```sql
SELECT
  percentile_cont(0.90) WITHIN GROUP (ORDER BY (-value)) AS p90,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY (-value)) AS p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY (-value)) AS p99
FROM oraculo.metrics_series
WHERE instrument_id = :instrument_id
  AND metric = 'depletion_bid_doc'
  AND value < 0
  AND event_time >= now() - interval '7 days';
```

### SELL / R14 (`depletion_ask_doc`)

```sql
SELECT
  percentile_cont(0.90) WITHIN GROUP (ORDER BY (-value)) AS p90,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY (-value)) AS p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY (-value)) AS p99
FROM oraculo.metrics_series
WHERE instrument_id = :instrument_id
  AND metric = 'depletion_ask_doc'
  AND value < 0
  AND event_time >= now() - interval '7 days';
```

## Regla propuesta

- `dv_warn = p95`
- `dv_strong = p99`

Si hay demasiadas señales, subir a `p97/p99.5`. Si son escasas, bajar a `p90/p97`.

## Interpretación y ajuste

- `intensity ≈ 0` cerca de `dv_warn`.
- `intensity ≈ 1` cerca de `dv_strong`.
- RulesEngine mantendrá thresholds 0.40–0.60; la severidad **HIGH** comenzará aprox. en `dv_warn + 0.60 * (dv_strong - dv_warn)`.

### Notas de operación

- Verifique que `detectors.metrics_doc.depletion_doc_window_s` y `detectors.depletion.doc.window_s` coincidan.
- Mantenga `require_negative=true` salvo que se busquen reposiciones (replenishment).
- Use `clamp_abs_dv` para recortar outliers antes de calcular intensidad.
