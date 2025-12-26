# break_wall runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta **ruptura de muro** cuando aparece depleción severa en top niveles y el muro no se rellena dentro de `refill_window_s`, confirmando que la liquidez desaparece.
- No debe disparar si la caída de depth (`dep_pct` o `dv_dep_warn`) no supera el umbral o si se observa refill >`refill_min_pct` en la ventana.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Depth consolidado con al menos `top_levels_gate` niveles.
- Métricas DOC opcionales: `depletion_bid_doc`, `depletion_ask_doc`, `refill_*` (ventana `metrics_doc.depletion_doc_window_s`).
- Auditorías en `/SQL`: `audit_breakwall_from_raw.sql`, `audit_breakwall_snapshots_from_raw.sql`, `audit_breakwall_metrics_for_slicing.sql`.
- Lag objetivo <300 ms; monitorear `ws_msg_lag_ms{stream="depth"}` y `alerts_queue_time_latest_seconds{stream="depth"}`.

## 3) Configuración (knobs YAML + valores recomendados iniciales)

```yaml
detectors:
  break_wall:
    n_min: 3
    dep_pct: 0.40
    basis_vel_abs_bps_s: 1.5
    metric_source: legacy          # legacy|doc|auto
    doc:
      window_s: 3.0
      dv_dep_warn: 20.0
      dv_refill_max: 10.0
      clamp_abs_dv: 300.0
      require_negative: true
    require_depletion: true
    top_levels_gate: 20
    tick_size: 0.1
    forbid_refill_under_pct: 0.6
    refill_window_s: 3.0
    refill_min_pct: 0.60
```
- `n_min`: repeticiones mínimas de la condición antes de alertar.
- `dep_pct`: caída relativa (legacy). En DOC usar `dv_dep_warn`/`dv_refill_max`.
- `metric_source`: elegir legacy, doc o auto.
- `forbid_refill_under_pct` / `refill_min_pct`: compuertas de refill.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Métrica primaria: `depletion_*_doc` (abs delta volumen) y `refill_*_doc` en `oraculo.metrics_series`.
- Regla base: warn=p95, strong=p99 de `-value` para depletion; usar p90/p95 para `dv_refill_max` (ver `docs/runbooks/breakwall_doc_calibration.md`).

```sql
-- Depleción DOC (lado bid)
SELECT
  percentile_cont(0.95) WITHIN GROUP (ORDER BY (-value)) AS dep_p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY (-value)) AS dep_p99
FROM oraculo.metrics_series
WHERE metric = 'depletion_bid_doc'
  AND value < 0
  AND instrument_id = :instrument
  AND event_time >= now() - interval '7 days';
```

```sql
-- Refill DOC (lado bid, misma ventana 3s)
SELECT
  percentile_cont(0.90) WITHIN GROUP (ORDER BY value) AS refill_p90,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY value) AS refill_p95
FROM oraculo.metrics_series
WHERE metric = 'refill_bid_3s'
  AND value > 0
  AND instrument_id = :instrument
  AND event_time >= now() - interval '7 days';
```
- Si se usa legacy: medir `dep_pct` con snapshots de depth top 20 (`audit_breakwall_snapshots_from_raw.sql`) y fijar warn≈p95, strong≈p99 del % drop.

## 5) Validación (queries de “health”, ejemplos de señales recientes)

```sql
SELECT event_time, side, intensity,
       fields->>'dep_dv' AS dep_dv,
       fields->>'refill_dv' AS refill_dv,
       fields->>'metric_source' AS source
FROM oraculo.slice_events
WHERE event_type = 'break_wall'
  AND event_time >= now() - interval '24 hours'
ORDER BY event_time DESC
LIMIT 50;
```
- Para un evento concreto, correr `SQL/audit_breakwall_from_raw.sql` y `SQL/audit_breakwall_snapshots_from_raw.sql` en la ventana ±5 s y revisar `expected_rule`.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Métricas DOC: `oraculo_depletion_events_total{source="doc"}`, `oraculo_depletion_doc_amount` (histogram por lado).
- Pipeline: `alerts_queue_depth{stream="depth"}`, `alerts_stage_duration_ms{stage="depth"}`.
- Logs incluyen `metric_source`, `dep_pct`/`dep_dv`, `refill_pct`/`refill_dv`, `basis_vel_abs_bps_s` aplicado y `latency_ms`.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Consultas sobre `metrics_series` deben filtrar por `metric`, `instrument_id` y rango de tiempo para usar índices Timescale.
- Mantener `window_s` alineado con `metrics_doc.depletion_doc_window_s` para evitar doble cálculo.
- `top_levels_gate` limita cardinalidad; no exceder 50 sin necesidad.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Sin señales: revisar que `require_depletion=true` tenga insumo (ver `metrics_doc.depletion_doc_window_s`) y que `metric_source=legacy` no esté recibiendo depth parcial.
- Falsos positivos: subir `dv_dep_warn` o `dep_pct`; exigir `require_negative=true`.
- Lag negativo/clock skew: verificar `ws_last_msg_age_seconds` y sincronización NTP; auditar `seq` en raw depth.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Subir `dv_dep_warn` a p99 y `refill_min_pct` a 0.8 para modo estricto.
- Desactivar solo la rama DOC poniendo `metric_source=legacy`; o deshabilitar por completo con `enabled=false` en reglas.
