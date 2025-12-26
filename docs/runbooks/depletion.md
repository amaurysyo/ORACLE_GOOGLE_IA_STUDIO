# depletion runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta **depleción abrupta de liquidez** en un lado del libro (caída relativa o absoluta en DOC) dentro de una ventana corta (`window_s`).
- No debe disparar si la caída es positiva (refill) o si el spread es amplio y la métrica proviene de una fuente degradada.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Depth consolidado hasta `n_levels`.
- Métricas DOC y legacy persistidas en `oraculo.metrics_series`: `depletion_bid_doc`, `depletion_ask_doc`, `dep_bid`, `dep_ask`.
- Lag objetivo <200 ms; revisar `ws_msg_lag_ms{stream="depth"}` y `alerts_queue_time_latest_seconds{stream="depth"}`.

## 3) Configuración (knobs YAML + valores recomendados iniciales)

```yaml
detectors:
  depletion:
    enabled: true
    window_s: 2.0
    n_levels: 50
    pct_drop_bid: 0.35
    pct_drop_ask: 0.35
    pct_drop: 0.35
    hold_ms: 800
    retrigger_s: 30
    metric_source: legacy
    doc:
      window_s: 3
      dv_warn: 10.0
      dv_strong: 30.0
      clamp_abs_dv: 200.0
      severity_mode: linear
      require_negative: true
```
- `metric_source`: `legacy|doc|auto` decide la serie usada.
- `pct_drop_*` aplican a la caída porcentual; `dv_*` a delta volumen DOC.
- `hold_ms` suaviza flicker.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Para DOC usar `docs/runbooks/depletion_doc_calibration.md` (percentiles de `-value`).

```sql
-- Ejemplo lado bid DOC
SELECT
  percentile_cont(0.90) WITHIN GROUP (ORDER BY (-value)) AS p90,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY (-value)) AS p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY (-value)) AS p99
FROM oraculo.metrics_series
WHERE metric = 'depletion_bid_doc'
  AND value < 0
  AND instrument_id = :instrument
  AND event_time >= now() - interval '7 days';
```
- Regla: `dv_warn=p95`, `dv_strong=p99`. Para legacy usar % drop calculado desde snapshots top `n_levels` (p95/p99) y alinear con `pct_drop_{side}`.
- Ajustar `hold_ms` a p95 de duración de caídas observadas para evitar ruido.

## 5) Validación (queries de “health”, ejemplos de señales recientes)

```sql
SELECT event_time, side, intensity,
       fields->>'metric_source' AS source,
       fields->>'drop_pct' AS drop_pct,
       fields->>'drop_dv' AS drop_dv
FROM oraculo.slice_events
WHERE event_type = 'depletion'
  AND event_time >= now() - interval '24 hours'
ORDER BY event_time DESC
LIMIT 50;
```
- Para verificar DOC, consultar `metrics_series` en la ventana del evento y confirmar que `value` < `-dv_warn`.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Métrica dedicada: `oraculo_depletion_events_total{source=...,side=...}` y `oraculo_depletion_doc_amount` (histogram de magnitudes).
- Pipeline depth: `alerts_queue_depth{stream="depth"}`.
- Logs incluyen `metric_source`, `drop_pct`, `drop_dv`, `hold_ms`, `latency_ms`.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Consultas Timescale sobre `metrics_series` deben limitar tiempo y `metric`.
- `n_levels` controla costo; mantener en 50–200 según venue.
- `clamp_abs_dv` previene outliers que podrían inflar percentiles.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Sin señales: revisar `metric_source` (legacy puede fallar si depth parcial) y `window_s` alineado con `metrics_doc.depletion_doc_window_s`.
- Falsos positivos: subir `dv_warn`/`pct_drop` o `hold_ms`; verificar que `require_negative=true` siga activo.
- Clock skew: confirmar `ws_last_msg_age_seconds` y secuencia `seq` en depth.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Subir `dv_warn` a p99 y `pct_drop` a 0.50 para modo conservador.
- Cambiar `metric_source=legacy` si DOC está ruidoso o viceversa; desactivar totalmente con `enabled=false`.
