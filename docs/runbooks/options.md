# options runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Agrupa reglas R19–R22 sobre **volatilidad implícita y skew de OI** en Deribit: spikes de IV (`iv_spike`) y desequilibrio de OI (`oi_skew`).
- No debe disparar si faltan mínimos de contratos (`min_calls/min_puts`) o si la superficie usada (`surface`) no está disponible.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Superficie de IV (`options_iv_surface`) y snapshots de open interest por strike/tenor.
- Métricas derivadas almacenadas en `oraculo.metrics_series` (series de IV y OI) y auditoría en eventos de macros (`oraculo.slice_events` `event_type` iv_spike / oi_skew).
- Lag esperado <1 s; monitorear `alerts_queue_time_latest_seconds{stream="macro"}` y `ws_msg_lag_ms{stream="options"}`.

## 3) Configuración (knobs YAML + valores recomendados iniciales)

```yaml
detectors:
  options:
    iv_spike:
      window_s: 60
      up_pct: 15
      down_pct: -15
      surface: "ATM_30d"
    oi_skew:
      min_calls: 100
      min_puts: 100
      bull_ratio: 1.5
      bear_ratio: 1.5
```
- `iv_spike`: cambios relativos (%) de IV en `window_s`.
- `oi_skew`: ratios calls/puts y puts/calls mínimo.
- Ajustar `window_s` para alinear con frecuencia de recomputo de surface.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- IV spike: medir delta IV en ventanas `window_s` desde `metrics_series` o consultas a la surface.

```sql
WITH iv AS (
  SELECT event_time, value AS iv
  FROM oraculo.metrics_series
  WHERE metric = 'iv_atm_30d'
    AND instrument_id = :underlying
    AND event_time >= now() - interval '30 days'
), dv AS (
  SELECT i1.event_time,
         (i1.iv - i0.iv) / NULLIF(i0.iv,0) AS dv
  FROM iv i1
  JOIN LATERAL (
    SELECT iv FROM iv
    WHERE event_time <= i1.event_time - interval '60 seconds'
    ORDER BY event_time DESC LIMIT 1
  ) i0 ON true
)
SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY dv) AS p95,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY dv) AS p99
FROM dv;
```
- OI skew: percentiles de ratio calls/puts y puts/calls en la ventana diaria; fijar warn≈p95, strong≈p99.
- Para calibraciones más detalladas usar `docs/runbooks/iv_spike_calibration.md` y `docs/runbooks/oi_spike.md` (macro dedicada) como referencia de percentiles.

## 5) Validación (queries de “health”, ejemplos de señales recientes)

```sql
SELECT event_time, event_type, intensity,
       fields->>'metric' AS metric,
       fields->>'dv' AS dv,
       fields->>'ratio' AS ratio
FROM oraculo.slice_events
WHERE event_type IN ('iv_spike','oi_skew')
  AND event_time >= now() - interval '72 hours'
ORDER BY event_time DESC
LIMIT 50;
```
- Validar inputs consultando la surface/raw OI para la ventana del evento y confirmar mínimos `min_calls/min_puts`.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Pipeline macro: `alerts_batch_duration_seconds{stream="macro"}` y `alerts_queue_depth{stream="macro"}`.
- Logs incluyen `surface`, `window_s`, `dv`, `ratio`, `n_instruments`, `latency_ms`.
- Si hay fallback a ticker, se registra `metric_used` y `source` en fields.

## 7) Performance (índices recomendados, filtros lookback, límites)
- `metrics_series` debe filtrarse por `metric` y `event_time`; mantener retention suficiente para 30d.
- Evitar ventanas >5 min para no recalcular surfaces extensas; cachear surface por tenor.
- Clamps en código para IV/ratio evitan outliers; mantenerlos alineados a p99.5.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Sin señales: verificar `detectors.options` habilitado y disponibilidad de surface/ OI; revisar `min_calls/min_puts` no excesivos.
- Señales ruidosas: subir `up_pct`/`down_pct` o ratios; usar clamps más estrictos.
- Lag: comprobar `ws_last_msg_age_seconds{stream="options"}` y `alerts_db_queue_depth`.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Subir `up_pct`/`down_pct` a p99 y `bull_ratio`/`bear_ratio` a valores >2 para modo estricto.
- Desactivar bloque de opciones en reglas; mantener auditoría con scripts SQL para análisis post-mortem.
