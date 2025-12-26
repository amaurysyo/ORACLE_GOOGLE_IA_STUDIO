# basis_dislocation runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta **desplazamientos fuertes de basis** acompañados de velocidad alta y opcionalmente confirmados por tendencia de funding (`require_funding_confirm`).
- No debe disparar si `metric_source=doc` pero la serie DOC falta y `allow_emit_without_funding=false` bloquea el evento.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Series de basis y velocidad (`basis_bps_doc`, `basis_vel_bps_s_doc`) en `oraculo.metrics_series`.
- Funding rate y tendencia (`funding_trend_*`) en la misma tabla o vista de funding.
- Eventos macro en `oraculo.slice_events` (`event_type='basis_dislocation'`).
- Lag esperado <1 s; revisar `alerts_queue_time_latest_seconds{stream="macro"}` y freshness de funding (poll 10s).

## 3) Configuración (knobs YAML + valores recomendados iniciales)

```yaml
detectors:
  basis_dislocation:
    enabled: false
    poll_s: 10
    retrigger_s: 180
    metric_source: auto
    basis_warn_bps: 60.0
    basis_strong_bps: 120.0
    vel_warn_bps_s: 1.0
    vel_strong_bps_s: 2.0
    require_funding_confirm: true
    funding_window_s: 900
    funding_trend_warn: 0.00001
    funding_trend_strong: 0.00003
    allow_emit_without_funding: false
    clamp_abs_basis_bps: 1000.0
    clamp_abs_vel_bps_s: 50.0
```
- `metric_source`: elige entre legacy/DOC.
- Funding confirm opcional; si no hay funding, `allow_emit_without_funding` decide.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Métrica primaria: `basis_bps_doc` y `basis_vel_bps_s_doc`. Funding: `funding_trend` calculado en `funding_window_s`.
- Ver guía en `docs/runbooks/basis_dislocation_calibration.md`.

```sql
SELECT
  percentile_cont(0.95) WITHIN GROUP (ORDER BY abs(value)) AS basis_p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY abs(value)) AS basis_p99
FROM oraculo.metrics_series
WHERE metric = 'basis_bps_doc'
  AND instrument_id = :instrument
  AND event_time >= now() - interval '30 days';
```

```sql
SELECT
  percentile_cont(0.95) WITHIN GROUP (ORDER BY abs(value)) AS vel_p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY abs(value)) AS vel_p99
FROM oraculo.metrics_series
WHERE metric = 'basis_vel_bps_s_doc'
  AND instrument_id = :instrument
  AND event_time >= now() - interval '30 days';
```
- Regla: `basis_warn/strong`≈p95/p99; `vel_warn/strong`≈p95/p99. Funding warn/strong conforme a tendencia promedio en `funding_window_s`.

## 5) Validación (queries de “health”, ejemplos de señales recientes)

```sql
SELECT event_time, intensity,
       fields->>'basis_bps' AS basis,
       fields->>'basis_vel_bps_s' AS vel,
       fields->>'funding_trend' AS funding_trend,
       fields->>'metric_source' AS source
FROM oraculo.slice_events
WHERE event_type = 'basis_dislocation'
  AND event_time >= now() - interval '14 days'
ORDER BY event_time DESC
LIMIT 50;
```
- Si `allow_emit_without_funding=false`, verificar en logs el motivo de bloqueo y revisar la serie de funding en la ventana.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Pipeline macro: `alerts_queue_depth{stream="macro"}`.
- Logs incluyen `basis_bps`, `basis_vel`, `funding_trend`, `metric_source`, clamps aplicados y `latency_ms`.
- Monitorear `ws_last_msg_age_seconds{stream="funding"}`.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Filtrar `metrics_series` por `metric/instrument/time` para usar índices; `poll_s=10` limita costo.
- `clamp_abs_basis_bps/vel_bps_s` previenen outliers que inflan percentiles; mantenerlos en línea con p99.5.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Sin señales: confirmar disponibilidad de series DOC y funding; revisar `allow_emit_without_funding`.
- Falsos positivos: subir `basis_warn/vel_warn` o exigir funding (`require_funding_confirm=true`).
- Clock skew: verificar latencias en price feed vs funding.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Elevar umbrales a p99 y `retrigger_s` a 600 para modo observación.
- Desactivar con `enabled=false`; mantener queries de calibración para monitorear pasivamente.
