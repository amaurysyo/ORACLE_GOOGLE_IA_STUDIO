# basis runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta desviaciones de **basis** (mark/index vs spot) y su velocidad para reglas R15–R18: extremos y mean-revert.
- No debe disparar si `metric_source=doc` pero la serie DOC está ausente; el modo `auto` cae a legacy para preservar cobertura.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Series de precios mark/index y spot (wmid) por instrumento.
- Series persistidas en `oraculo.metrics_series`: `basis_bps`, `basis_bps_doc`, `basis_vel_bps_s_doc`, `basis_accel_bps_s2_doc`.
- Lag esperado <300 ms; monitorear `ws_msg_lag_ms{stream="price"}` y `alerts_queue_time_latest_seconds{stream="doc_depth"}`.

## 3) Configuración (knobs YAML + valores recomendados iniciales)

```yaml
detectors:
  basis:
    metric_source: auto
    doc_sign_mode: legacy
    extreme:
      pos_bps: 100
      neg_bps: -100
    mean_revert:
      gate_abs_bps: 25
      vel_gate_abs: 1.5
      retrigger_s: 60
```
- `doc_sign_mode`: invierte signo para mantener semántica legacy.
- Umbrales `extreme` para contango/backwardation; `mean_revert` usa gates de basis y velocidad.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Métrica primaria: `basis_bps_doc` (o legacy) y `basis_vel_bps_s_doc`.

```sql
WITH b AS (
  SELECT value
  FROM oraculo.metrics_series
  WHERE metric = 'basis_bps_doc'
    AND instrument_id = :instrument
    AND event_time >= now() - interval '14 days'
)
SELECT
  percentile_cont(0.90) WITHIN GROUP (ORDER BY value) AS p90,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY value) AS p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY value) AS p99
FROM b;
```

```sql
-- Velocidad DOC
SELECT
  percentile_cont(0.95) WITHIN GROUP (ORDER BY abs(value)) AS vel_p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY abs(value)) AS vel_p99
FROM oraculo.metrics_series
WHERE metric = 'basis_vel_bps_s_doc'
  AND instrument_id = :instrument
  AND event_time >= now() - interval '14 days';
```
- Regla: usar p95/p99 como warn/strong para `pos_bps`/`neg_bps`. Para mean-revert, fijar `gate_abs_bps`≈p90 y `vel_gate_abs`≈p95 de velocidad.

## 5) Validación (queries de “health”, ejemplos de señales recientes)

```sql
SELECT event_time, intensity,
       fields->>'basis_bps' AS basis,
       fields->>'basis_vel_bps_s' AS vel,
       fields->>'metric_source' AS source
FROM oraculo.slice_events
WHERE event_type = 'basis'
  AND event_time >= now() - interval '48 hours'
ORDER BY event_time DESC
LIMIT 50;
```
- Health por hora: conteo de señales R15–R18 y comparación con `basis_bps_doc`/`basis_vel_bps_s_doc` en `metrics_series`.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Pipeline macro: `alerts_queue_depth{stream="macro"}`, `alerts_batch_duration_seconds{stream="macro"}`.
- Logs incluyen `metric_source`, `doc_sign_mode`, `basis_bps`, `basis_vel_bps_s`, `latency_ms`.
- Revisar `ws_last_msg_age_seconds{stream="doc_depth"}` para fuente DOC.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Consultas `metrics_series` deben filtrar por `metric`/`instrument_id`/tiempo.
- Mantener `basis_doc_window_s` alineado con YAML para no recalcular múltiple veces.
- Clamps en código (`clamp_abs_basis_bps`, `clamp_abs_vel_bps_s`) evitan outliers; usarlos antes de recalibrar.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Sin señales: validar fuente `metric_source` y disponibilidad de mark/index; revisar `doc_sign_mode` si semántica invertida.
- Falsos positivos: subir `pos_bps`/`neg_bps` o `vel_gate_abs`; considerar `metric_source=doc` si legacy es ruidoso.
- Lag/clock skew: revisar `ws_msg_lag_ms` y `alerts_db_queue_depth`.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Subir umbrales a p99 y `retrigger_s` a 120 para reducir spam.
- Cambiar `metric_source=legacy` si DOC falla; desactivar reglas relacionadas si persiste.
