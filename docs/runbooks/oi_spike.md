# oi_spike runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta **spikes de open interest (OI)** combinados con momentum de precio. Emite eventos buy/sell según signo del cambio de OI y dirección de precio (`require_same_dir`).
- No debe disparar si el delta de OI no supera `oi_warn_pct/oi_strong_pct`, si el momentum USD no alcanza los mínimos configurados o si la fuente DOC está ausente y el delta se calcula con fallback.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Serie de OI por instrumento (`open_interest` o `oi_delta_pct_doc`) en `oraculo.metrics_series`.
- Precio mid (`wmid`) para momentum y validación de signo.
- Eventos macro en `oraculo.slice_events` (`event_type='oi_spike'`).
- Lag esperado <1 s; monitorear `alerts_queue_time_latest_seconds{stream="macro"}` y `ws_msg_lag_ms{stream="oi"}`.

## 3) Configuración (knobs YAML + valores recomendados iniciales)

```yaml
detectors:
  oi_spike:
    enabled: false
    poll_s: 5
    retrigger_s: 60
    oi_window_s: 120
    momentum_window_s: 60
    oi_warn_pct: 0.80
    oi_strong_pct: 1.50
    mom_warn_usd: 8.0
    mom_strong_usd: 20.0
    require_same_dir: true
```
- `oi_window_s`: lookback para delta OI.
- `momentum_window_s`: ventana para Δprecio.
- `require_same_dir`: exige que el precio acompañe el cambio de OI.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Métrica primaria: delta porcentual de OI en `oi_window_s` y movimiento de precio en USD.

```sql
WITH oi AS (
  SELECT event_time, value AS oi
  FROM oraculo.metrics_series
  WHERE metric = 'oi_delta_pct_doc'
    AND instrument_id = :instrument
    AND event_time >= now() - interval '30 days'
), delta AS (
  SELECT o1.event_time,
         (o1.oi - o0.oi) / NULLIF(o0.oi,0) AS oi_pct
  FROM oi o1
  JOIN LATERAL (
    SELECT oi FROM oi
    WHERE event_time <= o1.event_time - interval '120 seconds'
    ORDER BY event_time DESC LIMIT 1
  ) o0 ON true
)
SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY oi_pct) AS p95,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY oi_pct) AS p99
FROM delta;
```
- Para precio:

```sql
WITH px AS (
  SELECT event_time, value AS wmid
  FROM oraculo.metrics_series
  WHERE metric = 'wmid'
    AND instrument_id = :instrument
    AND event_time >= now() - interval '30 days'
), mov AS (
  SELECT p1.event_time, abs(p1.wmid - p0.wmid) AS move_usd
  FROM px p1
  JOIN LATERAL (
    SELECT wmid FROM px
    WHERE event_time <= p1.event_time - interval '60 seconds'
    ORDER BY event_time DESC LIMIT 1
  ) p0 ON true
)
SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY move_usd) AS p95,
       percentile_cont(0.99) WITHIN GROUP (ORDER BY move_usd) AS p99
FROM mov;
```
- Regla: `oi_warn_pct≈p95`, `oi_strong_pct≈p99`; `mom_warn_usd/mom_strong_usd` en p95/p99 de movimiento. Ajustar `require_same_dir=false` para pruebas.

## 5) Validación (queries de “health”, ejemplos de señales recientes)

```sql
SELECT event_time, side, intensity,
       fields->>'oi_delta_pct' AS oi_delta_pct,
       fields->>'price_move_usd' AS price_move_usd,
       fields->>'metric_used_oi' AS metric_used
FROM oraculo.slice_events
WHERE event_type = 'oi_spike'
  AND event_time >= now() - interval '7 days'
ORDER BY event_time DESC
LIMIT 50;
```
- Comparar con `metrics_series` en la ventana exacta para asegurar que el delta y el movimiento cumplen umbrales.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Pipeline macro: `alerts_queue_depth{stream="macro"}`.
- Logs incluyen `metric_used_oi`, `oi_window_s`, `momentum_window_s`, `oi_delta_pct`, `latency_ms`.
- Revisar `ws_last_msg_age_seconds{stream="oi"}` cuando haya huecos.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Consultas a `metrics_series` deben filtrar por `metric` y rango de tiempo.
- `poll_s`=5 limita costo; evitar `oi_window_s` > 5 min sin pre-agregados.
- `retrigger_s` controla spam en regímenes de alto OI.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Sin eventos: revisar disponibilidad de OI DOC o fallback; confirmar que `oi_warn_pct` no esté por encima del p99 real.
- Señales ruidosas: subir `oi_warn_pct`, `mom_warn_usd` o habilitar `require_same_dir`.
- Clock skew: validar `ws_msg_lag_ms` y sincronización entre streams de OI y precio.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Elevar umbrales a p99 y `retrigger_s` a 180 para modo estricto.
- Desactivar con `enabled=false`; mantener monitoreo pasivo con SQL de calibración.
