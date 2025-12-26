# liq_cluster runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta **clusters de liquidaciones** grandes en ventanas `window_s`, usando USD si está disponible (`use_usd=true`) y confirmando con movimiento de precio (`min_move_usd` y rebote máximo `max_rebound_usd`).
- No debe disparar si el cluster no supera `warn_usd/strong_usd` o si el movimiento de precio rebota más que `max_rebound_usd`.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Tabla de liquidaciones (`binance_futures.liquidations` o equivalente) con `quote_qty_usd`/`qty` y side.
- Serie de precio `wmid` en `oraculo.metrics_series` (window_s=1) para confirmar movimiento.
- Eventos persistidos en `oraculo.slice_events` (`event_type='liq_cluster'`).
- Lag esperado <1 s; revisar `alerts_queue_time_latest_seconds{stream="macro"}` y `ws_last_msg_age_seconds{stream="liquidations"}`.

## 3) Configuración (knobs YAML + valores recomendados iniciales)

```yaml
detectors:
  liq_cluster:
    enabled: false
    poll_s: 5
    window_s: 60
    retrigger_s: 90
    warn_usd: 1_000_000
    strong_usd: 3_000_000
    confirm_s: 10
    momentum_window_s: 30
    min_move_usd: 25.0
    max_rebound_usd: 10.0
    use_usd: true
    clamp_usd: 50_000_000
```
- `warn_usd/strong_usd`: umbrales principales de cluster.
- `confirm_s` y `momentum_window_s`: validan ausencia de rebote.
- `retrigger_s`: evita spam en periodos con muchas liquidaciones.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Usar `docs/runbooks/liq_cluster_calibration.md` para metodología completa.
- Percentiles de volumen liquidado por ventana 60s (USD) y del movimiento de precio:

```sql
WITH win AS (
  SELECT
    date_trunc('minute', event_time) AS bucket,
    side,
    COALESCE(SUM(quote_qty_usd), SUM(qty)) AS vol_usd
  FROM binance_futures.liquidations
  WHERE instrument_id = :instrument
    AND event_time >= now() - interval '14 days'
  GROUP BY 1, 2
)
SELECT side,
  percentile_cont(0.90) WITHIN GROUP (ORDER BY vol_usd) AS p90,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY vol_usd) AS p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY vol_usd) AS p99
FROM win
GROUP BY side;
```
- Regla: `warn_usd≈p95`, `strong_usd≈p99`. Para `min_move_usd` usar percentiles de |Δwmid| en 30s (query en el doc de calibración).

## 5) Validación (queries de “health”, ejemplos de señales recientes)

```sql
SELECT event_time, side, intensity,
       fields->>'cluster_usd' AS cluster_usd,
       fields->>'n_events' AS n_events,
       fields->>'price_move_usd' AS price_move_usd
FROM oraculo.slice_events
WHERE event_type = 'liq_cluster'
  AND event_time >= now() - interval '7 days'
ORDER BY event_time DESC
LIMIT 50;
```
- Confirmar movimiento de precio con `metrics_series` (`wmid`) en la ventana `[event_time, event_time + momentum_window_s]`.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Pipeline macro: `alerts_queue_depth{stream="macro"}`, `alerts_batch_duration_seconds{stream="macro"}`.
- Logs incluyen `cluster_usd`, `use_usd`, `min_move_usd`, `max_rebound_usd`, `latency_ms`, `instrument_id`.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Indexar liquidaciones por `instrument_id,event_time`; considerar compresión Timescale.
- `clamp_usd` evita outliers que distorsionan percentiles.
- Mantener `window_s` ≤60s para evitar scans grandes; `poll_s`=5s limita costo.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Sin señales: verificar feed de liquidaciones y que `use_usd` sea true cuando `quote_qty_usd` existe.
- Ruido: subir `warn_usd/strong_usd`, `min_move_usd`, o aumentar `confirm_s`.
- Clock skew: revisar `ws_last_msg_age_seconds` y latencia de liquidaciones.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Subir `warn_usd` a p99 y `min_move_usd` a p90 de |Δwmid| para modo estricto.
- Desactivar con `enabled=false`; mantener consultas SQL de calibración para monitoreo pasivo.
