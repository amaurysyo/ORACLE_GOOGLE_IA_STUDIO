# tape_pressure runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta **presión agresiva en la cinta** midiendo Order Flow Imbalance (OFI) y volumen de trades en una ventana corta (`window_s`).
- No debe disparar si no se cumplen mínimos de trades (`min_trades`), volumen (`min_qty_btc`) o si el spread supera `max_spread_usd`.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Stream de trades con side y qty.
- Serie de precios (wmid/spread) para el gate de `max_spread_usd`.
- Persistencia en `oraculo.slice_events` (`event_type='tape_pressure'`) y series de OFI en `oraculo.metrics_series` (`ofi_up/down` si están materializadas por ventana de 1s).
- Lag esperado <200 ms; vigilar `ws_msg_lag_ms{stream="trades"}` y `alerts_queue_time_latest_seconds{stream="trades"}`.

## 3) Configuración (knobs YAML + valores recomendados iniciales)

```yaml
detectors:
  tape_pressure:
    enabled: true
    window_s: 5.0
    min_trades: 10
    min_qty_btc: 30.0
    ofi_up: 0.65
    ofi_down: -0.65
    max_spread_usd: 2.0
    hold_ms: 500
    retrigger_s: 20
```
- OFI = (BUY_VOL − SELL_VOL)/(BUY_VOL + SELL_VOL) sobre la ventana.
- `hold_ms` evita flicker; `retrigger_s` evita spam.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Métrica primaria: OFI y volumen en ventanas `window_s`.

```sql
WITH trades AS (
  SELECT
    date_trunc('second', event_time) AS bucket,
    SUM(CASE WHEN side='buy' THEN qty ELSE 0 END) AS buy_qty,
    SUM(CASE WHEN side='sell' THEN qty ELSE 0 END) AS sell_qty,
    COUNT(*) AS n_trades
  FROM binance_futures.trades
  WHERE instrument_id = :instrument
    AND event_time >= now() - interval '7 days'
  GROUP BY 1
), agg AS (
  SELECT bucket,
         (buy_qty - sell_qty) / NULLIF(buy_qty + sell_qty,0) AS ofi,
         buy_qty + sell_qty AS qty,
         n_trades
  FROM trades
)
SELECT
  percentile_cont(0.90) WITHIN GROUP (ORDER BY ofi) AS ofi_p90,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY ofi) AS ofi_p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY ofi) AS ofi_p99,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY qty) AS qty_p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY qty) AS qty_p99
FROM agg
WHERE n_trades >= 10;
```
- Regla: setear `ofi_up`/`ofi_down` a p95/p99 y `min_qty_btc` a p95 de qty. Ajustar `hold_ms` a p95 de duración de rachas OFI.

## 5) Validación (queries de “health”, ejemplos de señales recientes)

```sql
SELECT event_time, side, intensity,
       fields->>'ofi' AS ofi,
       fields->>'qty' AS qty,
       fields->>'n_trades' AS n_trades
FROM oraculo.slice_events
WHERE event_type = 'tape_pressure'
  AND event_time >= now() - interval '24 hours'
ORDER BY event_time DESC
LIMIT 50;
```
- Verificar que `spread_usd` < `max_spread_usd` en la ventana usando `metrics_series` (`spread_usd` con `window_s=1`).

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- `alerts_queue_depth{stream="trades"}`, `alerts_stage_duration_ms{stage="trades"}`.
- Logs incluyen `ofi`, `qty`, `n_trades`, `hold_ms`, `latency_ms`.
- Si OFI viene de una serie agregada, revisar `alerts_stage_rows_total{stage="cpu_ofi"}`.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Indexar trades por `event_time` e `instrument_id`; en Timescale usar compresión para ventanas históricas.
- Mantener `window_s` en ≤10s; ventanas mayores requieren agregados previos.
- `min_trades` filtra ruido y reduce cómputo.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Sin eventos: revisar ingesta de trades, que `n_trades` alcance `min_trades` y que `max_spread_usd` no bloquee.
- Ruido: subir `ofi_up`/`ofi_down` o `min_qty_btc`; incrementar `hold_ms`.
- Lag negativo: validar `ws_last_msg_age_seconds` y clocks.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Subir `min_qty_btc` a p99 y `ofi_up/down` a ±0.8 para reducir sensibilidad.
- Desactivar con `enabled=false` en reglas manteniendo la ingesta para análisis.
