# iv_spike runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta **spikes de volatilidad implícita** en buckets tenor/moneyness específicos. Calcula ΔIV en `window_s` y opcionalmente su velocidad; puede usar fallback a ticker ATM.
- No debe disparar si faltan suficientes instrumentos (`min_instruments` del ticker fallback) o si `require_positive_spike=true` y el cambio es negativo.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Superficie `options_iv_surface` (DOC) y ticker ATM de respaldo (`options_ticker`).
- Eventos en `oraculo.slice_events` (`event_type='iv_spike'`) con metadata del source.
- Lag esperado <1 s; revisar `alerts_queue_time_latest_seconds{stream="macro"}` y freshness de surface/ticker.

## 3) Configuración (knobs YAML + valores recomendados iniciales)

```yaml
detectors:
  iv_spike:
    enabled: false
    poll_s: 30
    retrigger_s: 600
    underlying: "BTC"
    tenor_bucket: "0_7d"
    moneyness_bucket: "NA"
    lookback_s: 900
    window_s: 300
    dv_warn: 0.03
    dv_strong: 0.07
    vel_warn_per_s: 0.00010
    vel_strong_per_s: 0.00025
    use_velocity_gate: false
    clamp_iv: [0.0, 5.0]
    clamp_dv: [-1.0, 1.0]
    clamp_vel: [-0.01, 0.01]
    require_positive_spike: true
    fallback_to_ticker: true
    ticker_delta_gate_enabled: true
    ticker_delta_min: 0.35
    ticker_delta_max: 0.65
    ticker_expiry_max_days: 45
    ticker_moneyness_abs: 0.10
    ticker_min_instruments: 20
```
- `dv_*`: cambios absolutos de IV (decimal).
- `use_velocity_gate`: habilita umbrales de velocidad.
- Clamps aplican antes de calcular intensidad.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Usar la guía en `docs/runbooks/iv_spike_calibration.md` para percentiles por tenor/moneyness.

```sql
WITH iv AS (
  SELECT event_time, value AS iv
  FROM oraculo.metrics_series
  WHERE metric = 'iv_0_7d'
    AND instrument_id = :underlying
    AND event_time >= now() - interval '30 days'
), dv AS (
  SELECT i1.event_time,
         (i1.iv - i0.iv) AS dv,
         (i1.iv - i0.iv) / EXTRACT(EPOCH FROM (i1.event_time - i0.event_time)) AS vel
  FROM iv i1
  JOIN LATERAL (
    SELECT iv, event_time FROM iv
    WHERE event_time <= i1.event_time - interval '300 seconds'
    ORDER BY event_time DESC LIMIT 1
  ) i0 ON true
)
SELECT
  percentile_cont(0.95) WITHIN GROUP (ORDER BY dv) AS dv_p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY dv) AS dv_p99,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY abs(vel)) AS vel_p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY abs(vel)) AS vel_p99
FROM dv;
```
- Regla: `dv_warn/strong`≈p95/p99; `vel_warn/strong`≈p95/p99 si el gate está activo. Ajustar clamps si hay colas pesadas.

## 5) Validación (queries de “health”, ejemplos de señales recientes)

```sql
SELECT event_time, intensity,
       fields->>'dv' AS dv,
       fields->>'vel_per_s' AS vel,
       fields->>'source' AS source,
       fields->>'n_instruments' AS n_instr
FROM oraculo.slice_events
WHERE event_type = 'iv_spike'
  AND event_time >= now() - interval '14 days'
ORDER BY event_time DESC
LIMIT 50;
```
- Comparar con surface/ticker en la ventana para confirmar que el clamp no recortó datos reales.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Pipeline macro: `alerts_queue_depth{stream="macro"}`.
- Logs incluyen `source` (surface/ticker), `dv`, `vel`, clamps y `latency_ms`.
- Monitorear `ws_last_msg_age_seconds{stream="options"}`.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Filtrar `metrics_series` por `metric/instrument/time`; usar compresión para históricos.
- `poll_s`=30 evita recomputos costosos; no reducir sin caching de surface.
- Clamps previenen overflow; mantenerlos ajustados a p99.5.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Sin eventos: validar `enabled` y `ticker_min_instruments`; revisar que `clamp_iv` no recorte toda la surface.
- Ruido: subir `dv_warn/strong` o habilitar `use_velocity_gate`; aumentar `retrigger_s`.
- Clock skew: revisar latencia de surface vs ticker y `ws_msg_lag_ms`.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Subir `dv_warn/strong` a colas extremas y `retrigger_s` a 1200.
- Desactivar con `enabled=false`; seguir corridas de SQL de calibración para monitoreo sin alertas.
