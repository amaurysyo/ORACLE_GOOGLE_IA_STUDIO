# gamma_flip runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta **cambios de signo en la gamma neta (GEX)** agregada por instrumento. Emite cuando la gamma neta cruza 0 y supera `min_abs_net_gex`, aplicando hysteresis (`flip_hysteresis`) y filtros de liquidez.
- No debe disparar si el número de instrumentos < `min_instruments`, si la delta/moneyness/expiry cae fuera de los rangos o si los clamps recortan el valor a 0.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Superficie de opciones con greeks (gamma, oi, spot) por strike/tenor.
- Eventos macro en `oraculo.slice_events` (`event_type='gamma_flip'`) con payload agregado.
- Lag esperado <1 s; revisar `alerts_queue_time_latest_seconds{stream="macro"}` y freshness de surface.

## 3) Configuración (knobs YAML + valores recomendados iniciales)

```yaml
detectors:
  gamma_flip:
    enabled: false
    poll_s: 30
    retrigger_s: 900
    underlying: "BTC"
    lookback_s: 600
    expiry_max_days: 45
    moneyness_abs: 0.10
    delta_gate_enabled: true
    delta_min: 0.35
    delta_max: 0.65
    min_instruments: 30
    min_abs_net_gex: 1.0e6
    flip_hysteresis: 0.10
    clamp_spot: [1000, 1000000]
    clamp_gamma: [0, 1e3]
    clamp_oi: [0, 1e9]
    intensity_warn: 1.0e6
    intensity_strong: 5.0e6
    require_stable_samples: 2
    min_oi: 0.0
```
- Filtros de elegibilidad: expiries, moneyness, delta gates, clamps.
- `flip_hysteresis`: evita re-flips rápidos.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Métrica primaria: |gamma neta| y su cambio de signo; guía detallada en `docs/runbooks/gamma_flip_calibration.md`.

```sql
WITH gex AS (
  SELECT event_time,
         (fields->>'net_gex')::numeric AS net_gex
  FROM oraculo.slice_events
  WHERE event_type = 'gamma_flip'
    AND event_time >= now() - interval '90 days'
), abs_gex AS (
  SELECT abs(net_gex) AS abs_gex FROM gex
)
SELECT
  percentile_cont(0.95) WITHIN GROUP (ORDER BY abs_gex) AS gex_p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY abs_gex) AS gex_p99
FROM abs_gex;
```
- Regla: `intensity_warn≈p95`, `intensity_strong≈p99`. Ajustar `min_abs_net_gex` cerca de p90 para evitar flips por ruido.
- Revisar estabilidad: `require_stable_samples` debe cubrir al menos 2 polls consecutivos con el mismo signo.

## 5) Validación (queries de “health”, ejemplos de señales recientes)

```sql
SELECT event_time, intensity,
       fields->>'net_gex' AS net_gex,
       fields->>'n_instruments' AS n_instr,
       fields->>'sign' AS sign
FROM oraculo.slice_events
WHERE event_type = 'gamma_flip'
  AND event_time >= now() - interval '30 days'
ORDER BY event_time DESC
LIMIT 50;
```
- Validar que se cumplan clamps y filtros (delta/moneyness/expiry) revisando logs del evento.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Pipeline macro: `alerts_queue_depth{stream="macro"}`.
- Logs incluyen `net_gex`, `flip_hysteresis`, `min_abs_net_gex`, `n_instruments`, `latency_ms`.
- Monitorear `ws_last_msg_age_seconds{stream="options"}` para freshness de surface.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Computo proporcional al nº de instrumentos; mantener `poll_s`=30 y `min_instruments` razonable.
- Clamps de spot/gamma/oi previenen explosiones numéricas; revisar que estén cerca de p99.5.
- `lookback_s`=600 limita data a 10 min; no ampliar sin pre-agregados.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Sin eventos: revisar que `enabled=false` no esté activo y que `min_abs_net_gex` no sea superior al p99 real.
- Ruido: subir `min_abs_net_gex`, `flip_hysteresis` o `require_stable_samples`.
- Lag o datos faltantes: revisar feed de options y logs de clamps aplicados.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Subir `min_abs_net_gex` a p99 y `intensity_warn/strong` a colas extremas; aumentar `retrigger_s` a 1800.
- Desactivar con `enabled=false`; mantener consultas de calibración para supervisión sin alertas.
