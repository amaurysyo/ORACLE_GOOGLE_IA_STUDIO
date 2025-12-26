# Runbook global de cierre — Oráculo

## Checklist GO/NO-GO (previo a activar)
- [ ] Pipelines de ingesta activos y con lag dentro de objetivos (<30s trades/depth; <2m OI/funding/options).
- [ ] Métricas clave (`metrics_series`) actualizadas en las últimas 5m para BTCUSDT/BTC.
- [ ] `rule_alerts` sin ráfagas anómalas en las últimas 24h (picos aislados investigados).
- [ ] Jobs Timescale (`timescaledb_information.jobs`) sin fallos en las últimas 6h.
- [ ] CAGGs críticos (`oraculo.iv_surface_1m` u otros) con bucket reciente (<5m) y `count>0`.
- [ ] Capacidad OK: sin spikes sostenidos de CPU/IO en DB ni colas de workers.
- [ ] Plan de rollback probado (toggle `enabled=false` + subir umbrales) y accesible.

## Panel mínimo
```sql
-- Qué valida: Liveness y lag de trades Binance (futuros). Criterio OK: max(event_time) dentro de 30s y lag_promedio_ms < 1000.
SELECT max(event_time) AS last_ts,
       EXTRACT(EPOCH FROM (now() - max(event_time))) AS lag_s,
       avg((meta->>'ingest_latency_ms')::float) AS lag_promedio_ms
FROM binance_futures.trades
WHERE event_time >= now() - interval '10 minutes';

-- Qué valida: Liveness y lag de depth Binance. Criterio OK: last_ts <30s y lag_promedio_ms < 1000.
SELECT max(event_time) AS last_ts,
       EXTRACT(EPOCH FROM (now() - max(event_time))) AS lag_s,
       avg((meta->>'ingest_latency_ms')::float) AS lag_promedio_ms
FROM binance_futures.depth
WHERE event_time >= now() - interval '10 minutes';

-- Qué valida: Liveness de OI + funding Binance. Criterio OK: last_ts <2m y ambos streams presentes.
SELECT 'open_interest' AS stream, max(event_time) AS last_ts, EXTRACT(EPOCH FROM (now()-max(event_time))) AS lag_s
FROM binance_futures.open_interest
UNION ALL
SELECT 'mark_funding' AS stream, max(event_time) AS last_ts, EXTRACT(EPOCH FROM (now()-max(event_time))) AS lag_s
FROM binance_futures.mark_funding;

-- Qué valida: Liveness de liquidaciones Binance. Criterio OK: last_ts <5m y count>0 si hubo mercado.
SELECT max(event_time) AS last_ts,
       EXTRACT(EPOCH FROM (now() - max(event_time))) AS lag_s,
       count(*) AS n_events
FROM binance_futures.liquidations
WHERE event_time >= now() - interval '30 minutes';

-- Qué valida: Ratios top traders Binance. Criterio OK: last_ts <15m y valores no nulos.
SELECT 'account_ratio' AS series, max(event_time) AS last_ts
FROM binance_futures.top_trader_account_ratio
UNION ALL
SELECT 'position_ratio' AS series, max(event_time) AS last_ts
FROM binance_futures.top_trader_position_ratio;

-- Qué valida: Liveness de options_ticker Deribit (IV spot). Criterio OK: last_ts <2m y n>0.
SELECT max(event_time) AS last_ts,
       EXTRACT(EPOCH FROM (now() - max(event_time))) AS lag_s,
       count(*) AS n_rows
FROM deribit.options_ticker
WHERE event_time >= now() - interval '15 minutes';

-- Qué valida: Cobertura options_instruments Deribit. Criterio OK: count>0 y updated_at dentro de 24h.
SELECT count(*) AS instruments,
       max(updated_at) AS last_update
FROM deribit.options_instruments;

-- Qué valida: Liveness IV surface Deribit. Criterio OK: max(event_time) <5m y min bucket reciente.
SELECT max(event_time) AS last_ts,
       min(event_time) FILTER (WHERE event_time >= now() - interval '1 hour') AS oldest_recent,
       count(*) FILTER (WHERE event_time >= now() - interval '10 minutes') AS rows_10m
FROM deribit.options_iv_surface;

-- Qué valida: Liveness de rule_alerts (pipeline alertas). Criterio OK: last_ts <5m y sin backlog (count razonable).
SELECT max(event_time) AS last_ts,
       EXTRACT(EPOCH FROM (now()-max(event_time))) AS lag_s,
       count(*) AS n_24h
FROM oraculo.rule_alerts
WHERE event_time >= now() - interval '24 hours';

-- Qué valida: Top N métricas recientes en metrics_series. Criterio OK: todas las métricas base con last_ts <2m.
SELECT metric, window_s, max(event_time) AS last_ts
FROM oraculo.metrics_series
WHERE event_time >= now() - interval '15 minutes'
GROUP BY metric, window_s
ORDER BY last_ts DESC
LIMIT 12;

-- Qué valida: Jobs Timescale que puedan bloquear inserts/refresh (deribit/binance_futures). Criterio OK: last_run_success=true y lag<5m.
SELECT j.job_id, j.application_name, j.hypertable_schema, j.hypertable_name,
       js.last_run_started_at, js.last_successful_finish,
       js.last_run_success, js.next_scheduled_run
FROM timescaledb_information.jobs j
JOIN timescaledb_information.job_stats js USING (job_id)
WHERE j.hypertable_schema IN ('binance_futures','deribit');

-- Qué valida: Salud de CAGGs (ej. oraculo.iv_surface_1m). Criterio OK: max(bucket) <5m y count>0.
SELECT max(bucket) AS last_bucket,
       count(*) AS buckets,
       min(bucket) AS first_bucket
FROM oraculo.iv_surface_1m
WHERE bucket >= now() - interval '2 hours';
```

## Plan de activación por entorno

### Orden recomendado — STAGING
- **Wave 1 (30–60 min)**: `slicing_aggr`, `slicing_hit`, `slicing_pass` — objetivo: validar ingest trades/depth y severidades bajas.
- **Wave 2 (30–60 min)**: `absorption`, `break_wall`, `dominance`, `depletion` — objetivo: confirmar métricas depth y basis_vel.
- **Wave 3 (60–90 min)**: `tape_pressure`, `spread_squeeze`, `spoofing` — objetivo: stress de throughput agresiones.
- **Wave 4 (90+ min)**: macros cripto `oi_spike`, `liq_cluster`, `top_traders` — objetivo: confirmar pollers REST y ratios.
- **Wave 5 (90+ min)**: derivados IV `basis_dislocation`, `skew_shock`, `gamma_flip`, `iv_spike`, `term_structure_invert` — objetivo: validar surface y gates DOC.
- **Criterios de paso por wave**:
  - 0 excepciones en logs (`alerts_runner`, `ingest_*`).
  - Lag fuente: trades/depth <30s; OI/funding/options <2m; surface <5m.
  - Tasa de eventos en rango esperado (0–5/h para macros; 1–10/h slicing/break_wall; <2/h spoofing).
  - CPU/DB sin spikes sostenidos; cola de worker estable (<1k en métricas internas).

### Orden recomendado — PRODUCCIÓN
- **Wave 1 (60+ min)**: un detector de bajo riesgo (ej. `slicing_aggr`).
- **Wave 2 (60–90 min)**: añadir 1–2 detectores de microestructura (`break_wall` o `dominance`) según resultados.
- **Wave 3 (90–120 min)**: habilitar 1 detector macro (`oi_spike` o `top_traders`) tras confirmar pollers.
- **Wave 4 (120+ min)**: añadir derivados IV (`skew_shock`/`term_structure_invert`) sólo si surface estable.
- **Criterios de rollback inmediato**:
  - Tasa de eventos >5/min no esperada.
  - Crecimiento sostenido de lag/cola del worker o `rule_alerts` sin `ts_first` avanzado.
  - Errores DB (timeout, shared memory, deadlocks) o jobs Timescale fallidos.

## Criterios de paso por detector
| Detector / familia | Tasa objetivo inicial | Señal esperada | Knobs para bajar ruido | Rollback |
| --- | --- | --- | --- | --- |
| slicing_aggr / slicing_hit / slicing_pass | 1–10 eventos/h | Rafagas de trades/refills consistentes | Subir `k_min`, `qty_min`, `retrigger_s`; exigir `require_equal=true` en hitting | `enabled=false` en YAML |
| absorption | 0–3/h | Volumen >p95 con drift ≤2 ticks | Subir `vol_btc`, `dur_s` | `enabled=false` |
| break_wall | 0–5/h | Ruptura con basis_vel alineado | Subir `basis_vel_abs_bps_s`, `dep_pct`, `hold_ms`; usar `metric_source=legacy` | `enabled=false` |
| dominance | 0–5/h | Dominancia sostenida con spread bajo | Subir `dom_pct`, `hold_ms`; set `metric_source=legacy` | `enabled=false` |
| spoofing | 0–2/h | Muros que aparecen/desaparecen rápido | Subir `wall_size_btc`, `distance_ticks`; bajar `max_cancel_ratio` | `enabled=false` |
| depletion | 0–3/h | Caída abrupta depth | Subir `pct_drop_*`, `hold_ms`; cambiar `metric_source=legacy` | `enabled=false` |
| basis (extreme/mean_revert) | 0–5/h | Basis extremo o revert | Subir `basis_extreme_*`, `vel_gate_abs`; set `metric_source=legacy` | `enabled=false` |
| tape_pressure | 1–5/h | OFI extremo en 5s | Subir `min_trades`, `min_qty_btc`, `retrigger_s` | `enabled=false` |
| spread_squeeze | 1–5/h | Compresión spread | Subir `max_spread_usd`, `hold_ms`, `retrigger_s` | `enabled=false` |
| oi_spike | 0–3/h | ΔOI% con momentum precio | Subir `oi_warn/strong_pct`, `mom_*`; ampliar `retrigger_s` | `enabled=false` |
| liq_cluster | 0–2/h | Clusters 60s sin rebound | Subir `warn/strong_usd`, `retrigger_s`; acotar `momentum_window_s` | `enabled=false` |
| top_traders | 0–2/h | Sesgo ratios account/position | Subir `acc_*`, `pos_*`; exigir `require_both=true` | `enabled=false` |
| basis_dislocation | 0–2/h | Basis+funding en tendencia | Subir `basis_*`, `vel_*`; permitir sólo con funding (`allow_emit_without_funding=false`) | `enabled=false` |
| skew_shock | 0–1/h | Shock RR25d | Subir `delta_*`, `vel_*`; activar `use_velocity_gate=true` | `enabled=false` |
| gamma_flip | 0–1/h | Cambio signo GEX estable | Subir `min_abs_net_gex`, `flip_hysteresis`, `retrigger_s`; endurecer `delta_gate`/`moneyness_abs` | `enabled=false` |
| iv_spike | 0–2/h | ΔIV + velocidad | Subir `dv_*`, `vel_*`; activar `use_velocity_gate` | `enabled=false` |
| term_structure_invert | 0–1/h | Inversión curva IV | Subir `spread_*`, `vel_*`; `use_velocity_gate=true` | `enabled=false` |

## Rollback / Safe-mode único
1) Pausar nuevas alertas: `detectors.<name>.enabled=false` en `config/rules.yaml` y recargar servicio.
2) Subir umbrales de severidad (warn/strong) y `retrigger_s` en familias con ruido.
3) Forzar métricas `metric_source=legacy` si los streams DOC presentan lag.
4) Detener waves en curso y volver al último wave estable; mantener ingest pipelines activos.
5) Registrar timestamp de rollback y motivos en canal de incidentes + issue de seguimiento.

## Cierre completado
- Documentación coherente: Sección 6 y 7 actualizadas y referenciadas desde este runbook.
- Tests de cobertura de runbooks OK (incluido smoke del archivo de cierre).
- Panel mínimo en staging: todos los criterios OK durante ≥60 minutos.
- Waves 1–2 completadas en staging sin incidentes; rollback probado.
- PR aprobado y checklist GO/NO-GO marcado como completo.
