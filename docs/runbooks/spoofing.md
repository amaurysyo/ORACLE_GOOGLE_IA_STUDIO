# spoofing runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta **spoofing** cuando aparece un muro grande cerca del best (`distance_ticks`) que surge rápido, se sostiene por `hold_min_ms` y luego se cancela sin ejecución suficiente (`min_exec_ratio`/`max_cancel_ratio`).
- No debe disparar si el muro está lejos (`top_levels_gate`/`distance_ticks`) o si se ejecuta una fracción significativa (protege contra absorción legítima).

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Depth nivelado con eventos `insert/update/delete` y secuencias.
- Trades para calcular ratios de ejecución/cancelación.
- Tablas `oraculo.depth` y `oraculo.trades`; auditoría offline se puede reconstruir con los `seq` y timestamps.
- Lag esperado <250 ms; revisar `ws_msg_lag_ms` de depth/trades y `alerts_queue_time_latest_seconds`.

## 3) Configuración (knobs YAML + valores recomendados iniciales)

```yaml
detectors:
  spoofing:
    enabled: true
    top_levels_gate: 30
    wall_size_btc: 80.0
    distance_ticks: 10
    appear_within_ms: 400
    hold_min_ms: 500
    cancel_within_ms: 1200
    min_exec_ratio: 0.05
    max_cancel_ratio: 0.85
    per_side:
      bid:
        wall_size_btc: 80.0
      ask:
        wall_size_btc: 80.0
```
- `wall_size_btc` por lado define el tamaño mínimo agregado.
- `appear_within_ms` limita muros que se forman instantáneamente.
- `min_exec_ratio` / `max_cancel_ratio` calibran la sospecha.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Métrica primaria: tamaño del muro agregado (`wall_qty`) y distancia al best (`distance_ticks`).
- Reglas: warn=p95, strong=p99 de `wall_qty` observado en ventanas recientes.

```sql
WITH walls AS (
  SELECT
    event_time,
    side,
    (fields->>'wall_qty')::numeric AS wall_qty,
    (fields->>'distance_ticks')::numeric AS dist,
    (fields->>'exec_ratio')::numeric AS exec_ratio,
    (fields->>'cancel_ratio')::numeric AS cancel_ratio
  FROM oraculo.slice_events
  WHERE event_type = 'spoofing'
    AND event_time >= now() - interval '14 days'
)
SELECT
  percentile_cont(0.90) WITHIN GROUP (ORDER BY wall_qty) AS qty_p90,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY wall_qty) AS qty_p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY wall_qty) AS qty_p99,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY dist)     AS dist_p95
FROM walls;
```
- Ajustar `wall_size_btc` a p95 y `distance_ticks` a p95 de distancias observadas para filtrar muros lejanos.
- Si hay ruido por cancelaciones legítimas, subir `min_exec_ratio` (p.ej. 0.10) o bajar `max_cancel_ratio`.

## 5) Validación (queries de “health”, ejemplos de señales recientes)

```sql
SELECT event_time, side, intensity,
       fields->>'wall_qty' AS wall_qty,
       fields->>'exec_ratio' AS exec_ratio,
       fields->>'cancel_ratio' AS cancel_ratio,
       fields->>'distance_ticks' AS dist
FROM oraculo.slice_events
WHERE event_type = 'spoofing'
  AND event_time >= now() - interval '24 hours'
ORDER BY event_time DESC
LIMIT 50;
```
- Reconstruir manualmente revisando depth/trades entre `event_time` y `event_time + 2s` para validar `appear_within_ms` y `cancel_within_ms`.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Pipeline depth/trades: `alerts_queue_depth`, `alerts_stage_duration_ms`.
- Logs incluyen `metric_source` (si se habilita DOC en el futuro), `wall_qty`, `exec_ratio`, `cancel_ratio`, `latency_ms`.
- Monitorear `ws_heartbeat_miss_total` para detectar huecos que podrían simular cancelaciones masivas.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Consultas en `slice_events` limitadas por tiempo para usar índices.
- `top_levels_gate` controla cardinalidad; mantener ≤50.
- `appear_within_ms` y `cancel_within_ms` deben alinearse con latencia WS para evitar falsos negativos.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Sin eventos: revisar que `wall_size_btc` no esté >p99 y que depth tenga `top_levels_gate` suficiente.
- Falsos positivos: incrementar `min_exec_ratio` o reducir `max_cancel_ratio`; elevar `hold_min_ms`.
- Lag negativo/clock skew: validar `ws_msg_lag_ms` y orden de `seq` en raw depth.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Subir `wall_size_btc` a p99 y `distance_ticks` a 5–10% del spread medio.
- Desactivar con `enabled=false` en reglas; mantener auditoría offline para investigación.
