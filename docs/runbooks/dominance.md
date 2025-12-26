# dominance runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta **dominancia de profundidad** cuando uno de los lados concentra la mayoría de volumen disponible en los primeros `levels` del libro.
- No debe disparar si el spread supera `max_spread_usd` o si la fuente DOC no está disponible y `metric_source=doc`.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Depth consolidado a `levels` niveles.
- Métricas DOC/legacy persistidas en `oraculo.metrics_series` (`dom_bid`, `dom_ask`, `dominance_bid_doc`, `dominance_ask_doc`).
- Lag esperado <200 ms; vigilar `ws_msg_lag_ms{stream="depth"}` y `alerts_queue_time_latest_seconds{stream="depth"}`.

## 3) Configuración (knobs YAML + valores recomendados iniciales)

```yaml
detectors:
  dominance:
    enabled: true
    dom_pct: 0.80
    dom_pct_doc: 0.60
    metric_source: auto
    max_spread_usd: 2.0
    levels: 1000
    hold_ms: 1000
    retrigger_s: 30
```
- `dom_pct` (legacy) y `dom_pct_doc` (DOC) son los umbrales base.
- `metric_source`: auto intenta DOC y cae a legacy.
- `hold_ms`: duración mínima de la condición antes de alertar.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Métrica primaria: razón de volumen por lado `dom_bid`/`dom_ask` o versión DOC.

```sql
WITH dom AS (
  SELECT
    CASE WHEN metric LIKE 'dominance_%_doc' THEN 'doc' ELSE 'legacy' END AS src,
    metric,
    value
  FROM oraculo.metrics_series
  WHERE metric IN ('dom_bid','dom_ask','dominance_bid_doc','dominance_ask_doc')
    AND instrument_id = :instrument
    AND event_time >= now() - interval '7 days'
)
SELECT src, metric,
  percentile_cont(0.90) WITHIN GROUP (ORDER BY value) AS p90,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY value) AS p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY value) AS p99
FROM dom
GROUP BY src, metric;
```
- Regla: warn≈p95, strong≈p99. Ajustar `hold_ms` a p95 de duración de rachas dominantes (usar `event_time` diffs en series) para filtrar flicker.

## 5) Validación (queries de “health”, ejemplos de señales recientes)

```sql
SELECT event_time, side, intensity,
       fields->>'dom_pct' AS dom_pct,
       fields->>'metric_source' AS source
FROM oraculo.slice_events
WHERE event_type = 'dominance'
  AND event_time >= now() - interval '24 hours'
ORDER BY event_time DESC
LIMIT 50;
```
- Health por hora:

```sql
SELECT date_trunc('hour', event_time) AS h, count(*)
FROM oraculo.slice_events
WHERE event_type = 'dominance'
  AND event_time >= now() - interval '7 days'
GROUP BY 1
ORDER BY 1 DESC;
```

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- `oraculo_depletion_events_total` no aplica; usar métricas generales: `alerts_queue_depth{stream="depth"}`.
- Logs por evento incluyen `metric_source`, `levels`, `dom_pct`/`dom_pct_doc`, spread y `latency_ms`.
- Monitorear `ws_last_msg_age_seconds{stream="doc_depth"}` cuando `metric_source=doc`.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Consultas sobre `metrics_series` deben filtrar por `metric` y `instrument_id`.
- `levels` grandes aumentan CPU; en mercados ilíquidos usar 200–500.
- Mantener `hold_ms` > latencia media para evitar rebotes.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Sin señales: verificar `max_spread_usd` (puede bloquear en sesiones volátiles) y fuente DOC disponible.
- Falsos positivos: subir `dom_pct`/`dom_pct_doc` o `hold_ms`.
- Clock skew: revisar `ws_msg_lag_ms` y si hay huecos en depth (resyncs frecuentes).

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Elevar `dom_pct`/`dom_pct_doc` a p99 y `hold_ms` a 1500 ms para modo conservador.
- Cambiar `metric_source=legacy` si DOC está ruidoso; desactivar con `enabled=false` en reglas si persiste.
