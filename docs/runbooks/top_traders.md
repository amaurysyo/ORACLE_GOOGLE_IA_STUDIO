# top_traders runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta **sesgo extremo de las cuentas top** midiendo ratios de cuentas/posiciones largas vs cortas y escogiendo la política (`choose_by`) configurada.
- No debe disparar si los mínimos de cuentas/posiciones no se cumplen o si el detector está deshabilitado (`enabled=false`, default).

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Fuentes de posición y account-level exposure (feed de exchange o agregados internos).
- Eventos macro en `oraculo.slice_events` (`event_type='top_traders'`) con payload de ratios.
- Lag esperado <1 s; revisar `alerts_queue_time_latest_seconds{stream="macro"}` y freshness del feed de posiciones.

## 3) Configuración (knobs YAML + valores recomendados iniciales)

```yaml
detectors:
  top_traders:
    enabled: false
    poll_s: 15
    retrigger_s: 300
    acc_warn: 0.60
    acc_strong: 0.70
    pos_warn: 0.60
    pos_strong: 0.70
    require_both: false
    choose_by: "max_score"
```
- `acc_*` ratios por cuentas; `pos_*` por tamaño de posición.
- `require_both`: exige que ambas métricas superen umbral.
- `choose_by`: selecciona criterio de severidad (máx score vs por métrica).

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Métrica primaria: ratio de cuentas y de posiciones para las top N cuentas (según implementación).
- Regla: warn=p95, strong=p99 de cada ratio calculado en una ventana de 30 días.

```sql
WITH ratios AS (
  SELECT event_time,
         (fields->>'account_ratio')::numeric AS acc_ratio,
         (fields->>'position_ratio')::numeric AS pos_ratio
  FROM oraculo.slice_events
  WHERE event_type = 'top_traders'
    AND event_time >= now() - interval '30 days'
)
SELECT
  percentile_cont(0.95) WITHIN GROUP (ORDER BY acc_ratio) AS acc_p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY acc_ratio) AS acc_p99,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY pos_ratio) AS pos_p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY pos_ratio) AS pos_p99
FROM ratios;
```
- Para guía más completa ver `docs/runbooks/top_traders_calibration.md`.
- Ajustar `require_both=true` si se quiere mayor precisión; subir `retrigger_s` en mercados con rotación rápida.

## 5) Validación (queries de “health”, ejemplos de señales recientes)

```sql
SELECT event_time, side, intensity,
       fields->>'account_ratio' AS account_ratio,
       fields->>'position_ratio' AS position_ratio,
       fields->>'policy' AS policy
FROM oraculo.slice_events
WHERE event_type = 'top_traders'
  AND event_time >= now() - interval '30 days'
ORDER BY event_time DESC
LIMIT 50;
```
- Health: conteo por hora/día para medir frecuencia vs thresholds.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Pipeline macro: `alerts_queue_depth{stream="macro"}`.
- Logs incluyen `policy`, `require_both`, `n_accounts`, `latency_ms` y ratios finales.
- Monitorear lag del feed de posiciones; si está stale, se registran advertencias en logs.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Computo depende de agregados por cuenta; mantener `poll_s`≥15 y limitar cardinalidad (top N) para evitar O(N²).
- Persistir solo campos resumidos (ratios) en events; evitar payloads masivos.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Sin eventos: verificar feed de cuentas y que `enabled` esté en true; umbrales pueden estar por encima del p99.
- Ruido: subir `acc_warn/strong` y `pos_warn/strong`; activar `require_both`.
- Lag: revisar `alerts_batch_duration_seconds{stream="macro"}` y backpressure en metrics collector.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Subir umbrales a p99 y `retrigger_s` a 900 para observación pasiva.
- Desactivar con `enabled=false`; mantener consultas de calibración para monitorear sin alertar.
