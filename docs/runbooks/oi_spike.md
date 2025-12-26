# oi_spike runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta incrementos rápidos de open interest combinando momentum y ratios de crecimiento.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Feeds de open interest por instrumento/tenor.
- Opcional: precios para confirmar dirección.

## 3) Configuración (knobs YAML + valores recomendados iniciales)
- `poll_s` (5), `retrigger_s` (60).
- Ventanas `oi_window_s` (120) y `momentum_window_s` (60).
- Umbrales `oi_warn_pct`/`oi_strong_pct` (0.80/1.50) y `mom_warn_usd`/`mom_strong_usd` (8/20).
- `require_same_dir` (true) para consistencia.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Percentiles de crecimiento de OI para ajustar `oi_*` thresholds.
- Backtest con eventos históricos de apertura de posiciones.

## 5) Validación (queries de “health”, ejemplos de señales recientes)
- Health: cambios de OI por hora y señales emitidas.
- Ejemplos recientes con payload de OI inicial/final.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Métricas de delta OI y momentum.
- Logs con dirección (long/short) y confirmaciones.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Cachear últimos snapshots para evitar relecturas.
- Limitar universo de instrumentos a los más líquidos.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Si falta datos: revisar feed de OI o ventanas de polling.
- Si ruido: subir `oi_warn_pct` o requerir más instrumentos.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Deshabilitar temporalmente o reducir `require_same_dir` para solo monitoreo.
- Aumentar `retrigger_s` para evitar spam.
