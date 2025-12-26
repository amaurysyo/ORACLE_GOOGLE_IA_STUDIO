# gamma_flip runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta cambio de signo de gamma neta agregada en opciones, que puede alterar dinámica de cobertura.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Surface de opciones con greeks (`net_gex`).
- Spot para clamps (`clamp_spot`).

## 3) Configuración (knobs YAML + valores recomendados iniciales)
- `poll_s` (30), `retrigger_s` (900).
- Filtros de instrumentos: `lookback_s`, `expiry_max_days`, `moneyness_abs`, `delta_gate_enabled` + rangos de delta.
- Umbrales `min_instruments`, `min_abs_net_gex`, `flip_hysteresis` (0.10).
- Clamps `clamp_gamma`, `clamp_oi`.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Analizar distribución histórica de net_gex para fijar `min_abs_net_gex`.
- Ajustar filtros de delta/moneyness según liquidez del underlying.

## 5) Validación (queries de “health”, ejemplos de señales recientes)
- Health: nº de flips detectados y estabilidad del signo previo.
- Ejemplos recientes con payload agregando tenor y signo.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Métricas con valor neto de gamma y signo.
- Logs con instrumentos incluidos/excluidos y hysteresis aplicada.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Filtrar instrumentos por expiración y delta para reducir cardinalidad.
- Cachear surface de 10 min para evitar recomputes.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Si sin datos: revisar feed de OI/greeks y clamps.
- Si ruido: subir `min_abs_net_gex` o `min_instruments`.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Desactivar detector o aumentar `flip_hysteresis` para amortiguar cambios.
- Extender `retrigger_s` para menor spam.
