# basis_dislocation runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta dislocaciones de basis confirmadas por funding y velocidad de cambio.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Basis spot vs perp, y stream de funding (`funding_window_s`).
- DOC opcional según `metric_source`.

## 3) Configuración (knobs YAML + valores recomendados iniciales)
- `poll_s` (10), `retrigger_s` (180).
- Umbrales basis/vel en bps (`basis_warn_bps`, `basis_strong_bps`, `vel_warn_bps_s`, `vel_strong_bps_s`).
- `require_funding_confirm`, `funding_window_s`, `funding_trend_warn/strong`.
- Clamps: `clamp_abs_basis_bps`, `clamp_abs_vel_bps_s`.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Usar distribuciones de basis y funding para fijar warn/strong.
- Analizar falsos positivos al variar `require_funding_confirm`.

## 5) Validación (queries de “health”, ejemplos de señales recientes)
- Health: eventos de dislocación vs frecuencia de funding.
- Ejemplos con basis/vel/funding en payload.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Métricas de basis y funding trend.
- Logs que indiquen si se usó fallback sin funding.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Cachear basis y funding recientes para no releer histórico.
- Limitar `funding_window_s` para cómputo ligero.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Si no dispara: revisar feeds de funding/basis y clamps.
- Si ruido: subir `basis_warn_bps` o exigir funding confirm.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Setear `allow_emit_without_funding=true` solo en monitoreo.
- Aumentar umbrales o deshabilitar en YAML.
