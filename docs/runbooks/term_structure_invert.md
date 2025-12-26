# term_structure_invert runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta inversión de la curva de IV entre tenores cortos y largos (short vs long tenor).

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Surface IV por tenor corto (`short_tenor_bucket`) y largo (`long_tenor_bucket`).
- Spot opcional para clamps.

## 3) Configuración (knobs YAML + valores recomendados iniciales)
- `poll_s` (30), `retrigger_s` (600).
- `lookback_s` (600) y moneyness bucket (NA).
- Spread thresholds `spread_warn` (0.05) y `spread_strong` (0.10).
- Velocidad `vel_warn_per_s`/`vel_strong_per_s` y `use_velocity_gate`.
- Clamps `clamp_abs_iv`, `clamp_abs_spread`, `clamp_abs_vel_per_s`, flags `require_both_present`, `require_positive_inversion`.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Percentiles de spread corto-largo para fijar thresholds.
- Evaluar ruido en vel para decidir `use_velocity_gate`.

## 5) Validación (queries de “health”, ejemplos de señales recientes)
- Health: nº de inversiones detectadas por día.
- Ejemplos recientes con valores de short/long IV y spread.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Métricas de spread y vel por tenor par.
- Logs con clamps y flags aplicados.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Cache de surface; limitar cómputo a buckets configurados.
- Clamp de valores para estabilidad numérica.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Si sin datos: revisar buckets disponibles en surface.
- Si ruido: subir thresholds o exigir `require_both_present`.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Desactivar o subir umbrales; deshabilitar velocity gate si bloquea.
- Aumentar `retrigger_s` para menor frecuencia.
