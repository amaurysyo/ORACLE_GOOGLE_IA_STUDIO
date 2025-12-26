# skew_shock runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta shocks de skew (rr_25d) en opciones en ventana de 5 min con thresholds de delta y velocidad.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Surface de opciones con rr_25d por tenor.
- Precios subyacentes para clamps.

## 3) Configuración (knobs YAML + valores recomendados iniciales)
- `poll_s` (15), `retrigger_s` (300).
- `delta_warn`/`delta_strong` (0.010/0.020) y velocidades `vel_warn_per_s`/`vel_strong_per_s` (3e-5/6e-5).
- Clamps `clamp_abs_rr`, `clamp_abs_delta`, `clamp_abs_vel_per_s`.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Percentiles históricos de rr_25d para fijar delta/vel.
- Separar por underlying (`underlying`=BTC).

## 5) Validación (queries de “health”, ejemplos de señales recientes)
- Health: conteo de shocks por día y sesiones sin datos.
- Ejemplos recientes con serie rr_25d y velocidad.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Métricas por tenor y severidad.
- Logs con clamps aplicados.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Cache de surface por poll; limitar tenores relevantes.
- Clamp valores extremos para estabilidad numérica.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Si sin señales: revisar surface y latencias.
- Si ruido: subir deltas o requerir `require_recent_past=true`.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Desactivar o subir umbrales.
- Aumentar `retrigger_s` para reducir frecuencia.
