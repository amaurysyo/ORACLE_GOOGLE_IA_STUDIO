# iv_spike runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta spikes de volatilidad implícita en tenor y moneyness específicos usando ventana de 5 min.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Surface IV por tenor (`tenor_bucket`) y moneyness (`moneyness_bucket`).
- Ticker fallback si surface falta.

## 3) Configuración (knobs YAML + valores recomendados iniciales)
- `poll_s` (30), `retrigger_s` (600).
- `lookback_s` (900) y `window_s` (300).
- Umbrales `dv_warn`/`dv_strong` (0.03/0.07) y velocidades `vel_warn_per_s`/`vel_strong_per_s`.
- Gating `require_positive_spike`, fallback a ticker con gates de delta/expiry.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Percentiles de dv por tenor para ajustar warn/strong.
- Verificar si usar `use_velocity_gate` según ruido en vel.

## 5) Validación (queries de “health”, ejemplos de señales recientes)
- Health: spikes detectados vs lecturas de surface.
- Ejemplos recientes con tenor/moneyness y dv observado.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Métricas por tenor, moneyness y severidad.
- Logs indicando si se usó fallback a ticker.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Cachear surface de 15 min; clamp IV/dv/vel para estabilidad.
- Filtrar instrumentos según `ticker` gates para reducir costos.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Si sin surface: revisar ingestión o activar fallback.
- Si ruido: subir dv thresholds o habilitar velocity gate.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Desactivar o subir dv thresholds temporalmente.
- Extender `retrigger_s` para menor frecuencia.
