# break_wall runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta rupturas de muro de liquidez tras depleción y falta de refill en ventana corta.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Depth con niveles top (top_levels_gate 20).
- Opcional: trades para confirmar presión agresiva.

## 3) Configuración (knobs YAML + valores recomendados iniciales)
- `n_min` (3) repeticiones de señal.
- `dep_pct` (0.40) caída mínima.
- `basis_vel_abs_bps_s` (1.5) para gating de basis.
- Bloque `doc` con `dv_dep_warn`, `dv_refill_max`, `clamp_abs_dv`.
- `require_depletion`, `forbid_refill_under_pct`, `refill_window_s`, `refill_min_pct`.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Usar percentiles de depleción DOC para ajustar `dv_dep_warn` y `dv_refill_max`.
- Revisar casos de ruptura exitosa vs falsos disparos.

## 5) Validación (queries de “health”, ejemplos de señales recientes)
- Comparar señales de break_wall con métricas de refill en la misma ventana.
- Revisar 5 eventos recientes con snapshots de profundidad.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Métricas: tasa de rupturas, proporción con refill bloqueado.
- Logs con detalle de ventana DOC y parámetros usados.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Indexar depth por `ts` y `level`; limitar a `top_levels_gate`.
- Clamp de doc para evitar outliers y cálculos costosos.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Si no dispara: revisar consistencia de `metrics_doc` y que `require_depletion=true` tenga insumos.
- Si sobre-dispara: subir `dep_pct` o `dv_dep_warn`.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Configurar `enabled=false` o `require_depletion=false` para modo solo-refill.
- Aumentar `refill_min_pct` para requerir más confirmación.
