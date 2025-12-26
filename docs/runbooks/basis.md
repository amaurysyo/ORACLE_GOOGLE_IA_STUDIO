# basis runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Monitorea desviaciones de basis y su velocidad para eventos extremos o mean-revert.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Mark/Index price stream y spot para cálculo de basis.
- Opcional: funding para confirmaciones.

## 3) Configuración (knobs YAML + valores recomendados iniciales)
- `metric_source` y `doc_sign_mode`.
- `extreme.pos_bps`/`neg_bps` (±100 bps).
- `mean_revert.gate_abs_bps` (25) y `vel_gate_abs` (1.5).
- `retrigger_s` (60).

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Usar percentiles de basis por instrumento para validar `extreme`.
- Analizar derivadas para `vel_gate_abs`.

## 5) Validación (queries de “health”, ejemplos de señales recientes)
- Health: frecuencia de basis extremo vs histórico.
- Ejemplos recientes con basis y vel en payload.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Métricas de basis actual, vel y severidad.
- Logs con fuente usada (legacy/doc/auto).

## 7) Performance (índices recomendados, filtros lookback, límites)
- Cache de últimas lecturas de basis para evitar queries largas.
- Clamps para limitar valores extremos.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Si señal errática: revisar `doc_sign_mode` y clamps.
- Si no dispara: validar feed de index/mark y latencia.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Subir umbrales `extreme` o desactivar mean-revert temporalmente.
- Setear `metric_source=legacy` si DOC falla.
