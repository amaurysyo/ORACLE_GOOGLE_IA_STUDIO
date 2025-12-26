# spoofing runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta muros que aparecen y se retiran rápido con baja ejecución, patrón típico de spoofing bid/ask.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Depth granular (top_levels_gate=30).
- Trades para medir ratio de ejecución vs cancelación.

## 3) Configuración (knobs YAML + valores recomendados iniciales)
- `wall_size_btc` (80) por lado, `distance_ticks` (10) al best.
- Timing: `appear_within_ms` (400), `hold_min_ms` (500), `cancel_within_ms` (1200).
- Ratios: `min_exec_ratio` (0.05), `max_cancel_ratio` (0.85).
- `per_side` overrides para bid/ask.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Usar percentiles de cancelación rápida para fijar `cancel_within_ms`.
- Analizar tamaños típicos para ajustar `wall_size_btc` por mercado.

## 5) Validación (queries de “health”, ejemplos de señales recientes)
- Health: conteo de muros detectados y ratio de ejecución real.
- Ejemplos recientes mostrando trayectoria del muro.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Métricas por side y distancia al best.
- Logs con timestamps de aparición/cancelación.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Procesar solo `top_levels_gate` para reducir costo.
- Clamp de tamaños para evitar overflow en instrumentos con lotes pequeños.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Si no detecta: bajar `wall_size_btc` o ampliar `distance_ticks`.
- Si hay ruido: endurecer `min_exec_ratio` o `hold_min_ms`.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Desactivar `enabled` o subir `wall_size_btc` mientras se recalibra.
- Reducir `max_cancel_ratio` para exigir mayor ejecución.
