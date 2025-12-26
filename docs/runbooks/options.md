# options runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Agrupa detectores de opciones (IV spike y OI skew) con thresholds específicos para R19-R22.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Curva de volatilidad implícita (surface) y open interest agregado.
- Greeks/mark prices por tenor y moneyness.

## 3) Configuración (knobs YAML + valores recomendados iniciales)
- Sub-bloques `iv_spike` (window_s=60, up_pct/down_pct=±15%).
- Sub-bloques `oi_skew` con mínimos de calls/puts y ratios bull/bear (1.5).

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Usar percentiles de IV por tenor para ajustar up/down.
- Evaluar distribución de OI para fijar ratios de skew.

## 5) Validación (queries de “health”, ejemplos de señales recientes)
- Health: conteo de spikes vs tasa de publicaciones de surface.
- Ejemplos con tenores y strikes afectados.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Métricas separadas por sub-detector (iv_spike, oi_skew).
- Logs con tenor/moneyness y valor de referencia.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Cachear surfaces recientes; limitar a tenores configurados.
- Indexar OI por expiry y option_type.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Si falta surface: validar ingesta de Deribit/vendor y fallback a ticker.
- Si ruido: subir umbrales o exigir `min_calls`/`min_puts` mayores.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Desactivar sub-bloques o subir ratios hasta estabilizar.
- Reducir `retrigger_s` solo para monitoreo pasivo.
