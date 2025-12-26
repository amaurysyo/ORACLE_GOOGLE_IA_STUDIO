# absorption runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta absorción de flujo agresivo cuando un muro sostiene múltiples ejecuciones sin moverse significativamente.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Trades con side y qty para medir presión agresiva.
- Depth en best niveles para confirmar que el muro persiste.

## 3) Configuración (knobs YAML + valores recomendados iniciales)
- `dur_s` (10s) duración de observación.
- `vol_btc` (450) volumen mínimo absorbido.
- `max_price_drift_ticks` (2) y `tick_size` (0.1) para tolerancia de precio.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Usar percentiles de volumen ejecutado por 10s para ajustar `vol_btc`.
- Evaluar drift promedio para fijar `max_price_drift_ticks`.

## 5) Validación (queries de “health”, ejemplos de señales recientes)
- Health: número de absorciones por hora y depth residual.
- Ejemplos recientes con precio y qty restante en muro.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Métricas de conteo y duración de absorciones.
- Logs con nivel de precio, side y qty absorbida.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Indices en trades por tiempo y símbolo.
- Limitar lookback a 15m para evitar scans extensos.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Si no hay casos: revisar que `vol_btc` no sea muy alto para pares ilíquidos.
- Si latencia alta: revisar join de depth+trades.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Desactivar con `enabled=false` o subir `vol_btc` temporalmente.
- Relajar `max_price_drift_ticks` solo si hay falsos negativos.
