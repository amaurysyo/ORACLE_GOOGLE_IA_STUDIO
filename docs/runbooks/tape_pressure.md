# tape_pressure runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Detecta presión agresiva sostenida en el tape usando OFI y volumen en ventana corta.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Trades en streaming con side y qty para OFI.
- Spread para gating (`max_spread_usd`).

## 3) Configuración (knobs YAML + valores recomendados iniciales)
- `window_s` (5s), `min_trades` (10), `min_qty_btc` (30).
- OFI thresholds `ofi_up` (0.65) y `ofi_down` (-0.65).
- `hold_ms` (500) y `retrigger_s` (20).

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Calcular percentiles de OFI para definir umbrales por mercado.
- Revisar distribución de volumen por ventana para ajustar `min_qty_btc`.

## 5) Validación (queries de “health”, ejemplos de señales recientes)
- Health: conteo de ventanas con OFI extremo y spread permitido.
- Ejemplos recientes mostrando series OFI y precio.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Métricas OFI por símbolo y severidad.
- Logs con trade_ids y acumulado por ventana.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Indexar trades por `ts` y `symbol`; usar rolling window en memoria si es posible.
- Limitar lookback a últimos minutos para cálculo continuo.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Si falta detección: verificar feed de trades o side inference.
- Si ruido: subir `min_qty_btc` o OFI thresholds.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Subir umbrales o desactivar `enabled` en YAML.
- Extender `retrigger_s` para reducir alertas.
