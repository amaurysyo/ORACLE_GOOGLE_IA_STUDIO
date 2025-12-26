# top_traders runbook

## 1) Propósito y definición operativa (qué detecta exactamente)
- Evalúa concentración de cuentas/posiciones top para detectar crowding o migración de liquidez.

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Feeds agregados de account/position ratios por exchange.
- Opcional: volumen por cuenta para ponderar.

## 3) Configuración (knobs YAML + valores recomendados iniciales)
- `poll_s` (15) y `retrigger_s` (300).
- Umbrales `acc_warn`/`acc_strong` y `pos_warn`/`pos_strong` (0.60/0.70).
- Política `require_both` y `choose_by`.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Revisar distribución histórica de ratios para fijar warn/strong.
- Definir si usar both o max_score según ruido observado.

## 5) Validación (queries de “health”, ejemplos de señales recientes)
- Health: ratio promedio y nº de señales por día.
- Ejemplos recientes con payload de cuentas/posiciones top.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Métricas separadas para account/position y decisión final.
- Logs con `choose_by` y valores crudos.

## 7) Performance (índices recomendados, filtros lookback, límites)
- Indexar datos por snapshot time.
- Limitar número de cuentas consideradas para cálculo en memoria.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Si falta data: validar fuente de cuentas top.
- Si ruido: subir umbrales o setear `require_both=true`.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Desactivar o aumentar umbrales de warn/strong.
- Extender `retrigger_s` para menor frecuencia.
