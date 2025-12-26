# Plantilla de runbook para detectores

## 1) Propósito y definición operativa (qué detecta exactamente)
- Describir en 2-3 frases el objetivo del detector y las señales que lo disparan.
- Aclarar cuándo **no** debe disparar (excepciones o gating).

## 2) Dependencias de datos (tablas/vistas/streams, lag esperado)
- Streams/tablas necesarios con nombres exactos y particiones (ej: `depth_stream`, `trades_stream`).
- Lag máximo aceptable por fuente y qué hacer si se degrada.

## 3) Configuración (knobs YAML + valores recomendados iniciales)
- Ubicación: `config/rules.yaml` → `detectors.<detector>`.
- Ejemplo de bloque YAML:

```yaml
detectors:
  <detector>:
    enabled: true
    warn_threshold: 10
    strong_threshold: 20
    retrigger_s: 30
```
- Documentar knobs clave, rangos válidos y valores iniciales recomendados.

## 4) Calibración (percentiles + cómo elegir warn/strong/retrigger)
- Query para obtener percentiles recientes y derivar umbrales.

```sql
-- Ejemplo genérico
SELECT
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY metric) AS p50,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY metric) AS p95
FROM detector_signals
WHERE detector = '<detector>'
  AND ts >= NOW() - INTERVAL '7 days';
```
- Definir cómo ajustar warn/strong/retrigger según la distribución.

## 5) Validación (queries de “health”, ejemplos de señales recientes)
- Query de health para confirmar ingestión y emisión de señales.

```sql
SELECT date_trunc('hour', ts) AS h, COUNT(*)
FROM detector_signals
WHERE detector = '<detector>'
  AND ts >= NOW() - INTERVAL '24 hours'
GROUP BY 1
ORDER BY 1 DESC;
```
- Añadir ejemplos de señales recientes con payload resumido.

## 6) Observabilidad (métricas Prometheus / logs / auditoría)
- Métricas recomendadas: tasa de señales, latencia de cómputo, errores.
- Logs clave y etiquetas para auditoría (instrumento, side, severity).

## 7) Performance (índices recomendados, filtros lookback, límites)
- Índices sugeridos para tablas de eventos y dimensiones.
- Límites de lookback y clamping para evitar OOM.

## 8) Troubleshooting (tabla vacía, lag, blockers, fallbacks)
- Checklist cuando no hay señales: ¿flujo de datos?, ¿gating?, ¿umbrales demasiado altos?
- Fallbacks seguros: subir umbrales, reducir lookback, desactivar paths opcionales.

## 9) Rollback / Safe mode (enabled=false, subir umbrales, etc.)
- Pasos rápidos para reducir riesgo: `enabled=false` en YAML, aumentar umbrales, deshabilitar retrigger corto.
- Cómo validar el rollback y plan para restaurar.
