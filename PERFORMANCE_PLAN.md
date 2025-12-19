# Plan de optimización y validación para el servicio de alerts

Este plan prioriza cambios en código y pruebas de carga para reducir el **event-loop lag** (~79 s), eliminar drops por `stale_drop` y lograr latencias de cola <200 ms en los streams `depth` y `trade`.

## 1. Cambios de código prioritarios

1. **Desacoplar trabajo CPU-bound/I-O bloqueante del event loop**
   - Ubicación sugerida: `oraculo/alerts/` (procesadores de depth y trade) y cualquier stage de reglas que corra en el loop principal.
   - Acción: detectar funciones de parsing/puntuación que tarden >2–5 ms y moverlas a `asyncio.to_thread` o a un `ProcessPoolExecutor` si son CPU-bound; envolver I/O bloqueante (DB/HTTP) con clientes async.
   - Verificación: habilitar logs de `asyncio` con `slow_callback_duration=0.05` (Py3.11) y grafana panel de `oraculo_event_loop_lag_seconds` <0.5 s.

2. **Micro-batching y yields cooperativos en depth**
   - Ubicación: consumidor de depth en la ruta hot de alerts.
   - Acción: limitar batch size (p. ej. 200–500 mensajes o 50 ms de ventana), y tras cada batch ejecutar `await asyncio.sleep(0)` para ceder el loop.
   - Métrica objetivo: `oraculo_alerts_queue_time_latest_seconds{stream="depth"}` <0.2 s; `queue_depth` <50.

3. **Parallelizar consumo de depth**
   - Ubicación: pipeline de depth; shardear por símbolo o venue si es seguro.
   - Acción: crear N consumidores (N=2–4) con partición determinística; proteger secciones críticas con locks o aislar estado por shard.
   - Riesgo/control: probar idempotencia y orden por símbolo.

4. **Optimizar ruta de DB de alerts**
   - Ubicación: función `upsert_rule_alert` y batcher de insert de dispatch logs.
   - Acción: asegurar que las escrituras usan pool async y fuera del loop; agrupar upserts o usar operaciones bulk. Meta: p50 <20 ms, p95 <50 ms.
   - Añadir métricas de throughput/errores si faltan (`oraculo_db_insert_ms`).

5. **Backpressure temprano y descartes controlados**
   - Ubicación: entrada al hot path de alerts.
   - Acción: si `queue_depth` supera umbral (p. ej. 500), suspender ingestión de depth con un semáforo/`asyncio.Queue(maxsize=...)` o degradar a muestreo (drop aleatorio) antes de procesar.
   - Objetivo: `oraculo_alerts_queue_dropped_total` y `stale_drop` ≈ 0 bajo carga normal.

6. **Instrumentación adicional**
   - Añadir métricas por etapa: tasa de entrada/salida (msgs/s), tiempo de CPU por batch, y tamaños de batch efectivos.
   - Loggear top-N callbacks lentos (usar `asyncio.get_running_loop().slow_callback_duration`) y exportarlos a Prometheus.

## 2. Plan de pruebas de carga y validación

1. **Generador de carga WS simulado**
   - Script nuevo: `scripts/loadtest_ws.py`.
   - Función: publicar mensajes de depth/trade al mismo ritmo que producción (configurable), midiendo end-to-end lag y drops.
   - Debe poder configurar: rps, tamaño de batch, distribución por símbolo, duración de la prueba.

2. **Runner reproducible**
   - Añadir `make loadtest-alerts` o comando en `scripts/cli.py` que ejecute el generador contra un entorno local con Prometheus scrape habilitado.
   - Guardar resultados en `artifacts/loadtest/<timestamp>/metrics.json` (incluye lag, drops, event-loop lag máximo).

3. **KPIs y criterios de aceptación**
   - Event-loop lag p95 < 0.5 s.
   - `oraculo_alerts_queue_time_seconds` media <200 ms para depth/trade; `queue_depth` pico <50.
   - `stale_drop` = 0 en pruebas de 15 min al 120 % de la carga actual.
   - DB upsert p95 <50 ms, sin timeouts.

4. **Procedimiento de prueba**
   - Levantar entorno local: `python scripts/cli.py health` y asegurar Prometheus scrape.
   - Ejecutar: `python scripts/loadtest_ws.py --stream depth --rps 1500 --duration 900 --symbols 50` (ajustar según meta).
   - Capturar métricas antes/después de aplicar cambios y comparar KPIs.

## 3. Orden de ejecución recomendado

1) Implementar yields/micro-batching y mover CPU-bound a ejecutores (pasos 1–2). Medir lag y drops.
2) Parallelizar depth si aún hay backlog (paso 3).
3) Optimizar DB y backpressure temprano (pasos 4–5).
4) Ampliar instrumentación y automatizar load tests (pasos 6, sección 2).

## 4. Entregables

- PR con cambios en el pipeline de alerts (yields, ejecutores, sharding depth) + mejoras DB.
- Script `scripts/loadtest_ws.py` y objetivo en `make`/CLI para correr pruebas.
- Dashboard/alerta nueva en Grafana: event-loop lag, queue depth, stale drops, p95 de colas.

Este plan es incremental: después de cada fase, correr el load test y validar KPIs antes de avanzar.

## 5. PromQL para confirmar CPU-bound/GIL y correlación con lag

- **Confirmar saturación single-core:**
  - `avg_over_time(oraculo_cpu_process_cores{service="alerts"}[5m])`
  - Esperado: >0.9 si el proceso está al 100 % de 1 core.
- **Detectar contención de GIL con varios hilos:**
  - `oraculo_threads_total{service="alerts"}` (debe ser >=5 para sospechar GIL).
  - `sum(rate(oraculo_thread_cpu_seconds{service="alerts"}[1m]))` ≈ 1.0 cores indica contención.
  - `topk(10, rate(oraculo_thread_cpu_seconds{service="alerts"}[1m]))` para ver reparto por hilo (thread_name/native_id).
- **Diferenciar single-thread vs GIL contention:**
  - Si un solo hilo tiene ~1.0 core y el resto ~0: diseño single-thread/hotspot único.
  - Si 3–10 hilos aportan CPU>0 pero la suma no pasa de ~1.0: contención del GIL muy probable.
- **Correlacionar con lag y ejecutor:**
  - `oraculo_event_loop_lag_seconds{service="alerts"}` frente a `oraculo_to_thread_executor_queue_depth{service="alerts"}`.
  - `oraculo_to_thread_executor_threads{service="alerts"}` vs `oraculo_to_thread_executor_max_workers{service="alerts"}` para ver saturación.
  - `increase(oraculo_event_loop_lag_spikes_total{service="alerts",threshold=~"0.5|2.0|10.0"}[15m])` para ver frecuencia de spikes.
- **Errores y dumps automáticos:**
  - `increase(oraculo_task_exceptions_total{service="alerts"}[15m])` y `oraculo_last_exception_timestamp_seconds{service="alerts"}` para ubicar fallos.
  - `increase(oraculo_dumps_total{service="alerts",reason="event_loop_lag"}[1h])` para confirmar que se emitieron stacks durante lag alto.
