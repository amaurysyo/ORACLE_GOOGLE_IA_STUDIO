# BreakWall DOC – calibración de umbrales de depleción/refill

## Metodología

1. Extraer el `depletion_*_doc` por lado (bid/ask) en la ventana configurada (ej.: 3s).
2. Calcular:
   - `depletion_amount = max(0, -value)` (solo valores negativos).
   - `refill_amount = max(0, value)` (solo valores positivos).
3. Obtener percentiles:
   - `depletion_amount`: p50/p90/p95/p99.
   - `refill_amount`: p50/p90/p95/p99.

## Recomendaciones iniciales

- **dv_dep_warn**: usar el p95 de `depletion_amount` como umbral de aviso.
- **dv_refill_max**: usar el p90 (o p95 si se busca más tolerancia) de `refill_amount` como máximo permitido antes de bloquear la señal.

Ajustar ambos valores según la sensibilidad deseada del detector y la microestructura del instrumento.
