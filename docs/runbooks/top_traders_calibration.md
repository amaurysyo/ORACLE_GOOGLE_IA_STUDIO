# Top Traders — Calibración de umbrales

## Objetivo
Calibrar los umbrales `acc_warn/acc_strong` y `pos_warn/pos_strong` del detector `top_traders` usando percentiles de las series DOC.

## Datos de entrada
- Tablas Timescale/Postgres:
  - `binance_futures.top_trader_account_ratio` (campos: `event_time`, `long_ratio`, `short_ratio`, `meta`)
  - `binance_futures.top_trader_position_ratio` (campos: `event_time`, `long_ratio`, `short_ratio`, `meta`)
- Configuración inicial en `config/rules.yaml` (placeholders razonables):
  - `acc_warn=0.60`, `acc_strong=0.70`
  - `pos_warn=0.60`, `pos_strong=0.70`
  - `choose_by=max_score`, `require_both=false`, `enabled=false`

## Procedimiento sugerido
1. **Extraer percentiles históricos** (ventana ≥30 días, por instrumento):
   ```sql
   SELECT
     percentile_cont(0.95) WITHIN GROUP (ORDER BY long_ratio) AS p95_long,
     percentile_cont(0.99) WITHIN GROUP (ORDER BY long_ratio) AS p99_long,
     percentile_cont(0.95) WITHIN GROUP (ORDER BY short_ratio) AS p95_short,
     percentile_cont(0.99) WITHIN GROUP (ORDER BY short_ratio) AS p99_short
   FROM binance_futures.top_trader_account_ratio
   WHERE instrument_id = $1;
   ```
   Repetir para `top_trader_position_ratio`. Usar `p95` como `warn` y `p99` como `strong` de cada métrica.

2. **Comparar regímenes intradía** (opcional):
   - Ejecutar la misma consulta particionando por sesión (ej. `EXTRACT(HOUR FROM event_time)` en UTC) para detectar sesgos en USA vs Asia.
   - Si hay diferencias significativas, considerar umbrales por sesión vía plantillas de rules.

3. **Seleccionar política de disparo** (`require_both` / `choose_by`):
   - **Conservador**: `require_both=true`, `choose_by=max_score` → exige señal simultánea en account y position; útil para reducir falsos positivos.
   - **Flexible**: `require_both=false`, `choose_by=max_score` → dispara si cualquiera supera `warn` y toma el mayor score.
   - **Degradado**: `choose_by=account_only` o `position_only` si alguna tabla está incompleta; mantener `require_both=false` en este modo.

4. **Validación en replay/offline**:
   - Reproducir la lógica del detector sobre históricos y contar hits/semana.
   - Revisar distribución de `intensity` (0..1) para ajustar `warn/strong` hasta que `p50` quede <0.4 y `p95` cerca de 0.8 en eventos verdaderamente sesgados.

5. **Habilitación gradual**:
   - Mantener `detectors.top_traders.enabled=false` en producción hasta validar.
   - Habilitar sólo en entornos de staging o con `poll_s` alto (≥15s) para evitar sobrecarga.

## Checklist de auditoría del evento
El evento `top_traders` publica en `fields`:
- `acc_event_time`, `pos_event_time`
- `acc_long_ratio`, `acc_short_ratio`, `pos_long_ratio`, `pos_short_ratio`
- `acc_meta`, `pos_meta`
- `acc_warn`, `acc_strong`, `pos_warn`, `pos_strong`
- `require_both`, `choose_by`, `metric_used`
- `score_acc_long`, `score_acc_short`, `score_pos_long`, `score_pos_short`

Verificar que los valores usados (warn/strong y política) coincidan con la calibración elegida antes de habilitar la alerta.
