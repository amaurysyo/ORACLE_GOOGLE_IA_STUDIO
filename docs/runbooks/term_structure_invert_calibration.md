# Term structure invertida — Runbook de calibración

## Objetivo
- Calibrar los umbrales de spread entre buckets de tenor (`short_tenor_bucket` vs `long_tenor_bucket`) para el detector `term_structure_invert` (R37).
- Medir percentiles del spread y, opcionalmente, de su velocidad para elegir `spread_warn`/`spread_strong` (y `vel_warn_per_s`/`vel_strong_per_s` si se habilita el gate de velocidad).

## Datos y buckets
- Fuente: `deribit.options_iv_surface` (no usar el cagg `iv_surface_1m` porque no agrupa `tenor_bucket`). El detector aplica lookback corto (`lookback_s`, por defecto 600s) y `LIMIT 1` para evitar scans completos.
- Buckets de referencia (alineados con el surface builder):
  - `short_tenor_bucket`: `"0_7d"`
  - `long_tenor_bucket`: `"30_90d"` (alternativa configurable `"90_180d"` si se quiere más back tenor)
  - `moneyness_bucket`: `"NA"` (curve “pura”, sin RR/BF)
- Unidades de IV: decimales (ej. 0.55 = 55 vol points). Clamp de seguridad en detector: `clamp_abs_iv=5.0`, `clamp_abs_spread=1.0`.

## Cómo estimar percentiles de spread
1) Construir el spread minuto a minuto (o por snapshot disponible) en una ventana reciente (p. ej. 7–30 días) respetando lookback corto. Ejemplo SQL:
   ```sql
   WITH short AS (
     SELECT date_trunc('minute', event_time) AS bucket, MAX(event_time) AS ts_short, MAX(iv) AS iv_short
     FROM deribit.options_iv_surface
     WHERE underlying = 'BTC' AND tenor_bucket = '0_7d' AND moneyness_bucket = 'NA'
     GROUP BY 1
   ), long AS (
     SELECT date_trunc('minute', event_time) AS bucket, MAX(event_time) AS ts_long, MAX(iv) AS iv_long
     FROM deribit.options_iv_surface
     WHERE underlying = 'BTC' AND tenor_bucket = '30_90d' AND moneyness_bucket = 'NA'
     GROUP BY 1
   )
   SELECT s.bucket,
          s.iv_short - l.iv_long AS spread,
          s.ts_short,
          l.ts_long
   FROM short s
   JOIN long l USING (bucket)
   WHERE s.iv_short IS NOT NULL AND l.iv_long IS NOT NULL;
   ```
2) Exportar la serie de `spread` y calcular percentiles (p95, p99). Recomendación inicial:
   - `spread_warn` ≈ p95
   - `spread_strong` ≈ p99
3) Si se quiere usar velocidad (`use_velocity_gate=true`), derivar `Δspread/Δt` sobre la misma serie y tomar p95/p99 para `vel_warn_per_s` y `vel_strong_per_s` (en unidades de spread por segundo).

## Recomendaciones de configuración
- Mantener `detectors.term_structure_invert.enabled=false` hasta completar la calibración y validar falsos positivos.
- `require_both_present=true` evita emitir cuando falta alguno de los buckets; si se cambia a `false`, confirmar que el spread no se calcule con datos viejos.
- `require_positive_inversion=true` filtra shocks negativos; sólo desactivarlo si se desea alertar también por steepening (IV long > IV short).
- El cooldown `retrigger_s` (600s por defecto) previene spam en mercados laterales; ajustar según frecuencia observada de inversiones.

## Validación
- Verificar auditoría en los eventos: `iv_short`, `iv_long`, `spread`, `ts_short`, `ts_long`, `n_used_*`, umbrales (`spread_warn`/`spread_strong`), y, si aplica, `vel_per_s`/`i_vel`.
- Confirmar que los queries de producción usan `LIMIT 1` y lookback corto para no cargar la tabla de surface.
