# Runbook: Calibración de `iv_spike` (R19)

## Objetivo
Determinar umbrales (`dv_warn`, `dv_strong`, y opcionalmente `vel_warn_per_s`/`vel_strong_per_s`) para el detector macro `iv_spike`, basado en la superficie DOC `deribit.options_iv_surface` y con fallback opcional a `options_ticker.mark_iv`.

## Metodología de percentiles
1. **Extracción (últimas 24–72h)**  
   - Query sobre `deribit.options_iv_surface` filtrando `underlying`, `tenor_bucket` y `moneyness_bucket` configurados.  
   - Calcular `dv = iv_t - iv_{t - window_s}` para cada bucket válido dentro de `lookback_s`.  
   - Calcular `vel = dv / Δt` usando la separación real entre `event_time` de las muestras (mínimo 1s).  
2. **Percentiles**  
   - `dv_warn` ≈ p90–p95 de `dv` (en valor absoluto).  
   - `dv_strong` ≈ p97–p99 de `dv`.  
   - Si se activa la compuerta de velocidad, fijar `vel_warn_per_s` y `vel_strong_per_s` en los mismos percentiles de `|vel|`.  
3. **Validación rápida**  
   - Revisar que `n_used` (muestras usadas por el surface builder) sea estable y sin “saltos” de lag.  
   - Probar la señal en entorno de staging con `detectors.iv_spike.enabled=true` y `poll_s` alineado con el surface (30s).

## Interpretación de unidades
- **IV, dv**: en **decimales** (0.03 == 3 “vol points”).  
- **Velocidad**: IV por segundo (ej. 0.00010 ≈ 1 vol point cada 100s).  
- **Clamps** (`clamp_iv`, `clamp_dv`, `clamp_vel`): protegen contra outliers del builder o datos faltantes.

## Fallback a `options_ticker.mark_iv`
- Activar `fallback_to_ticker=true` solo para evitar ceguera cuando el surface esté vacío o atrasado.  
- Se aplica un filtro ATM aproximado: `delta` (0.35–0.65), `moneyness_abs`, `expiry_max_days` y mínimo de instrumentos (`ticker_min_instruments`).  
- Auditoría en el evento: `source="ticker_fallback"` más los filtros usados.

## Troubleshooting
- **Surface vacío o atrasado**: verificar que el surface builder DOC esté habilitado y con política de cagg al día.  
- **DV siempre cero**: revisar que `tenor_bucket`/`moneyness_bucket` coincidan con las buckets publicadas y que `lookback_s` cubra al menos dos muestras.  
- **Velocidad siempre baja**: considerar desactivar `use_velocity_gate` o recalibrar percentiles de `vel` con una ventana más larga.
