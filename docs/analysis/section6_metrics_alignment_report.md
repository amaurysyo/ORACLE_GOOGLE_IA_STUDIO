# Alineación Sección 6 (Métricas microestructurales)

## Tabla original del DOC (Sección 6)

| Métrica | Definición | Ventana | Uso |
| --- | --- | --- | --- |
| Imbalance | (ΣBid−ΣAsk)/(ΣBid+ΣAsk) sobre N niveles | 1–5s | Presión neta en libro |
| Dominance Ask/Bid | Volumen % en top-n por lado | 1–3s | Sesgo local |
| Spread | BestAsk − BestBid | tick | Liquidez inmediata |
| Wmid | (BestAsk+BestBid)/2 | tick | Sesgo vs ask/bid |
| Depletion/Replenishment | Δvolumen por lado en top-n | 1–5s | Huella de agresión |
| Basis | (Index−Mark)/Mark en bps | 60–300s | Spot–Perp dislocación |
| Velocity/Accel Basis | d(basis)/dt, d²(basis)/dt² en bps/s, bps/s² | 60–300s | Cinemática funding |
| OI Δ% | (OI_t−OI_{t−Δ})/OI_{t−Δ} | 60–300s | Cambios estructurales |

Fuente: Sección 6 “Métricas Microestructurales” del DOC `📘 Proyecto — Oráculo Btcusdt  V1 — ACTUALIZADO.docx`.

## Estado actual (post-tareas 1–11.1)
- Métricas DOC implementadas y persistidas en `metrics_series`: `wmid`, `imbalance_doc`, `dominance_*_doc`, `depletion_*_doc`, `basis_bps_doc`, `basis_vel_bps_s_doc`, `basis_accel_bps_s2_doc`, `oi_delta_pct_doc`. Legacy se preserva en paralelo.
- El resolver de métricas usa `metric_source=doc|legacy|auto` con fallback para mantener compatibilidad en reglas durante la transición.
- Algunas reglas continúan consumiendo métricas legacy por política de rollout, aunque las métricas DOC ya estén disponibles.

## Mapeo DOC → CÓDIGO → BD por métrica

### Imbalance
- **Código**: cálculo instantáneo `(ΣBid−ΣAsk)/(ΣBid+ΣAsk)` sobre los `top_n` niveles del libro reconstruido. No usa ventana temporal.【F:oraculo/detect/metrics_engine.py†L162-L169】
- **Persistencia**: se inserta como métrica `imbalance` en `oraculo.metrics_series` con `window_s=1` desde el CPU worker.【F:oraculo/alerts/cpu_worker.py†L477-L493】【F:SQL/SQL_ORACULO_BACKUP.sql†L124114-L124122】
- **Estado**: Parcial (fórmula coincide pero sin la ventana 1–5s especificada en DOC).

### Dominance Ask/Bid
- **Código**: porcentaje de niveles no nulos por lado (`nz_levels_side/total_levels`), no porcentaje de volumen. Usa `top_n` niveles.【F:oraculo/detect/metrics_engine.py†L151-L160】
- **Detección**: `DominanceDetector` documenta la misma semántica basada en conteo de niveles.【F:oraculo/detect/detectors.py†L347-L384】
- **Persistencia**: se guardan `dom_bid` y `dom_ask` con `window_s=1`.【F:oraculo/alerts/cpu_worker.py†L487-L493】
- **Estado**: H1 confirmada (diverge: cuenta niveles, no volumen).

### Spread
- **Código**: `best_ask - best_bid` usando el snapshot del libro.【F:oraculo/detect/metrics_engine.py†L186-L199】
- **Persistencia**: `spread_usd` se inserta en `metrics_series` con `window_s=1`.【F:oraculo/alerts/cpu_worker.py†L484-L493】
- **Estado**: Alineada (mismas magnitudes; DOC expresa en ticks pero la implementación usa USD del libro).

### Wmid
- **Código**: `Snapshot` expone `wmid` calculado a partir del best bid/ask; los detectores lo consumen con fallback al cálculo directo cuando falta la serie.【F:oraculo/detect/metrics_engine.py†L78-L209】【F:oraculo/detect/macro_detectors.py†L456-L516】
- **Persistencia**: se inserta `wmid` en `metrics_series` con `window_s` configurado; detectores macro consultan la serie si el snapshot no trae valor reciente.【F:oraculo/alerts/cpu_worker.py†L477-L500】【F:oraculo/detect/macro_detectors.py†L223-L287】
- **Estado**: Implementada (DOC) con fallback legacy.

### Depletion / Replenishment
- **Código**: proxy en ventana fija de 3s por lado: `dep = deletions/(insertions+deletions)` y `refill = min(insertions/deletions, 1)`. No usa Δvolumen top-n.【F:oraculo/detect/metrics_engine.py†L171-L184】
- **Detección**: `DepletionDetector` y `BreakWallDetector` consumen estas claves (`dep_*`, `refill_*_3s`).【F:oraculo/detect/detectors.py†L482-L511】【F:oraculo/detect/detectors.py†L261-L333】
- **Persistencia**: métricas `dep_bid`, `dep_ask`, `refill_bid_3s`, `refill_ask_3s` con `window_s=1`.【F:oraculo/alerts/cpu_worker.py†L477-L493】
- **Estado**: H3 confirmada (usa proxy de ins/del en 3s, no Δvolumen top-n ni ventana 1–5s).

### Basis
- **Código**: `basis_bps = (mark/index - 1) * 10000`; misma fórmula se ingesta desde Binance WS (`(mark-index)/index * 10000`).【F:oraculo/detect/metrics_engine.py†L139-L148】【F:oraculo/ingest/binance_ws.py†L346-L356】
- **Persistencia**: `basis_bps` con `window_s=1`.【F:oraculo/alerts/cpu_worker.py†L485-L493】
- **Estado**: H2 confirmada (signo y denominador difieren del DOC `(Index−Mark)/Mark`).

### Velocity / Accel Basis
- **Código**: deriva `basis_vel_bps_s` y `basis_accel_bps_s2_doc` sobre la ventana DOC configurable, manteniendo cálculo legacy en paralelo.【F:oraculo/detect/metrics_engine.py†L139-L209】
- **Persistencia**: se guardan `basis_vel_bps_s_doc` y `basis_accel_bps_s2_doc` en `metrics_series` con `window_s=basis_doc_window_s`.【F:oraculo/alerts/cpu_worker.py†L477-L500】
- **Estado**: Implementada (velocidad y aceleración DOC disponibles con fallback legacy). 

### OI Δ%
- **Código/BD**: derivación y persistencia de `oi_delta_pct_doc` vía ingest REST con ventanas configurables; detectores consumen la serie con fallback a `open_interest` si falta.【F:oraculo/ingest/binance_rest.py†L125-L191】【F:oraculo/detect/macro_detectors.py†L147-L241】
- **Estado**: Implementada (DOC) con fallback a legacy.

## Validación de hipótesis H1–H6

| Hipótesis | Estado | Evidencia clave |
| --- | --- | --- |
| H1 Dominance Ask/Bid | Confirmada | Dominance usa conteo de niveles no nulos (no volumen).【F:oraculo/detect/metrics_engine.py†L151-L160】【F:oraculo/detect/detectors.py†L347-L384】 |
| H2 Basis | Confirmada | Código aplica `(mark/index−1)*10000` (signo/denominador invertidos vs DOC).【F:oraculo/detect/metrics_engine.py†L139-L148】【F:oraculo/ingest/binance_ws.py†L346-L356】 |
| H3 Depletion/Replenishment | Confirmada | Proxy ins/del 3s en vez de Δvolumen top-n.【F:oraculo/detect/metrics_engine.py†L171-L184】 |
| H4 Wmid | Actualizada | `wmid` se calcula y persiste; los detectores usan snapshot o serie con fallback al cálculo directo.【F:oraculo/detect/metrics_engine.py†L78-L209】【F:oraculo/alerts/cpu_worker.py†L477-L500】【F:oraculo/detect/macro_detectors.py†L223-L287】 |
| H5 Velocity/Accel Basis | Actualizada | Se derivan y persisten `basis_vel_bps_s_doc` y `basis_accel_bps_s2_doc` junto al legacy; el resolver elige fuente según `metric_source`.【F:oraculo/detect/metrics_engine.py†L139-L209】【F:oraculo/alerts/cpu_worker.py†L477-L500】 |
| H6 OI Δ% | Actualizada | `oi_delta_pct_doc` se deriva en ingest REST y se persiste con fallback a `open_interest` para detectores.【F:oraculo/ingest/binance_rest.py†L125-L191】【F:oraculo/detect/macro_detectors.py†L147-L241】 |

## Tabla resumen DOC vs Código vs BD

| Métrica DOC | Nombre en código | Fórmula / proxy en código | ¿Persiste en metrics_series? | Diferencia vs DOC | Impacto probable |
| --- | --- | --- | --- | --- | --- |
| Imbalance | `imbalance` | (ΣBid−ΣAsk)/(ΣBid+ΣAsk) instantáneo sobre `top_n`.【F:oraculo/detect/metrics_engine.py†L162-L169】 | Sí (`window_s=1`).【F:oraculo/alerts/cpu_worker.py†L477-L493】 | Sin ventana 1–5s. | Cambiar a ventana temporal alteraría Depletion/BW gating que usa snapshots actuales. |
| Dominance Ask/Bid | `dom_bid` / `dom_ask` | Conteo de niveles no nulos por lado / total niveles.【F:oraculo/detect/metrics_engine.py†L151-L160】 | Sí. | No usa % volumen (significado distinto). | Cambiar semántica afectaría `DominanceDetector` y alertas R9/R10.【F:oraculo/detect/detectors.py†L347-L384】【F:oraculo/alerts/runner.py†L1402-L1415】 |
| Spread | `spread_usd` | best_ask − best_bid.【F:oraculo/detect/metrics_engine.py†L186-L199】 | Sí. | Alineada (unidad USD vs “tick” en DOC). | Impacto bajo. |
| Wmid | `wmid` | Calculado como midpoint (best_bid+best_ask)/2 y expuesto en snapshot; detectores usan snapshot o serie persistida con fallback.【F:oraculo/detect/metrics_engine.py†L78-L209】【F:oraculo/alerts/cpu_worker.py†L477-L500】 | Sí. | Alineada (mid DOC disponible). | Impacto bajo; mantener fallback legacy mientras se migra el consumo. |
| Depletion/Replenishment | `dep_bid` / `dep_ask` y `refill_*_3s` | Proxy: deletions/(ins+del) y ins/del (cap 1) en 3s.【F:oraculo/detect/metrics_engine.py†L171-L184】 | Sí. | No es Δvolumen top-n; ventana fija 3s. | Cambiar proxy rompería `DepletionDetector` y `BreakWallDetector`.【F:oraculo/detect/detectors.py†L261-L333】【F:oraculo/detect/detectors.py†L482-L511】 |
| Basis | `basis_bps` | (Mark/Index − 1) * 10000.【F:oraculo/detect/metrics_engine.py†L139-L148】 | Sí. | Signo/denominador distinto. | Revertir signo afectaría triggers R15/R16 y mean-revert R17/R18.【F:oraculo/alerts/runner.py†L1426-L1449】 |
| Velocity Basis | `basis_vel_bps_s_doc` | Δbasis_doc/Δt (bps/s) sobre ventana configurable.【F:oraculo/detect/metrics_engine.py†L139-L209】 | Sí. | Alineada (más clamp/ventana DOC). | Mantener `metric_source` para compatibilidad durante transición. |
| Accel Basis | `basis_accel_bps_s2_doc` | Segunda derivada de basis_doc sobre ventana configurable.【F:oraculo/detect/metrics_engine.py†L139-L209】 | Sí. | Nueva vs DOC legacy (antes faltante). | Permite habilitar reglas/alertas dependientes de curvatura de basis. |
| OI Δ% | `oi_delta_pct_doc` | (OI_t−OI_{t−Δ})/OI_{t−Δ} derivado en ingest REST y persistido.【F:oraculo/ingest/binance_rest.py†L125-L191】 | Sí. | Alineada (con fallback a `open_interest`). | Requiere mantener ventana/config para coherencia con reglas macro. |

## Componentes y dependencias relevantes
- **Persistencia**: `metrics_series` almacena todas las series de microestructura (columna `metric` + `window_s`).【F:SQL/SQL_ORACULO_BACKUP.sql†L124114-L124122】
- **Detección**: `DominanceDetector`, `DepletionDetector`, `BreakWallDetector`, `BasisMeanRevertDetector` y `MetricTriggerDetector` consumen directamente `dom_*`, `dep_*`, `refill_*_3s`, `basis_bps`, `basis_vel_bps_s`.【F:oraculo/detect/detectors.py†L261-L333】【F:oraculo/detect/detectors.py†L482-L511】【F:oraculo/detect/detectors.py†L523-L562】【F:oraculo/detect/detectors.py†L565-L604】
- **Alertas/Rules**: el runner propaga eventos de dominancia, depleción y basis a reglas R1/R2, R9/R10, R13/R14, R15/R18.【F:oraculo/alerts/runner.py†L1360-L1449】
- **Dashboards/consultas**: no hay referencias a `basis_bps`, `basis_vel_bps_s`, `dom_*`, `dep_*`, `refill_*` en `dashboards/pack-min.json` (búsqueda sin coincidencias); las únicas dependencias de nombres de métrica están en los detectores y en el pipeline de alertas citado arriba.

## Rule migration status
- **R9/R10 (Dominance)**: el CPU worker soporta `legacy|doc|auto`; en `auto` prioriza `dominance_*_doc` y cae a niveles legacy si la métrica DOC no está disponible.【F:oraculo/alerts/cpu_worker.py†L455-L486】
- **R15/R16 (Basis extremo)**: los triggers de basis permiten `metric_source` `legacy|doc|auto`, con `doc_sign_mode` para invertir el signo de `basis_bps_doc` y registrar la métrica usada en el evento.【F:oraculo/detect/detectors.py†L526-L564】
- **R17/R18 (Basis mean-revert)**: mean-revert usa `basis_bps_doc`/`basis_vel_bps_s_doc` con fallback legacy y expone en el evento las métricas usadas, `metric_source` y `doc_sign_mode`.【F:oraculo/detect/detectors.py†L573-L614】
- `doc_sign_mode` queda en `legacy` por defecto para preservar la interpretación actual mientras se migra el consumo a métricas DOC.【F:config/rules.yaml†L76-L94】

## Section 7 progress
- **Cobertura actual**: R1/R2 (break_wall+basis_vel), R3/R4 (absorción), R5–R8 (slicing agresivo/pasivo), R9/R10 (dominance), R11/R12 (spoofing), R13/R14 (depletion proxy), R15–R18 (basis extremo/mean-revert), R28/R29 (oi_spike, gated por `detectors.oi_spike.enabled=false`), R30/R31 (top_traders, gated por `detectors.top_traders.enabled=false`), R32/R33 (liq_cluster, gated por `detectors.liq_cluster.enabled=false`), R34 (basis_dislocation, gated por `detectors.basis_dislocation.enabled=false`) y R35 (skew_shock, gated por `detectors.skew_shock.enabled=false`) están implementadas en el engine con semántica legacy o parcial respecto al DOC.【F:oraculo/rules/engine.py†L16-L271】【F:config/rules.yaml†L130-L219】
- **Brechas DOC**: gamma flip y term structure invertida siguen sin reglas ni eventos equivalentes; los detectores opcionales (oi_spike/top_traders/liq_cluster/basis_dislocation/skew_shock) permanecen feature-flagged (`enabled=false`).【F:oraculo/rules/engine.py†L16-L271】【F:config/rules.yaml†L130-L219】
- **Convenciones DOC vs Proyecto**: al mencionar reglas numeradas fuera del catálogo de proyecto (p.ej., liquidaciones DOC-R17/R18) se usa el prefijo `DOC-`; las reglas activas actuales R17/R18 del proyecto corresponden al basis mean-revert legacy.
- **Preparación Opción A**: se reservaron slots R36–R37 para cubrir las brechas sin renumerar reglas existentes; los stubs documentan nombre, `event_type` y `side` esperados y no alteran el runtime hasta conectar detectores específicos.【F:oraculo/rules/engine.py†L16-L33】
- **R28/R29 (oi_spike)**: implementadas y feature-flagged (`detectors.oi_spike.enabled=false` por defecto) para evitar cambios en producción hasta habilitación explícita.【F:oraculo/rules/engine.py†L16-L33】【F:config/rules.yaml†L130-L149】
- **Auditoría oi_spike**: el evento reporta `metric_used_oi`/`metric_used_price`, usa `oi_delta_pct_doc` con fallback `open_interest` y momentum de `wmid` cuando esté disponible.【F:oraculo/alerts/cpu_worker.py†L395-L493】【F:oraculo/detect/macro_detectors.py†L37-L148】
- **R30/R31 (top_traders)**: detector macro con poll `poll_s`, cooldown `retrigger_s`, elección de métrica (`choose_by`) y auditoría de ratios/meta/scores; mapea a reglas R30/R31 con severidad por intensidad 0.40/0.60/0.80 y compuerta `detectors.top_traders.enabled=false`.【F:oraculo/detect/macro_detectors.py†L464-L586】【F:oraculo/alerts/cpu_worker.py†L380-L560】【F:config/rules.yaml†L150-L173】【F:oraculo/rules/engine.py†L16-L249】
- **R32/R33 (liq_cluster)**: implementadas y compuerta `detectors.liq_cluster.enabled=false` por defecto; auditan `sell_v`/`buy_v`, `momentum_usd`, `rebound`, `armed_anchor_wmid` y `armed_ts` para trazabilidad del ancla y la confirmación.【F:oraculo/detect/macro_detectors.py†L335-L454】【F:config/rules.yaml†L122-L148】
- **R35 (skew_shock)**: detector macro sobre RR25d (delta y velocidad) con clamps de seguridad, auditoría completa (buckets, delta/velocidad, thresholds normalizados) y cooldown `retrigger_s`; mapea a R35 y se mantiene desactivado por defecto hasta calibración.【F:oraculo/detect/macro_detectors.py†L99-L226】【F:config/rules.yaml†L201-L219】【F:oraculo/alerts/cpu_worker.py†L492-L607】【F:oraculo/rules/engine.py†L253-L271】
- **Dependencia de surface**: `skew_shock` requiere `oraculo.iv_surface_1m`, alimentada por `deribit.options_iv_surface`. El nuevo surface builder incremental (feature-flag `deribit_surface_builder.enabled=false`) evita escaneos completos y permite poblar la vista 1m sin cambiar semántica existente.【F:oraculo/deribit/surface_builder.py†L10-L241】【F:config/config.yaml†L77-L91】

## Post-fix (DOC vs legacy)
- Se añadieron las series DOC, preservando las legacy: `imbalance_doc`, `dominance_bid_doc`, `dominance_ask_doc`, `wmid`, `depletion_bid_doc`, `depletion_ask_doc`, `basis_bps_doc`, `basis_vel_bps_s_doc`, `basis_accel_bps_s2_doc` y `oi_delta_pct_doc`.【F:oraculo/alerts/cpu_worker.py†L480-L500】【F:oraculo/ingest/binance_rest.py†L125-L155】
- Fórmulas DOC aplicadas:
  - Dominance DOC = ΣBid_vol_topn / (ΣBid+ΣAsk) y complementario para ask, con media rolling 1–3s configurable.【F:oraculo/detect/metrics_engine.py†L129-L153】
  - Imbalance DOC = media rolling de la serie instantánea sobre 1–5s configurable.【F:oraculo/detect/metrics_engine.py†L119-L145】
  - Depletion DOC = Δvolumen top-n por lado en ventana 1–5s (delta absoluto, cubre replenishment con signo).【F:oraculo/detect/metrics_engine.py†L145-L153】
  - Wmid = (best_bid + best_ask)/2 tick a tick.【F:oraculo/detect/metrics_engine.py†L115-L123】
  - Basis DOC = (Index−Mark)/Mark en bps + derivadas 1ª y 2ª sobre ventana 60–300s configurable.【F:oraculo/detect/metrics_engine.py†L88-L117】
  - OI Δ% = (OI_t−OI_{t−Δ})/OI_{t−Δ} calculado en ingest REST y persistido con `window_s` configurable (default 120s).【F:oraculo/ingest/binance_rest.py†L129-L155】
- Ventanas configurables añadidas (defaults DOC): imbalance_doc=3s, dominance_doc=2s, depletion_doc=3s, basis_doc=120s, oi_doc=120s en reglas/config para hot-reload sin afectar legacy.【F:config/rules.yaml†L85-L92】【F:config/config.yaml†L8-L17】
- Regla de migración: las reglas/detectores actuales siguen consumiendo las métricas legacy; la migración a métricas DOC queda pendiente de un sprint posterior.
