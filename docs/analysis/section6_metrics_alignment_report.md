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
- **Código**: existe helper `spread_wmid` en `orderbook`, pero el `Snapshot` del engine no expone ni calcula `wmid`.【F:oraculo/detect/orderbook.py†L119-L125】【F:oraculo/detect/metrics_engine.py†L78-L209】
- **Persistencia**: no se inserta ninguna serie `wmid` en `metrics_series`.【F:oraculo/alerts/cpu_worker.py†L477-L493】
- **Estado**: H4 confirmada (no calculada ni persistida).

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
- **Código**: sólo deriva velocidad `basis_vel_bps_s` como diferencia de basis entre marcas consecutivas / Δt; no se calcula aceleración ni segunda derivada.【F:oraculo/detect/metrics_engine.py†L139-L148】【F:oraculo/detect/metrics_engine.py†L196-L209】
- **Persistencia**: sólo `basis_vel_bps_s` se guarda; no existe métrica de aceleración.【F:oraculo/alerts/cpu_worker.py†L485-L493】
- **Estado**: H5 confirmada (velocidad presente, aceleración ausente).

### OI Δ%
- **Código/BD**: se ingesta `open_interest` en tabla homónima, pero no se deriva ni publica `oi_delta_pct` en `metrics_series` ni en el `Snapshot`.【F:SQL/SQL_ORACULO_BACKUP.sql†L1020-L1044】【F:oraculo/alerts/cpu_worker.py†L477-L493】
- **Estado**: H6 confirmada (serie derivada no implementada).

## Validación de hipótesis H1–H6

| Hipótesis | Estado | Evidencia clave |
| --- | --- | --- |
| H1 Dominance Ask/Bid | Confirmada | Dominance usa conteo de niveles no nulos (no volumen).【F:oraculo/detect/metrics_engine.py†L151-L160】【F:oraculo/detect/detectors.py†L347-L384】 |
| H2 Basis | Confirmada | Código aplica `(mark/index−1)*10000` (signo/denominador invertidos vs DOC).【F:oraculo/detect/metrics_engine.py†L139-L148】【F:oraculo/ingest/binance_ws.py†L346-L356】 |
| H3 Depletion/Replenishment | Confirmada | Proxy ins/del 3s en vez de Δvolumen top-n.【F:oraculo/detect/metrics_engine.py†L171-L184】 |
| H4 Wmid | Confirmada | Snapshot no expone ni persiste `wmid`; sólo helper independiente.【F:oraculo/detect/orderbook.py†L119-L125】【F:oraculo/detect/metrics_engine.py†L78-L209】【F:oraculo/alerts/cpu_worker.py†L477-L493】 |
| H5 Velocity/Accel Basis | Confirmada | Sólo velocidad `basis_vel_bps_s`; no hay aceleración ni persistencia asociada.【F:oraculo/detect/metrics_engine.py†L139-L148】【F:oraculo/alerts/cpu_worker.py†L485-L493】 |
| H6 OI Δ% | Confirmada | BD tiene `open_interest`, pero no se deriva ni se almacena `oi_delta_pct`.【F:SQL/SQL_ORACULO_BACKUP.sql†L1020-L1044】【F:oraculo/alerts/cpu_worker.py†L477-L493】 |

## Tabla resumen DOC vs Código vs BD

| Métrica DOC | Nombre en código | Fórmula / proxy en código | ¿Persiste en metrics_series? | Diferencia vs DOC | Impacto probable |
| --- | --- | --- | --- | --- | --- |
| Imbalance | `imbalance` | (ΣBid−ΣAsk)/(ΣBid+ΣAsk) instantáneo sobre `top_n`.【F:oraculo/detect/metrics_engine.py†L162-L169】 | Sí (`window_s=1`).【F:oraculo/alerts/cpu_worker.py†L477-L493】 | Sin ventana 1–5s. | Cambiar a ventana temporal alteraría Depletion/BW gating que usa snapshots actuales. |
| Dominance Ask/Bid | `dom_bid` / `dom_ask` | Conteo de niveles no nulos por lado / total niveles.【F:oraculo/detect/metrics_engine.py†L151-L160】 | Sí. | No usa % volumen (significado distinto). | Cambiar semántica afectaría `DominanceDetector` y alertas R9/R10.【F:oraculo/detect/detectors.py†L347-L384】【F:oraculo/alerts/runner.py†L1402-L1415】 |
| Spread | `spread_usd` | best_ask − best_bid.【F:oraculo/detect/metrics_engine.py†L186-L199】 | Sí. | Alineada (unidad USD vs “tick” en DOC). | Impacto bajo. |
| Wmid | — | No se calcula; helper externo `(ask+bid)/2`.【F:oraculo/detect/orderbook.py†L119-L125】 | No. | Métrica faltante. | Detectores/reglas que requieran referencia mid-price no pueden habilitarse. |
| Depletion/Replenishment | `dep_bid` / `dep_ask` y `refill_*_3s` | Proxy: deletions/(ins+del) y ins/del (cap 1) en 3s.【F:oraculo/detect/metrics_engine.py†L171-L184】 | Sí. | No es Δvolumen top-n; ventana fija 3s. | Cambiar proxy rompería `DepletionDetector` y `BreakWallDetector`.【F:oraculo/detect/detectors.py†L261-L333】【F:oraculo/detect/detectors.py†L482-L511】 |
| Basis | `basis_bps` | (Mark/Index − 1) * 10000.【F:oraculo/detect/metrics_engine.py†L139-L148】 | Sí. | Signo/denominador distinto. | Revertir signo afectaría triggers R15/R16 y mean-revert R17/R18.【F:oraculo/alerts/runner.py†L1426-L1449】 |
| Velocity Basis | `basis_vel_bps_s` | Δbasis/Δt (bps/s).【F:oraculo/detect/metrics_engine.py†L139-L148】 | Sí. | Falta aceleración. | Cálculo de aceleración requerido por DOC no disponible para reglas futuras. |
| Accel Basis | — | No implementada. | No. | Falta completa. | Debe añadirse cálculo y persistencia para cumplir DOC. |
| OI Δ% | — | No implementada; sólo OI bruto en BD.【F:SQL/SQL_ORACULO_BACKUP.sql†L1020-L1044】 | No. | Falta completa. | Dashboards/reglas sobre cambios OI no disponibles. |

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
