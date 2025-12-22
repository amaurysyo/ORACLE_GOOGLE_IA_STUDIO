# Sección 7 — Mapeo DOC → Reglas reales y plan Opción A (R28+)

## Tabla origen (DOC R1–R22)
Reglas copiadas de `📘 Proyecto — Oráculo Btcusdt  V1 — ACTUALIZADO.docx`, sección 7.

| DOC_Rule | DOC_Name | DOC_Event_Type / Fuentes | DOC_Inputs | DOC_Thresholds | Notas |
| --- | --- | --- | --- | --- | --- |
| R1 | BW + basis vel (BUY) | Break wall + basis_vel | BW_n≥3, depletion_bid, basis_vel | basis_vel≥1.5bps/s, depletion_bid≥40% | Perfil EU/US/AS multiplicadores; suppress 90s |
| R2 | BW + basis vel (SELL) | Break wall + basis_vel | BW_n≥3, depletion_ask, basis_vel | basis_vel≤−1.5bps/s, depletion_ask≥40% | Igual a R1 lado ask |
| R3 | Absorción BUY | absorption | dur≥10s, vol≥450BTC | — | Drift permitido pequeño |
| R4 | Absorción SELL | absorption | dur≥10s, vol≥450BTC | — | — |
| R5 | Slicing agresivo BUY | burst_trades | k≥8, gap≤80ms, qty≥Qmin | — | Equal-size bursts |
| R6 | Slicing agresivo SELL | burst_trades | k≥8, gap≤80ms, qty≥Qmin | — | Equal-size bursts |
| R7 | Slicing pasivo BUY | maker_refill | freq≥3, persist≥Tmin | — | Refill continuo en bid |
| R8 | Slicing pasivo SELL | maker_refill | freq≥3, persist≥Tmin | — | Refill continuo en ask |
| R9 | Dominancia BID + spread | dominance | imbalance≥0.7, spread≤$2 | — | Dominancia de volumen top-n |
| R10 | Dominancia ASK + spread | dominance | imbalance≤−0.7, spread≤$2 | — | — |
| R11 | Spoofing BID | spoofing | wall≥3×bucket_mean, cancel<1s, matched_pct<α | — | muro lejos y retirada rápida |
| R12 | Spoofing ASK | spoofing | wall≥3×bucket_mean, cancel<1s, matched_pct<α | — | — |
| R13 | OI Spike + precio | oi_spike + price | ΔOI% con momentum_price | ΔOI≥β% | Confluencia con trades |
| R14 | Basis dislocation | basis + funding | |basis|≥δbps, |vel|≥1bps/s, funding_trend↑ | — | Incluye dirección funding |
| R15 | Top Traders LONG | top_trader ratios | top_pos_ratio_long, top_acc_ratio_long | ≥θL o ≥θA | Baja severidad |
| R16 | Top Traders SHORT | top_trader ratios | top_pos_ratio_short, top_acc_ratio_short | ≥θL o ≥θA | Baja severidad |
| R17 | Liquidation cluster SELL | liquidations | liq_qty_sell≥X en 60s, no_rebound>z ticks | — | Ventana 60s |
| R18 | Liquidation cluster BUY | liquidations | liq_qty_buy≥X en 60s, no_pullback>z ticks | — | Ventana 60s |
| R19 | IV spike (Deribit) | iv_spike | ΔIV_1m | ΔIV≥σ_iv bps | Confluencia vol options |
| R20 | Skew shock 25Δ | skew | ΔRR_25d | ΔRR≥κ bps | Shock en risk reversals |
| R21 | Gamma flip (GEX) | gex | sign(GEX) cambia, spot≈strike_atm | — | — |
| R22 | Term structure invertida | iv term structure | IV_front−IV_back≥λ bps, vol↑ | — | Inversión curva |

## Tabla 1 — DOC → Implementación real

| DOC_Rule | DOC_Name | Proyecto_Rule | Proyecto_Event_Type | Estado | Comentarios |
| --- | --- | --- | --- | --- | --- |
| R1 | BW + basis vel (BUY) | R1 | break_wall (buy) | PARTIAL | Usa proxy de depleción/refill legacy o DOC; thresholds de severidad derivados de basis_vel y distinta semántica de depletion.【F:oraculo/rules/engine.py†L54-L103】【F:oraculo/detect/detectors.py†L262-L356】 |
| R2 | BW + basis vel (SELL) | R2 | break_wall (sell) | PARTIAL | Mismo caso que R1 con lado ask.【F:oraculo/rules/engine.py†L54-L103】【F:oraculo/detect/detectors.py†L262-L356】 |
| R3 | Absorción BUY | R3 | absorption (buy) | MATCH | Coincide con dur≥10s y vol≥450BTC; sólo añade gate de drift en ticks.【F:oraculo/rules/engine.py†L105-L118】【F:oraculo/detect/detectors.py†L214-L244】 |
| R4 | Absorción SELL | R4 | absorption (sell) | MATCH | Igual a R3 lado sell.【F:oraculo/rules/engine.py†L105-L118】【F:oraculo/detect/detectors.py†L214-L244】 |
| R5 | Slicing agresivo BUY | R5 | slicing_aggr (buy) | PARTIAL | k_min=5 (no 8), qty_min=1BTC y severidad por k; permite modo iceberg/hitting.【F:oraculo/rules/engine.py†L120-L134】【F:config/rules.yaml†L2-L21】 |
| R6 | Slicing agresivo SELL | R6 | slicing_aggr (sell) | PARTIAL | Igual que R5 lado sell.【F:oraculo/rules/engine.py†L120-L134】【F:config/rules.yaml†L2-L21】 |
| R7 | Slicing pasivo BUY | R7 | slicing_pass (buy) | PARTIAL | Refill detectado por secuencia de órdenes iguales (k_min=6, qty_min=5); no mide persistencia DOC explícita.【F:oraculo/rules/engine.py†L136-L150】【F:config/rules.yaml†L23-L33】 |
| R8 | Slicing pasivo SELL | R8 | slicing_pass (sell) | PARTIAL | Igual que R7 lado sell.【F:oraculo/rules/engine.py†L136-L150】【F:config/rules.yaml†L23-L33】 |
| R9 | Dominancia BID + spread | R9 | dominance (buy) | DIVERGED | Dominancia mide % de niveles no nulos (no volumen) y usa dom_pct=80%, no imbalance 0.7 con spread gate.|【F:oraculo/rules/engine.py†L152-L165】【F:oraculo/detect/detectors.py†L347-L384】 |
| R10 | Dominancia ASK + spread | R10 | dominance (sell) | DIVERGED | Misma divergencia que R9.|【F:oraculo/rules/engine.py†L152-L165】【F:oraculo/detect/detectors.py†L347-L384】 |
| R11 | Spoofing BID | R11 | spoofing (buy) | DIVERGED | Heurística basada en pared lejana y cancel_rate; no usa bucket_mean ni matched_pct DOC.|【F:oraculo/rules/engine.py†L167-L179】【F:oraculo/detect/detectors.py†L386-L454】 |
| R12 | Spoofing ASK | R12 | spoofing (sell) | DIVERGED | Igual que R11 lado ask.|【F:oraculo/rules/engine.py†L167-L179】【F:oraculo/detect/detectors.py†L386-L454】 |
| R13 | OI Spike + precio | R28 (BUY), R29 (SELL) | oi_spike (buy/sell) | MATCH | Implementado como R28/R29 con `event_type=oi_spike`, lado buy/sell, inputs `oi_delta_pct_doc` (fallback `open_interest`) + momentum `wmid`; feature flag `detectors.oi_spike.enabled` (default false).【F:oraculo/rules/engine.py†L16-L33】【F:oraculo/rules/engine.py†L218-L241】【F:oraculo/alerts/cpu_worker.py†L395-L493】【F:config/rules.yaml†L130-L149】 |
| R14 | Basis dislocation | R34 | `basis_dislocation` (side na) | PARTIAL | Usa métricas DOC (`basis_bps_doc`, `basis_vel_bps_s_doc`) con fallback legacy en modo auto, compuerta funding_rate opcional (`require_funding_confirm`, `allow_emit_without_funding`) y cooldown; gateado por `detectors.basis_dislocation.enabled=false`. Funding trend se aproxima con dos puntos (ahora y t−Δ).【F:oraculo/detect/macro_detectors.py†L90-L253】【F:config/rules.yaml†L175-L198】【F:oraculo/alerts/cpu_worker.py†L380-L560】【F:oraculo/rules/engine.py†L1-L106】【F:oraculo/rules/engine.py†L200-L249】 |
| R15 | Top Traders LONG | R30 | top_traders (long) | MATCH | Detector macro `top_traders` combina ratios de account/position (normados contra `acc_warn/acc_strong` y `pos_warn/pos_strong`) y emite evento `top_traders` lado long; mapeado a R30 con severidad por intensidad. Gate `detectors.top_traders.enabled=false` y política `choose_by=require_both/max_score` configurable en YAML.【F:oraculo/detect/macro_detectors.py†L464-L586】【F:oraculo/alerts/cpu_worker.py†L380-L560】【F:config/rules.yaml†L150-L173】【F:oraculo/rules/engine.py†L16-L33】【F:oraculo/rules/engine.py†L220-L249】 |
| R16 | Top Traders SHORT | R31 | top_traders (short) | MATCH | Mismo detector `top_traders` selecciona sesgo short y mapea a R31; incluye auditoría completa (ratios, timestamps, scores, política usada) y cooldown `retrigger_s`. Gateado por `detectors.top_traders.enabled=false` (opt-in).【F:oraculo/detect/macro_detectors.py†L464-L586】【F:oraculo/alerts/cpu_worker.py†L380-L560】【F:config/rules.yaml†L150-L173】【F:oraculo/rules/engine.py†L16-L33】【F:oraculo/rules/engine.py†L220-L249】 |
| R17 | Liquidation cluster SELL | R32 | liq_cluster (sell) | MATCH/PARTIAL | Detector macro `liq_cluster` con ventana 60s, momentum/rebound `wmid` y compuerta `detectors.liq_cluster.enabled` (default false). Usa `sell_v`/`buy_v` en USD (clamp 50M) y expone ancla `armed_anchor_wmid`/`armed_ts` para auditoría.【F:oraculo/detect/macro_detectors.py†L335-L454】【F:config/rules.yaml†L122-L148】 |
| R18 | Liquidation cluster BUY | R33 | liq_cluster (buy) | MATCH/PARTIAL | Mismo detector `liq_cluster` lado buy, con momentum positivo requerido y bloqueo por rebound. Gobernado por `detectors.liq_cluster.enabled` (default false) y publica campos de auditoría de ancla/momentum/rebound.【F:oraculo/detect/macro_detectors.py†L335-L454】【F:config/rules.yaml†L122-L148】 |
| R19 | IV spike (Deribit) | R19 | iv_spike_up | PARTIAL | Detector de ΔIV% sobre ventana; no cruza con volumen de opciones ni lados.|【F:oraculo/detect/detectors.py†L860-L934】【F:oraculo/rules/engine.py†L243-L249】 |
| R20 | Skew shock 25Δ | R20 (iv_spike_down) | iv_spike_down | DIVERGED | R20 actual detecta IV a la baja; no hay evento de skew 25Δ.|【F:oraculo/detect/detectors.py†L860-L934】【F:oraculo/rules/engine.py†L243-L249】 |
| R21 | Gamma flip (GEX) | N/A | — | NOT_IMPLEMENTED | No se ingesta ni calcula GEX.|【F:oraculo/rules/engine.py†L251-L258】 |
| R22 | Term structure invertida | N/A | — | NOT_IMPLEMENTED | No hay detector de curva IV.|【F:oraculo/rules/engine.py†L251-L258】 |

## Tabla 2 — Plan de completitud (Opción A)

| DOC_Rule faltante | Nuevo Proyecto_Rule | Evento propuesto (et/side) | Datos ya existentes | Dependencias | Prioridad | Estado / notas |
| --- | --- | --- | --- | --- | --- | --- |
| R13 OI Spike + precio | R28 (BUY), R29 (SELL) | `oi_spike` + momentum_price, side buy/sell | `open_interest`, `oi_delta_pct_doc`, trades/price ya persistidos | Reutilizar poller OI, derivar ΔOI% y momentum spot; gatillar por lado | P0 | **IMPLEMENTED** — usa `event_type=oi_spike`, `side=buy/sell`, inputs `oi_delta_pct_doc` (fallback `open_interest`) + momentum `wmid`; feature flag `detectors.oi_spike.enabled` (default false). |
| R14 Basis dislocation | R34 | `basis_dislocation` (side na) | `basis_bps_doc`, `basis_vel_bps_s_doc`, funding (mark/index) | Calcular funding_trend o ingestar funding rate; compuertas por vel| P1 | **IMPLEMENTED** — detector macro `basis_dislocation` (DOC-first, fallback legacy en modo auto) con gating de basis+vel, confirmación opcional por funding (tendencia en ventana `funding_window_s`), cooldown `retrigger_s` y feature flag `detectors.basis_dislocation.enabled=false` para preservar semántica actual. |
| R15 Top Traders LONG | R30 | `top_traders` (bias=long) | Tablas `top_trader_account_ratio`, `top_trader_position_ratio` | Detector para ratios ≥θL/θA y timestamp consolidado | P1 | **IMPLEMENTED** — detector macro `top_traders` con cool-down `retrigger_s`, auditoría completa en `fields` y gating `detectors.top_traders.enabled=false`. |
| R16 Top Traders SHORT | R31 | `top_traders` (bias=short) | Mismas tablas top_trader_* | Detector espejo lado short | P1 | **IMPLEMENTED** — misma lógica top_traders lado short; severidad por intensidad 0.40/0.60/0.80. |
| R17 Liquidation cluster SELL | R32 | `liq_cluster` (sell) | Streams/tables de liquidations y trades + `wmid` momentum/rebound | Agregador 60s con condición de no_rebound | P0 | **IMPLEMENTED** — controlado por feature flag `detectors.liq_cluster.enabled` (default false); expone auditoría `sell_v`/`buy_v`/`momentum`/`rebound` y ancla `armed_anchor_wmid`/`armed_ts`. |
| R18 Liquidation cluster BUY | R33 | `liq_cluster` (buy) | Streams/tables de liquidations y trades + `wmid` momentum/rebound | Agregador 60s con condición de no_pullback | P0 | **IMPLEMENTED** — mismo detector `liq_cluster` lado buy bajo gate `detectors.liq_cluster.enabled` (default false); incluye campos de auditoría de volúmenes/momentum/rebound y ancla. |
| R20 Skew shock 25Δ | R35 | `skew_shock` (na) | OI/IV de opciones (skew 25Δ) en ingesta Deribit | Derivar RR25d y Δbps; thresholds κ | P2 | Stub reservado |
| R21 Gamma flip (GEX) | R36 | `gamma_flip` (na) | Greeks (gamma) ya presentes en tabla de options | Calcular GEX agregado, detectar cambio de signo con spot~ATM | P2 | Stub reservado |
| R22 Term structure invertida | R37 | `term_structure_inverted` (na) | IV surface (front/back) si se expone en ingesta | Derivar term structure y tendencia de vol | P2 | Stub reservado |

## Decisiones de diseño
- Se elige la **Opción A**: mantener numeración existente (R1–R27) y añadir reglas nuevas R28+ para cubrir las brechas del DOC, evitando renumerar o alterar semántica actual.
- Convención de eventos: `event_type` en snake_case (`oi_spike`, `top_traders`, `liq_cluster`, `basis_dislocation`, `skew_shock`, `gamma_flip`, `term_structure_inverted`) con `side` explícito (`buy`/`sell`/`na`/`bias`).
- Campos de auditoría recomendados: `metric_source` (legacy/doc/auto), `window_s`, `thresholds` usados, `metric_used_*` cuando se combine DOC/legacy, y `profile` del `RuleContext` para mantener compatibilidad de telemetría.
- Las reglas R28/R29 están implementadas y gobernadas por `detectors.oi_spike.enabled` (default false) para no alterar producción hasta habilitación explícita; R30/R31 `top_traders` y R34 `basis_dislocation` siguen el mismo patrón de feature flag (`detectors.top_traders.enabled=false`, `detectors.basis_dislocation.enabled=false`). R35–R37 permanecen reservadas como stubs sin lógica en el engine.
