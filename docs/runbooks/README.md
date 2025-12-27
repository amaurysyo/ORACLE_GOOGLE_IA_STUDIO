# Runbook global de cierre
- [docs/runbooks/ORACULO_CLOSURE.md](./ORACULO_CLOSURE.md)

# Índice de runbooks por detector

| detector | archivo runbook | reglas afectadas (Rxx) | estado | fuentes de datos principales |
|---|---|---|---|---|
| slicing_aggr | [docs/runbooks/slicing_aggr.md](./slicing_aggr.md) | R01–R03 (slicing family) | RBK-2 completo (percentiles) | Trades, depth, `oraculo.slice_events` |
| slicing_hit | [docs/runbooks/slicing_hit.md](./slicing_hit.md) | R01–R03 (slicing family) | RBK-2 completo (percentiles) | Trades, `oraculo.slice_events` |
| slicing_pass | [docs/runbooks/slicing_pass.md](./slicing_pass.md) | R01–R03 (slicing family) | RBK-2 completo (percentiles) | Depth, `oraculo.slice_events` |
| absorption | [docs/runbooks/absorption.md](./absorption.md) | R04 (absorción) | RBK-2 completo (percentiles) | Trades, depth, auditoría `audit_absorption_exact` |
| break_wall | [docs/runbooks/break_wall.md](./break_wall.md) | R09–R10 (ruptura de muro) | RBK-2 completo (legacy+DOC) | Depth, DOC depletion/refill, auditorías `/SQL` |
| dominance | [docs/runbooks/dominance.md](./dominance.md) | R09–R10 (dominancia) | RBK-2 completo (percentiles) | Depth, `metrics_series` dominancia |
| spoofing | [docs/runbooks/spoofing.md](./spoofing.md) | R11–R12 (spoofing) | RBK-2 completo (percentiles) | Depth + trades |
| depletion | [docs/runbooks/depletion.md](./depletion.md) | R13–R14 (depletion) | RBK-2 completo (legacy+DOC) | Depth, DOC depletion/refill |
| basis | [docs/runbooks/basis.md](./basis.md) | R15–R18 (basis) | RBK-2 completo (percentiles) | Mark/Index/spot, `metrics_series` basis/vel |
| options | [docs/runbooks/options.md](./options.md) | R19–R22 (opciones IV/OI) | RBK-2 completo (percentiles) | Options surface + OI |
| tape_pressure | [docs/runbooks/tape_pressure.md](./tape_pressure.md) | R23–R24 (tape pressure) | RBK-2 completo (percentiles) | Trades, spread gating |
| oi_spike | [docs/runbooks/oi_spike.md](./oi_spike.md) | R28–R29 (OI spike) | RBK-2 completo (percentiles) | OI DOC, wmid |
| liq_cluster | [docs/runbooks/liq_cluster.md](./liq_cluster.md) | R26–R27 (clusters liquidación) | RBK-2 completo (percentiles) | Liquidaciones + wmid |
| top_traders | [docs/runbooks/top_traders.md](./top_traders.md) | R30–R31 (top traders) | RBK-2 completo (percentiles) | Ratios account/position |
| basis_dislocation | [docs/runbooks/basis_dislocation.md](./basis_dislocation.md) | R34 (basis dislocation) | RBK-2 completo (percentiles) | Basis/vel DOC + funding |
| skew_shock | [docs/runbooks/skew_shock.md](./skew_shock.md) | R35 (opciones skew) | RBK-2 completo (percentiles) | Options surface |
| gamma_flip | [docs/runbooks/gamma_flip.md](./gamma_flip.md) | R36 (opciones gamma) | RBK-2 completo (percentiles) | Options greeks |
| iv_spike | [docs/runbooks/iv_spike.md](./iv_spike.md) | R19 (IV spike DOC/ticker) | RBK-2 completo (percentiles) | Options IV surface/ticker |
| term_structure_invert | [docs/runbooks/term_structure_invert.md](./term_structure_invert.md) | R37 (term structure) | RBK-2 completo (percentiles) | Options IV buckets |
| spread_squeeze | [docs/runbooks/spread_squeeze.md](./spread_squeeze.md) | R38 (spread squeeze) | RBK-2 completo (percentiles) | Depth + `metrics_series` spread |
