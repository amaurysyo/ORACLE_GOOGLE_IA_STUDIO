# ===============================================
# oraculo/alerts/runner.py
# ===============================================
from __future__ import annotations

import asyncio
import datetime as dt
import json
import math
from typing import Any, Dict, Optional, Tuple, List, Sequence

import aiohttp
from loguru import logger

from oraculo.db import DB
from oraculo.detect.metrics_engine import MetricsEngine
from oraculo.detect.detectors import (
    SlicingAggConfig, SlicingAggDetector,
    AbsorptionCfg, AbsorptionDetector,
    BreakWallCfg, BreakWallDetector,
    DominanceCfg, DominanceDetector,
    SlicingPassConfig, SlicingPassiveDetector,
    SpoofingCfg, SpoofingDetector,
    DepletionCfg, DepletionDetector,
    MetricTrigCfg, MetricTriggerDetector,
    BasisMRcfg, BasisMeanRevertDetector,
    TapePressureCfg, TapePressureDetector,
    IVSpikeCfg, IVSpikeDetector,
    OISkewCfg, OISkewDetector,
    Event,
)
from oraculo.rules.engine import eval_rules, RuleContext
from oraculo.rules.router import TelegramRouter


BINANCE_FUT_INST = "BINANCE:PERP:BTCUSDT"

# --- Normalización de severidad hacia el enum de BD (ALTA/MEDIA/BAJA)
_SEV_MAP = {
    "HIGH": "ALTA",
    "MEDIUM": "MEDIA",
    "LOW": "BAJA",
    "ALTA": "ALTA",
    "MEDIA": "MEDIA",
    "BAJA": "BAJA",
}
def _sev_norm(x: str) -> str:
    return _SEV_MAP.get(str(x).strip().upper(), "MEDIA")


def _parse_deribit_option(instrument_id: str) -> tuple[Optional[str], Optional[str]]:
    """
    Extrae (underlying, tipo) de un instrument_id canónico de Deribit OPTIONS.
    Ejemplo esperado:
      DERIBIT:OPTIONS:BTC-28NOV25-50000-C -> ("BTC", "C")
    """
    s = str(instrument_id)
    if s.endswith("-C"):
        opt_type = "C"
    elif s.endswith("-P"):
        opt_type = "P"
    else:
        return None, None

    tail = s.split(":")[-1]  # BTC-28NOV25-50000-C
    parts = tail.split("-")
    if not parts:
        return None, None
    underlying = parts[0]
    return underlying, opt_type


# ----------------- Auxiliares -----------------

class DBTail:
    """
    Clase para seguir la cola de una tabla (tailing) usando una columna cursor (ID o Tiempo).
    Evita perder filas con el mismo timestamp usando IDs únicos cuando es posible.
    """
    def __init__(self, db: DB, table: str, id_col: str, default_val: Any) -> None:
        self.db = db
        self.table = table
        self.id_col = id_col
        self.last_val: Any = default_val  # Valor del cursor (int o datetime)

    async def init_live(self, instrument_id: str) -> None:
        """Inicializa el cursor al valor MÁS ALTO actual para empezar en modo LIVE."""
        sql = f"SELECT MAX({self.id_col}) FROM {self.table} WHERE instrument_id=$1"
        val = await self.db.fetchval(sql, instrument_id)
        if val is not None:
            self.last_val = val
            logger.info(f"[{self.table}] Tail initialized LIVE at {self.id_col}={self.last_val}")
        else:
            logger.info(f"[{self.table}] Table empty or no data for {instrument_id}, starting from default.")

    async def fetch_new(self, instrument_id: str, limit: int = 2000) -> list[dict]:
        """Recupera filas nuevas donde id_col > last_val."""
        sql = f"""SELECT * FROM {self.table}
                  WHERE instrument_id=$1 AND {self.id_col} > $2
                  ORDER BY {self.id_col} ASC
                  LIMIT {limit}"""
        
        rows = await self.db.fetch(sql, instrument_id, self.last_val)
        
        if rows:
            # Actualizamos el cursor al último procesado
            self.last_val = rows[-1][self.id_col]
            
        return [dict(r) for r in rows]


async def fetch_orderbook_snapshot(
    session: aiohttp.ClientSession, symbol: str = "BTCUSDT", depth: int = 1000
) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
    url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit={depth}"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
        r.raise_for_status()
        d = await r.json()
        bids = [(float(p), float(q)) for p, q in d.get("bids", [])]
        asks = [(float(p), float(q)) for p, q in d.get("asks", [])]
        return bids, asks


def _evdict(ev: Event) -> Dict[str, Any]:
    return {
        "type": getattr(ev, "kind", None),
        "side": getattr(ev, "side", None),
        "price": getattr(ev, "price", None),
        "intensity": getattr(ev, "intensity", None),
        "fields": (getattr(ev, "fields", {}) or {}),
        "ts": getattr(ev, "ts", None),
    }


def _routing_to_dict(routing_cfg: Any) -> Dict[str, Any]:
    """Acepta pydantic u objeto similar y devuelve un dict plano."""
    if isinstance(routing_cfg, dict):
        return routing_cfg
    if hasattr(routing_cfg, "model_dump"):
        return routing_cfg.model_dump()
    if hasattr(routing_cfg, "dict"):
        return routing_cfg.dict()
    # fallback conservador
    return {"telegram": {}}


# ---- Telemetría (agregada y volcada a tabla oraculo.rule_telemetry) ----
class Telemetry:
    def __init__(self, db: DB, instrument_id: str, profile: str):
        self.db = db
        self.instrument_id = instrument_id
        self.profile = profile
        self._agg: Dict[Tuple[dt.datetime, str, str], Dict[str, int]] = {}
        self._last_flush = 0.0

    @staticmethod
    def _bucket(ts: float) -> dt.datetime:
        dt_ = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)
        return dt_.replace(second=0, microsecond=0)

    def bump(
        self,
        ts: float,
        rule: str,
        side: str,
        *,
        emitted: int = 0,
        disc_dom_spread: int = 0,
        disc_metrics_none: int = 0,
        disc_iv_missing: int = 0,
        disc_oi_missing: int = 0,
        disc_oi_low: int = 0,
        disc_basis_vel_low: int = 0,
        disc_dep_low: int = 0,
        disc_refill_high: int = 0,
        disc_top_levels_gate: int = 0,
    ) -> None:
        key = (self._bucket(ts), rule, side)
        d = self._agg.setdefault(
            key,
            {
                "emitted": 0,
                "disc_dom_spread": 0,
                "disc_metrics_none": 0,
                "disc_iv_missing": 0,
                "disc_oi_missing": 0,
                "disc_oi_low": 0,
                "disc_basis_vel_low": 0,
                "disc_dep_low": 0,
                "disc_refill_high": 0,
                "disc_top_levels_gate": 0,
            },
        )
        d["emitted"] += emitted
        d["disc_dom_spread"] += disc_dom_spread
        d["disc_metrics_none"] += disc_metrics_none
        d["disc_iv_missing"] += disc_iv_missing
        d["disc_oi_missing"] += disc_oi_missing
        d["disc_oi_low"] += disc_oi_low
        d["disc_basis_vel_low"] += disc_basis_vel_low
        d["disc_dep_low"] += disc_dep_low
        d["disc_refill_high"] += disc_refill_high
        d["disc_top_levels_gate"] += disc_top_levels_gate

    async def flush_if_needed(self) -> None:
        now = dt.datetime.now(dt.timezone.utc).timestamp()
        if (now - self._last_flush) < 15.0:
            return
        self._last_flush = now
        if not self._agg:
            return

        rows: List[
            Tuple[
                dt.datetime,
                str,
                str,
                str,
                str,
                int,
                int,
                int,
                int,
                int,
                int,
                int,
                int,
                int,
                int,
            ]
        ] = []
        for (ts_bucket, rule, side), c in list(self._agg.items()):
            rows.append(
                (
                    ts_bucket,
                    self.instrument_id,
                    self.profile,
                    rule,
                    side,
                    c["emitted"],
                    c["disc_dom_spread"],
                    c["disc_metrics_none"],
                    c["disc_iv_missing"],
                    c["disc_oi_missing"],
                    c["disc_oi_low"],
                    c["disc_basis_vel_low"],
                    c["disc_dep_low"],
                    c["disc_refill_high"],
                    c["disc_top_levels_gate"],
                )
            )
        sql = """
            INSERT INTO oraculo.rule_telemetry(
              ts_bucket, instrument_id, profile, rule, side,
              emitted, disc_dom_spread, disc_metrics_none,
              disc_iv_missing, disc_oi_missing, disc_oi_low,
              disc_basis_vel_low, disc_dep_low, disc_refill_high,
              disc_top_levels_gate
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
            ON CONFLICT (ts_bucket, instrument_id, profile, rule, side)
            DO UPDATE SET
              emitted = oraculo.rule_telemetry.emitted + EXCLUDED.emitted,
              disc_dom_spread = oraculo.rule_telemetry.disc_dom_spread + EXCLUDED.disc_dom_spread,
              disc_metrics_none = oraculo.rule_telemetry.disc_metrics_none + EXCLUDED.disc_metrics_none,
              disc_iv_missing = oraculo.rule_telemetry.disc_iv_missing + EXCLUDED.disc_iv_missing,
              disc_oi_missing = oraculo.rule_telemetry.disc_oi_missing + EXCLUDED.disc_oi_missing,
              disc_oi_low = oraculo.rule_telemetry.disc_oi_low + EXCLUDED.disc_oi_low,
              disc_basis_vel_low = oraculo.rule_telemetry.disc_basis_vel_low + EXCLUDED.disc_basis_vel_low,
              disc_dep_low = oraculo.rule_telemetry.disc_dep_low + EXCLUDED.disc_dep_low,
              disc_refill_high = oraculo.rule_telemetry.disc_refill_high + EXCLUDED.disc_refill_high,
              disc_top_levels_gate = oraculo.rule_telemetry.disc_top_levels_gate + EXCLUDED.disc_top_levels_gate
        """
        try:
            await self.db.execute_many(sql, rows)
            self._agg.clear()
        except Exception as e:
            logger.warning(f"[telemetry] flush failed: {e!s}")


def _apply_rules_to_detectors(
    rules: Dict[str, Any],
    det_slice_eq: SlicingAggDetector,
    det_slice_hit: SlicingAggDetector,
    det_abs: AbsorptionDetector,
    det_bw: BreakWallDetector,
    det_pass: SlicingPassiveDetector,
    det_dom: DominanceDetector,
    det_spoof: SpoofingDetector,
    dep_bid_det: DepletionDetector,
    dep_ask_det: DepletionDetector,
    basis_pos_trig: MetricTriggerDetector,
    basis_neg_trig: MetricTriggerDetector,
    basis_mr: BasisMeanRevertDetector,
    tape_det: TapePressureDetector,
    iv_det: IVSpikeDetector,
    oi_skew_det: OISkewDetector,
) -> None:
    det = (rules or {}).get("detectors", {}) or {}

    # slicing iceberg (equal)
    s = det.get("slicing_aggr") or {}
    det_slice_eq.cfg.gap_ms = int(s.get("gap_ms", det_slice_eq.cfg.gap_ms))
    det_slice_eq.cfg.k_min = int(s.get("k_min", det_slice_eq.cfg.k_min))
    det_slice_eq.cfg.qty_min = float(s.get("qty_min", det_slice_eq.cfg.qty_min))
    det_slice_eq.cfg.require_equal = bool(s.get("require_equal", det_slice_eq.cfg.require_equal))
    det_slice_eq.cfg.equal_tol_pct = float(s.get("equal_tol_pct", det_slice_eq.cfg.equal_tol_pct))
    det_slice_eq.cfg.equal_tol_abs = s.get("equal_tol_abs", det_slice_eq.cfg.equal_tol_abs)

    # slicing hit (non-equal)
    h = det.get("slicing_hit") or {}
    det_slice_hit.cfg.gap_ms = int(h.get("gap_ms", det_slice_hit.cfg.gap_ms))
    det_slice_hit.cfg.k_min = int(h.get("k_min", det_slice_hit.cfg.k_min))
    det_slice_hit.cfg.qty_min = float(h.get("qty_min", det_slice_hit.cfg.qty_min))
    det_slice_hit.cfg.require_equal = False  # fuerza modo "hitting"

    # absorción
    a = det.get("absorption") or {}
    det_abs.cfg.dur_s = float(a.get("dur_s", det_abs.cfg.dur_s))
    det_abs.cfg.vol_btc = float(a.get("vol_btc", det_abs.cfg.vol_btc))
    det_abs.cfg.max_price_drift_ticks = int(a.get("max_price_drift_ticks", det_abs.cfg.max_price_drift_ticks))
    det_abs.cfg.tick_size = float(a.get("tick_size", det_abs.cfg.tick_size))

    # break wall (con gating por depleción/refill/basis_vel)
    b = det.get("break_wall") or {}
    det_bw.cfg.n_min = int(b.get("n_min", det_bw.cfg.n_min))
    det_bw.cfg.dep_pct = float(b.get("dep_pct", det_bw.cfg.dep_pct))
    det_bw.cfg.basis_vel_abs_bps_s = float(b.get("basis_vel_abs_bps_s", det_bw.cfg.basis_vel_abs_bps_s))
    det_bw.cfg.require_depletion = bool(b.get("require_depletion", det_bw.cfg.require_depletion))
    det_bw.cfg.forbid_refill_under_pct = float(b.get("forbid_refill_under_pct", det_bw.cfg.forbid_refill_under_pct))
    det_bw.cfg.top_levels_gate = int(b.get("top_levels_gate", det_bw.cfg.top_levels_gate))
    det_bw.cfg.tick_size = float(b.get("tick_size", det_bw.cfg.tick_size))
    det_bw.cfg.refill_window_s = float(b.get("refill_window_s", det_bw.cfg.refill_window_s))
    det_bw.cfg.refill_min_pct = float(b.get("refill_min_pct", det_bw.cfg.refill_min_pct))

    # slicing pasivo
    p = det.get("slicing_pass") or {}
    det_pass.cfg.gap_ms = int(p.get("gap_ms", det_pass.cfg.gap_ms))
    det_pass.cfg.k_min = int(p.get("k_min", det_pass.cfg.k_min))
    det_pass.cfg.qty_min = float(p.get("qty_min", det_pass.cfg.qty_min))

    # dominancia
    d = det.get("dominance") or {}
    det_dom.cfg.enabled = bool(d.get("enabled", det_dom.cfg.enabled))
    det_dom.cfg.dom_pct = float(d.get("dom_pct", det_dom.cfg.dom_pct))
    det_dom.cfg.max_spread_usd = float(d.get("max_spread_usd", det_dom.cfg.max_spread_usd))
    det_dom.cfg.levels = int(d.get("levels", det_dom.cfg.levels))
    det_dom.cfg.hold_ms = int(d.get("hold_ms", det_dom.cfg.hold_ms))
    det_dom.cfg.retrigger_s = int(d.get("retrigger_s", det_dom.cfg.retrigger_s))

    # --- R11–R12 Spoofing: mapear bien los campos del YAML actual ---
    sp = det.get("spoofing") or {}
    per_side = sp.get("per_side") or {}
    wall_global = sp.get("wall_size_btc")
    wall_bid = (per_side.get("bid") or {}).get("wall_size_btc")
    wall_ask = (per_side.get("ask") or {}).get("wall_size_btc")
    wall_candidate = wall_global if wall_global is not None else (wall_bid if wall_bid is not None else wall_ask)
    if wall_candidate is not None:
        det_spoof.cfg.wall_size_btc = float(wall_candidate)

    # distance_ticks_min: aceptar distance_ticks o distance_ticks_min
    if "distance_ticks_min" in sp:
        det_spoof.cfg.distance_ticks_min = int(sp["distance_ticks_min"])
    elif "distance_ticks" in sp:
        det_spoof.cfg.distance_ticks_min = int(sp["distance_ticks"])

    # ventana de evaluación: window_s explícito o derivado de cancel_within_ms
    if "window_s" in sp:
        det_spoof.cfg.window_s = float(sp["window_s"])
    elif "cancel_within_ms" in sp:
        det_spoof.cfg.window_s = float(sp["cancel_within_ms"]) / 1000.0

    # ratio de cancelación: usamos max_cancel_ratio como umbral
    if "cancel_rate_min" in sp:
        det_spoof.cfg.cancel_rate_min = float(sp["cancel_rate_min"])
    elif "max_cancel_ratio" in sp:
        det_spoof.cfg.cancel_rate_min = float(sp["max_cancel_ratio"])

    # tolerancia de ejecución: explícita o derivada de min_exec_ratio * wall_size
    if "exec_tolerance_btc" in sp:
        det_spoof.cfg.exec_tolerance_btc = float(sp["exec_tolerance_btc"])
    elif "min_exec_ratio" in sp and det_spoof.cfg.wall_size_btc is not None:
        det_spoof.cfg.exec_tolerance_btc = float(sp["min_exec_ratio"]) * float(det_spoof.cfg.wall_size_btc)

    det_spoof.cfg.tick_size = float(sp.get("tick_size", det_spoof.cfg.tick_size))

    # --- R13–R14 Depletion masivo (por lado) ---
    dep = det.get("depletion") or {}
    enabled = dep.get("enabled")
    if enabled is not None:
        dep_bid_det.cfg.enabled = bool(enabled)
        dep_ask_det.cfg.enabled = bool(enabled)

    base_p = dep.get("pct_drop")
    if base_p is not None:
        dep_bid_det.cfg.pct_drop = float(base_p)
        dep_ask_det.cfg.pct_drop = float(base_p)
    if "pct_drop_bid" in dep:
        dep_bid_det.cfg.pct_drop = float(dep["pct_drop_bid"])
    if "pct_drop_ask" in dep:
        dep_ask_det.cfg.pct_drop = float(dep["pct_drop_ask"])

    # hold_ms: usar hold_ms directo; si no, aproximar desde window_s (segundos)
    if "hold_ms" in dep:
        hold_ms_val = int(dep["hold_ms"])
        dep_bid_det.cfg.hold_ms = hold_ms_val
        dep_ask_det.cfg.hold_ms = hold_ms_val
    elif "window_s" in dep:
        hold_ms_val = int(float(dep["window_s"]) * 1000.0)
        dep_bid_det.cfg.hold_ms = hold_ms_val
        dep_ask_det.cfg.hold_ms = hold_ms_val

    # retrigger
    if "retrigger_s" in dep:
        dep_bid_det.cfg.retrigger_s = int(dep["retrigger_s"])
        dep_ask_det.cfg.retrigger_s = int(dep["retrigger_s"])

    # --- Basis config común (R15–R18) ---
    basis_cfg = det.get("basis") or {}

    # --- R15–R16 Basis extremo ---
    bx = det.get("basis_extreme") or {}
    thr_pos = basis_pos_trig.cfg.threshold
    thr_neg = basis_neg_trig.cfg.threshold

    # Preferimos detectors.basis.extreme_pos_bps / extreme_neg_bps
    if "extreme_pos_bps" in basis_cfg:
        try:
            thr_pos = float(basis_cfg["extreme_pos_bps"])
        except (TypeError, ValueError):
            pass
    elif "thr_bps" in bx:
        try:
            thr_pos = float(bx["thr_bps"])
        except (TypeError, ValueError):
            pass

    if "extreme_neg_bps" in basis_cfg:
        try:
            thr_neg = float(basis_cfg["extreme_neg_bps"])
        except (TypeError, ValueError):
            pass
    else:
        # si no se define explícito, simétrico
        thr_neg = -abs(thr_pos)

    basis_pos_trig.cfg.threshold = thr_pos
    basis_neg_trig.cfg.threshold = thr_neg

    # --- R17–R18 Basis mean-revert ---
    # Dos formas: bloque legacy basis_mr o claves dentro de basis.*
    bmr = det.get("basis_mr") or {}
    if bmr:
        basis_mr.cfg.gate_abs_bps = float(bmr.get("gate_abs_bps", basis_mr.cfg.gate_abs_bps))
        basis_mr.cfg.vel_gate_abs = float(bmr.get("vel_gate_abs", basis_mr.cfg.vel_gate_abs))
        basis_mr.cfg.retrigger_s = int(bmr.get("retrigger_s", basis_mr.cfg.retrigger_s))
    elif basis_cfg:
        if "mr_cross_eps_bps" in basis_cfg:
            try:
                basis_mr.cfg.gate_abs_bps = float(basis_cfg["mr_cross_eps_bps"])
            except (TypeError, ValueError):
                pass
        if "mr_vel_gate_abs" in basis_cfg:
            try:
                basis_mr.cfg.vel_gate_abs = float(basis_cfg["mr_vel_gate_abs"])
            except (TypeError, ValueError):
                pass
        if "mr_hold_ms" in basis_cfg:
            try:
                basis_mr.cfg.retrigger_s = float(basis_cfg["mr_hold_ms"]) / 1000.0
            except (TypeError, ValueError):
                pass

    # tape pressure (R23/R24 – extras)
    tp = det.get("tape_pressure") or {}
    tape_det.cfg.window_s = float(tp.get("window_s", tape_det.cfg.window_s))
    tape_det.cfg.buy_thr = float(tp.get("buy_thr", tape_det.cfg.buy_thr))
    tape_det.cfg.sell_thr = float(tp.get("sell_thr", tape_det.cfg.sell_thr))
    tape_det.cfg.retrigger_s = int(tp.get("retrigger_s", tape_det.cfg.retrigger_s))

    # Opciones (Deribit): IV spikes (R19/R20) + OI skew (R21/R22)
    opt = det.get("options") or {}
    iv_cfg = opt.get("iv_spike") or {}
    oi_cfg = opt.get("oi_skew") or {}

    if iv_cfg:
        iv_det.cfg.window_s = float(iv_cfg.get("window_s", iv_det.cfg.window_s))
        iv_det.cfg.up_thresh_pct = float(iv_cfg.get("up_pct", iv_det.cfg.up_thresh_pct))
        iv_det.cfg.down_thresh_pct = float(iv_cfg.get("down_pct", iv_det.cfg.down_thresh_pct))
        if "retrigger_s" in iv_cfg:
            iv_det.cfg.retrigger_s = float(iv_cfg["retrigger_s"])

    if oi_cfg:
        min_calls = float(oi_cfg.get("min_calls", 0.0) or 0.0)
        min_puts = float(oi_cfg.get("min_puts", 0.0) or 0.0)
        min_total_candidate = min_calls + min_puts if (min_calls > 0.0 and min_puts > 0.0) else 0.0
        if min_total_candidate > 0.0:
            oi_skew_det.cfg.min_total_oi = min_total_candidate
        oi_skew_det.cfg.bull_ratio_min = float(oi_cfg.get("bull_ratio", oi_skew_det.cfg.bull_ratio_min))
        oi_skew_det.cfg.bear_ratio_min = float(oi_cfg.get("bear_ratio", oi_skew_det.cfg.bear_ratio_min))
        if "retrigger_s" in oi_cfg:
            oi_skew_det.cfg.retrigger_s = float(oi_cfg["retrigger_s"])

    logger.info(
        f"Rules hot-applied: slicing={s} absorption={a} bw={b} pass={p} dominance={d} "
        f"spoof={sp} dep={dep} basis={basis_cfg} basis_extreme={bx} "
        f"tape={tp} options_iv={iv_cfg} options_oi={oi_cfg}"
    )


# ----------------- Runner principal -----------------
async def run_pipeline(
    db: DB,
    routing_cfg: Dict[str, Any],
    rules_profile: str = "EU",
    cfg_mgr: Any | None = None,
) -> None:
    engine = MetricsEngine(top_n=1000)

    # Detectores
    det_slice_eq = SlicingAggDetector(SlicingAggConfig(require_equal=True, equal_tol_pct=0.0, equal_tol_abs=0.0))
    det_slice_hit = SlicingAggDetector(SlicingAggConfig(require_equal=False))
    det_pass = SlicingPassiveDetector(SlicingPassConfig())
    det_abs = AbsorptionDetector(AbsorptionCfg())
    det_bw = BreakWallDetector(BreakWallCfg())
    det_dom = DominanceDetector(DominanceCfg(), book=engine.book)
    det_spoof = SpoofingDetector(SpoofingCfg())
    dep_bid_det = DepletionDetector(DepletionCfg(side="buy"))
    dep_ask_det = DepletionDetector(DepletionCfg(side="sell"))
    basis_pos_trig = MetricTriggerDetector(MetricTrigCfg(metric="basis_bps", threshold=100.0, direction="above"))
    basis_neg_trig = MetricTriggerDetector(MetricTrigCfg(metric="basis_bps", threshold=-100.0, direction="below"))
    basis_mr = BasisMeanRevertDetector(BasisMRcfg())
    tape_det = TapePressureDetector(TapePressureCfg())
    # Detectores Deribit opciones (R19–R22)
    iv_det = IVSpikeDetector(IVSpikeCfg())
    oi_skew_det = OISkewDetector(OISkewCfg())

    # Hot rules apply (y suscripción a cambios)
    if cfg_mgr is not None:
        _apply_rules_to_detectors(
            cfg_mgr.rules,
            det_slice_eq,
            det_slice_hit,
            det_abs,
            det_bw,
            det_pass,
            det_dom,
            det_spoof,
            dep_bid_det,
            dep_ask_det,
            basis_pos_trig,
            basis_neg_trig,
            basis_mr,
            tape_det,
            iv_det,
            oi_skew_det,
        )

        async def _on_rules_change(new_rules: Dict[str, Any]) -> None:
            _apply_rules_to_detectors(
                new_rules,
                det_slice_eq,
                det_slice_hit,
                det_abs,
                det_bw,
                det_pass,
                det_dom,
                det_spoof,
                dep_bid_det,
                dep_ask_det,
                basis_pos_trig,
                basis_neg_trig,
                basis_mr,
                tape_det,
                iv_det,
                oi_skew_det,
            )

        cfg_mgr.subscribe_rules(_on_rules_change)

    # Router (admite pydantic)
    routing_dict = _routing_to_dict(routing_cfg)
    router = TelegramRouter(routing_dict, db=db, rate_limit_per_min=60)
    ctx = RuleContext(instrument_id=BINANCE_FUT_INST, profile=rules_profile, suppress_window_s=90)

    # Telemetría
    telemetry = Telemetry(db, BINANCE_FUT_INST, ctx.profile)

    # --- DB Tails configurados por ID ---
    # Trades: Paginamos por 'trade_id_ext' para no perder ráfagas del mismo milisegundo
    tail_trades = DBTail(db, "binance_futures.trades", id_col="trade_id_ext", default_val=0)
    
    # Depth: Paginamos por 'seq' (UpdateID)
    tail_depth = DBTail(db, "binance_futures.depth", id_col="seq", default_val=0)
    
    # Mark: Seguimos usando 'event_time' (como no tiene ID secuencial claro y es baja frecuencia, es seguro)
    tail_mark = DBTail(db, "binance_futures.mark_funding", id_col="event_time", default_val=dt.datetime.fromtimestamp(0, tz=dt.timezone.utc))

    # Inicialización LIVE (Obtener MAX ID actual)
    await tail_trades.init_live(BINANCE_FUT_INST)
    await tail_depth.init_live(BINANCE_FUT_INST)
    await tail_mark.init_live(BINANCE_FUT_INST)
    
    logger.info("DB tails initialized via ID (Robust High-Frequency Mode).")

    # Warmup OB snapshot
    try:
        timeout = aiohttp.ClientTimeout(total=6)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            bids, asks = await fetch_orderbook_snapshot(s, "BTCUSDT", depth=1000)
            for p, q in bids:
                engine.book.apply("buy", "insert", p, q)
            for p, q in asks:
                engine.book.apply("sell", "insert", p, q)
            logger.info("Initialized OB snapshot via REST (depth=1000).")
    except Exception as e:
        logger.warning(f"Failed to init OB snapshot: {e!s}")

    # Estado para Deribit opciones (R19–R22)
    last_deriv_opt_ts: dt.datetime = await db.fetchval(
        "SELECT COALESCE(MAX(event_time), now()) FROM deribit.options_ticker"
    )
    oi_last_by_instr: Dict[str, float] = {}
    oi_calls_by_und: Dict[str, float] = {}
    oi_puts_by_und: Dict[str, float] = {}

    # ---------- persistencia anidada ----------
    async def insert_slice(ev: Event) -> None:
        await db.execute(
            "INSERT INTO oraculo.slice_events("
            " instrument_id,event_time,event_type,side,intensity,price,duration_ms,fields,latency_ms,profile)"
            " VALUES ($1,$2,$3,$4::side_t,$5,$6,$7,$8::jsonb,$9,$10)"
            " ON CONFLICT DO NOTHING",
            BINANCE_FUT_INST,
            dt.datetime.fromtimestamp(ev.ts, tz=dt.timezone.utc),
            ev.kind,
            ev.side,
            ev.intensity,
            ev.price,
            int((ev.fields.get("dur_s") or 0) * 1000),
            json.dumps(ev.fields or {}),
            None,
            ctx.profile,
        )

    async def upsert_rule(rule: dict, event_ts: float):
        """UPSERT y devuelve (alert_id, ts_first_dt) para registrar el envío con FK válida."""
        try:
            sev_db = _sev_norm(rule["severity"])
            rows = await db.fetch(
                """
                WITH up AS (
                  SELECT oraculo.upsert_rule_alert(
                    $1, $2, $3, $4::severity_t, $5, $6::jsonb, $7, $8
                  ) AS id
                )
                SELECT r.id, r.ts_first
                FROM up
                JOIN oraculo.rule_alerts r ON r.id = up.id
            """,
                BINANCE_FUT_INST,
                dt.datetime.fromtimestamp(event_ts, tz=dt.timezone.utc),
                rule["rule"],
                sev_db,
                rule["dedup_key"],
                json.dumps(rule["context"] or {}),
                None,
                ctx.profile,
            )
            row = rows[0] if rows else None
            if row:
                return int(row["id"]), row["ts_first"]
            return None, None
        except Exception as e:
            logger.error(f"upsert_rule_alert failed: {e!s}")
            return None, None

    async def insert_metrics(
        rows: Sequence[Tuple[str, dt.datetime, int, str, float, Optional[str], str]]
    ) -> None:
        """Inserta en bloque en oraculo.metrics_series; si falla, fila a fila."""
        if not rows:
            return
        sql = (
            "INSERT INTO oraculo.metrics_series("
            " instrument_id,event_time,window_s,metric,value,profile,meta"
            ") VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb)"
            " ON CONFLICT DO NOTHING"
        )
        try:
            await db.execute_many(sql, rows)
        except Exception as e:
            logger.warning(f"[metrics] bulk insert failed ({e!s}); fallback row-by-row")
            for r in rows:
                try:
                    await db.execute(sql, *r)
                except Exception as e2:
                    logger.error(f"[metrics] drop row {r}: {e2!s}")

    # ------------------- bucle principal -------------------
    while True:
        # 1) depth -> book & métricas + slicing pasivo + spoofing
        # Paginación por ID (seq)
        for r in await tail_depth.fetch_new(BINANCE_FUT_INST, limit=5000):
            ts = r["event_time"].timestamp()
            engine.on_depth(ts, r["side"], r["action"], float(r["price"]), float(r["qty"]))

            # slicing pasivo
            if r["action"] == "insert" and float(r["qty"]) > 0:
                evp = det_pass.on_depth(ts, r["side"], float(r["price"]), float(r["qty"]))
                if evp:
                    await insert_slice(evp)
                    for rule in eval_rules(_evdict(evp), ctx):
                        alert_id, ts_first_dt = await upsert_rule(rule, evp.ts)
                        if alert_id is not None:
                            # telemetría de emisión
                            telemetry.bump(evp.ts, rule["rule"], rule.get("side", "na"), emitted=1)
                            text = (
                                f"#{rule['rule']} {evp.side.upper()} slicing_pass @ {evp.price} | "
                                f"qty={evp.intensity:.2f} BTC"
                            )
                            await router.send("rules", text, alert_id=alert_id, ts_first=ts_first_dt)

            # spoofing detector (necesita bests)
            bb, ba = engine.book.best()
            ev_sp = det_spoof.on_depth(ts, r["side"], r["action"], float(r["price"]), float(r["qty"]), bb, ba)
            if ev_sp:
                for rule in eval_rules(_evdict(ev_sp), ctx):
                    aid, t0 = await upsert_rule(rule, ev_sp.ts)
                    if aid is not None:
                        telemetry.bump(ev_sp.ts, rule["rule"], rule.get("side", "na"), emitted=1)
                        text = (
                            f"#{rule['rule']} {ev_sp.side.upper()} spoofing {ev_sp.intensity:.2f} BTC @ "
                            f"{ev_sp.price} | cancel={ev_sp.fields.get('cancel_rate'):.2f}"
                        )
                        await router.send("rules", text, alert_id=aid, ts_first=t0)

        # 2) mark funding -> basis
        # Paginación por Tiempo (legacy para mark)
        for r in await tail_mark.fetch_new(BINANCE_FUT_INST, limit=1000):
            engine.on_mark(
                r["event_time"].timestamp(),
                float(r.get("mark_price") or 0) or None,
                float(r.get("index_price") or 0) or None,
            )

        # 2b) Deribit opciones ticker -> IV spikes (R19/R20) + OI skew (R21/R22)
        # Nota: Mantenemos el polling directo por tiempo aquí ya que es otra tabla/lógica
        rows_opt = await db.fetch(
            """
            SELECT instrument_id,
                   event_time,
                   mark_iv,
                   open_interest
            FROM deribit.options_ticker
            WHERE event_time > $1
            ORDER BY event_time ASC
            LIMIT 5000
            """,
            last_deriv_opt_ts,
        )
        if rows_opt:
            last_deriv_opt_ts = rows_opt[-1]["event_time"]
            for r in rows_opt:
                ts_opt = r["event_time"].timestamp()
                inst_id = str(r["instrument_id"])
                mark_iv = r.get("mark_iv")
                oi_val = r.get("open_interest")

                # -------- IV spikes (R19/R20) --------
                if mark_iv is None or float(mark_iv) <= 0.0:
                    telemetry.bump(ts_opt, "R19/R20", "na", disc_iv_missing=1)
                else:
                    ev_iv = iv_det.on_iv(ts_opt, float(mark_iv))
                    if ev_iv is not None:
                        ev_iv.fields.setdefault("instrument_id", inst_id)
                        for rule in eval_rules(_evdict(ev_iv), ctx):
                            aid, t0 = await upsert_rule(rule, ev_iv.ts)
                            if aid is not None:
                                telemetry.bump(ev_iv.ts, rule["rule"], rule.get("side", "na"), emitted=1)
                                text = (
                                    f"#{rule['rule']} IV spike {ev_iv.intensity:.2f}% "
                                    f"({inst_id})"
                                )
                                await router.send("rules", text, alert_id=aid, ts_first=t0)

                # -------- OI skew (R21/R22) --------
                if oi_val is None:
                    telemetry.bump(ts_opt, "R21/R22", "na", disc_oi_missing=1)
                    continue

                underlying, opt_type = _parse_deribit_option(inst_id)
                if underlying is None or opt_type is None:
                    # Opcional: podrías añadir otro contador de parse-fail aquí
                    continue

                prev = oi_last_by_instr.get(inst_id)
                if prev is not None:
                    if opt_type == "C":
                        oi_calls_by_und[underlying] = oi_calls_by_und.get(underlying, 0.0) - float(prev)
                    else:
                        oi_puts_by_und[underlying] = oi_puts_by_und.get(underlying, 0.0) - float(prev)

                oi_last_by_instr[inst_id] = float(oi_val)

                if opt_type == "C":
                    oi_calls_by_und[underlying] = oi_calls_by_und.get(underlying, 0.0) + float(oi_val)
                else:
                    oi_puts_by_und[underlying] = oi_puts_by_und.get(underlying, 0.0) + float(oi_val)

                oi_c = oi_calls_by_und.get(underlying, 0.0)
                oi_p = oi_puts_by_und.get(underlying, 0.0)

                # Telemetría de descarte por OI insuficiente
                total_oi = oi_c + oi_p
                min_total_oi = float(getattr(oi_skew_det.cfg, "min_total_oi", 0.0) or 0.0)
                if min_total_oi > 0.0 and total_oi < min_total_oi:
                    telemetry.bump(ts_opt, "R21/R22", "na", disc_oi_low=1)

                ev_oi = oi_skew_det.on_oi(ts_opt, oi_c, oi_p)
                if ev_oi is not None:
                    ev_oi.fields.setdefault("underlying", underlying)
                    for rule in eval_rules(_evdict(ev_oi), ctx):
                        aid, t0 = await upsert_rule(rule, ev_oi.ts)
                        if aid is not None:
                            telemetry.bump(ev_oi.ts, rule["rule"], rule.get("side", "na"), emitted=1)
                            text = f"#{rule['rule']} OI skew {ev_oi.intensity:.2f} ({underlying})"
                            await router.send("rules", text, alert_id=aid, ts_first=t0)

        # 3) trades -> slicing (iceberg/hitting) + absorción + break_wall + tape_pressure + spoofing_exec
        # Paginación por ID (trade_id_ext)
        for r in await tail_trades.fetch_new(BINANCE_FUT_INST, limit=5000):
            ts = r["event_time"].timestamp()
            side = r["side"]
            px = float(r["price"])
            qty = float(r["qty"])

            # actualizar métricas por trade si tu engine lo usa
            engine.on_trade(ts, side, px, qty)

            # spoofing: registrar ejecuciones cerca del muro
            det_spoof.on_trade(ts, side, px, qty)

            # slicing iceberg/hitting
            ev1 = det_slice_eq.on_trade(ts, side, px, qty)
            ev2 = det_slice_hit.on_trade(ts, side, px, qty)
            for ev in filter(None, [ev1, ev2]):  # type: ignore
                await insert_slice(ev)  # persistir slice event
                for rule in eval_rules(_evdict(ev), ctx):
                    alert_id, ts_first_dt = await upsert_rule(rule, ev.ts)
                    if alert_id is not None:
                        telemetry.bump(ev.ts, rule["rule"], rule.get("side", "na"), emitted=1)
                        label = "iceberg" if ev.fields.get("mode") == "iceberg" else "hitting"
                        text = (
                            f"#{rule['rule']} {side.upper()} slicing_{label} @ {px} | "
                            f"qty={ev.intensity:.2f} BTC"
                        )
                        await router.send("rules", text, alert_id=alert_id, ts_first=ts_first_dt)

            # Absorción (usa bests actuales para validar drift)
            bb, ba = engine.book.best()
            det_abs.on_best(bb, ba)
            ev_abs = det_abs.on_trade(ts, side, px, qty)
            if ev_abs:
                for rule in eval_rules(_evdict(ev_abs), ctx):
                    alert_id, ts_first_dt = await upsert_rule(rule, ev_abs.ts)
                    if alert_id is not None:
                        telemetry.bump(ev_abs.ts, rule["rule"], rule.get("side", "na"), emitted=1)
                        text = (
                            f"#{rule['rule']} {ev_abs.side.upper()} absorption @ {ev_abs.price} | "
                            f"vol={ev_abs.intensity:.2f} BTC"
                        )
                        await router.send("rules", text, alert_id=alert_id, ts_first=ts_first_dt)

            # BreakWall: gating por snapshot (depleción/refill/basis_vel)
            snap = engine.get_snapshot()
            if ev1 or ev2:
                # Telemetría de descarte por falta de métricas (basis_vel)
                if getattr(snap, "basis_vel_bps_s", None) is None:
                    telemetry.bump(ts, "R1/R2", (ev1 or ev2).side, disc_metrics_none=1)
                e2, gating_reason = det_bw.on_slicing(
                    ts, ev1 or ev2, snap.__dict__ if hasattr(snap, "__dict__") else dict()
                )
                if e2:
                    for rule in eval_rules(_evdict(e2), ctx):
                        alert_id, ts_first_dt = await upsert_rule(rule, e2.ts)
                        if alert_id is not None:
                            telemetry.bump(e2.ts, rule["rule"], rule.get("side", "na"), emitted=1)
                            text = (
                                f"#{rule['rule']} {e2.side.upper()} break_wall @ {e2.price} | "
                                f"k={e2.fields.get('k')}"
                            )
                            await router.send("rules", text, alert_id=alert_id, ts_first=ts_first_dt)
                elif gating_reason:
                    reason_map = {
                        "basis_vel_low": {"disc_basis_vel_low": 1},
                        "dep_low": {"disc_dep_low": 1},
                        "refill_high": {"disc_refill_high": 1},
                        "top_levels_gate": {"disc_top_levels_gate": 1},
                    }
                    bump_kwargs = reason_map.get(gating_reason, {})
                    if bump_kwargs:
                        telemetry.bump(
                            ts, "R1/R2", (ev1 or ev2).side, **bump_kwargs,
                        )

            # tape pressure
            ev_tp = tape_det.on_trade(ts, side, px, qty)
            if ev_tp:
                for rule in eval_rules(_evdict(ev_tp), ctx):
                    aid, t0 = await upsert_rule(rule, ev_tp.ts)
                    if aid is not None:
                        telemetry.bump(ev_tp.ts, rule["rule"], rule.get("side", "na"), emitted=1)
                        text = (
                            f"#{rule['rule']} {ev_tp.side.upper()} tape_pressure "
                            f"{ev_tp.intensity:.2f} @ {ev_tp.price}"
                        )
                        await router.send("rules", text, alert_id=aid, ts_first=t0)

        # 4) Triggers de snapshot: dominance, depletion, basis_extreme, basis_mean-revert
        snap = engine.get_snapshot()
        ts_now = dt.datetime.now(dt.timezone.utc).timestamp()

        # Dominance (timer-based) con telemetría de descartes por spread
        if getattr(snap, "spread_usd", None) is None:
            telemetry.bump(ts_now, "R9/R10", "na", disc_metrics_none=1)
        elif float(snap.spread_usd or 0.0) > det_dom.cfg.max_spread_usd:
            telemetry.bump(ts_now, "R9/R10", "na", disc_dom_spread=1)
        else:
            evd = det_dom.maybe_emit(ts_now, spread_usd=float(snap.spread_usd or 0.0))
            if evd:
                for rule in eval_rules(_evdict(evd), ctx):
                    aid, t0 = await upsert_rule(rule, evd.ts)
                    if aid is not None:
                        telemetry.bump(evd.ts, rule["rule"], rule.get("side", "na"), emitted=1)
                        text = (
                            f"#{rule['rule']} {evd.side.upper()} dominance {evd.intensity:.1f}% "
                            f"@ {evd.price}"
                        )
                        await router.send("rules", text, alert_id=aid, ts_first=t0)

        # Depletion masivo (R13/R14)
        ev_dep_bid = dep_bid_det.on_snapshot(ts_now, snap.__dict__ if hasattr(snap, "__dict__") else dict())
        ev_dep_ask = dep_ask_det.on_snapshot(ts_now, snap.__dict__ if hasattr(snap, "__dict__") else dict())
        for evd in [ev_dep_bid, ev_dep_ask]:
            if evd:
                for rule in eval_rules(_evdict(evd), ctx):
                    aid, t0 = await upsert_rule(rule, evd.ts)
                    if aid is not None:
                        telemetry.bump(evd.ts, rule["rule"], rule.get("side", "na"), emitted=1)
                        text = f"#{rule['rule']} {evd.side.upper()} depletion {evd.intensity:.2f}"
                        await router.send("rules", text, alert_id=aid, ts_first=t0)

        # Basis extremos (R15/R16) a través de MetricTrigger
        if getattr(snap, "basis_bps", None) is None:
            telemetry.bump(ts_now, "R15/R16", "na", disc_metrics_none=1)
        else:
            ev_bpos = basis_pos_trig.on_snapshot(ts_now, snap.__dict__ if hasattr(snap, "__dict__") else dict())
            ev_bneg = basis_neg_trig.on_snapshot(ts_now, snap.__dict__ if hasattr(snap, "__dict__") else dict())
            for evb in [ev_bpos, ev_bneg]:
                if evb:
                    for rule in eval_rules(_evdict(evb), ctx):
                        aid, t0 = await upsert_rule(rule, evb.ts)
                        if aid is not None:
                            telemetry.bump(evb.ts, rule["rule"], rule.get("side", "na"), emitted=1)
                            text = f"#{rule['rule']} basis_bps={evb.intensity:.1f}"
                            await router.send("rules", text, alert_id=aid, ts_first=t0)

        # Basis mean-revert (R17/R18)
        if getattr(snap, "basis_bps", None) is None or getattr(snap, "basis_vel_bps_s", None) is None:
            telemetry.bump(ts_now, "R17/R18", "na", disc_metrics_none=1)
        else:
            ev_mr = basis_mr.on_snapshot(ts_now, snap.__dict__ if hasattr(snap, "__dict__") else dict())
            if ev_mr:
                for rule in eval_rules(_evdict(ev_mr), ctx):
                    aid, t0 = await upsert_rule(rule, ev_mr.ts)
                    if aid is not None:
                        telemetry.bump(ev_mr.ts, rule["rule"], rule.get("side", "na"), emitted=1)
                        text = (
                            f"#{rule['rule']} basis_mean_revert {ev_mr.side.upper()} | "
                            f"vel={ev_mr.intensity:.2f} bps/s"
                        )
                        await router.send("rules", text, alert_id=aid, ts_first=t0)

        # 5) Flush de métricas y telemetría (1 Hz)
        now_ts = dt.datetime.now(dt.timezone.utc).timestamp()
        if (now_ts - ts_now) >= 0:  # ts_now es de este mismo tick
            now_dt = dt.datetime.fromtimestamp(now_ts, tz=dt.timezone.utc)
            snap = engine.get_snapshot()
            rows: List[Tuple[str, dt.datetime, int, str, float, Optional[str], str]] = []

            def add(name: str, val):
                if val is None:
                    return
                if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
                    return
                rows.append((BINANCE_FUT_INST, now_dt, 1, name, float(val), ctx.profile, "{}"))

            add("spread_usd", getattr(snap, "spread_usd", None))
            add("basis_bps", getattr(snap, "basis_bps", None))
            add("basis_vel_bps_s", getattr(snap, "basis_vel_bps_s", None))
            add("dom_bid", getattr(snap, "dom_bid", None))
            add("dom_ask", getattr(snap, "dom_ask", None))
            add("imbalance", getattr(snap, "imbalance", None))
            add("dep_bid", getattr(snap, "dep_bid", None))
            add("dep_ask", getattr(snap, "dep_ask", None))
            add("refill_bid_3s", getattr(snap, "refill_bid_3s", None))
            add("refill_ask_3s", getattr(snap, "refill_ask_3s", None))

            await insert_metrics(rows)
            await telemetry.flush_if_needed()

        await asyncio.sleep(0.05)  # 50ms cadence