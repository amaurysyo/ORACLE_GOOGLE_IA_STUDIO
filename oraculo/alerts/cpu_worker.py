from __future__ import annotations

import asyncio
import math
import queue
import multiprocessing as mp
import time
from dataclasses import dataclass
import datetime as dt
from typing import Any, Dict, Optional, Sequence, Tuple
import threading

from loguru import logger

try:
    import asyncpg  # type: ignore
except Exception:  # pragma: no cover - dependencia opcional
    asyncpg = None

from oraculo.detect.metrics_engine import MetricsEngine, Snapshot
from oraculo.detect.detectors import (
    SlicingAggConfig,
    SlicingAggDetector,
    AbsorptionCfg,
    AbsorptionDetector,
    BreakWallCfg,
    BreakWallDetector,
    DominanceCfg,
    DominanceDetector,
    SlicingPassConfig,
    SlicingPassiveDetector,
    SpoofingCfg,
    SpoofingDetector,
    DepletionCfg,
    DepletionDetector,
    MetricTrigCfg,
    MetricTriggerDetector,
    BasisMRcfg,
    BasisMeanRevertDetector,
    TapePressureCfg,
    TapePressureDetector,
    Event,
)
from oraculo.detect.macro_detectors import (
    LiqClusterCfg,
    LiqClusterDetector,
    OISpikeCfg,
    OISpikeDetector,
    BasisDislocationCfg,
    BasisDislocationDetector,
    TopTradersCfg,
    TopTradersDetector,
    SkewShockCfg,
    SkewShockDetector,
    TermStructureInvertCfg,
    TermStructureInvertDetector,
)
from oraculo.obs import metrics as obs_metrics


@dataclass
class WorkerRequest:
    kind: str
    payload: Any
    enqueued_at: float


@dataclass
class WorkerResult:
    kind: str
    payload: Any
    processing_seconds: float
    enqueued_at: float


@dataclass
class DepthProcessResult:
    passive_event: Optional[Event]
    spoof_event: Optional[Event]


@dataclass
class TradeProcessResult:
    slice_equal: Optional[Event]
    slice_hit: Optional[Event]
    absorption: Optional[Event]
    tape_pressure: Optional[Event]
    snapshot: Snapshot
    break_wall_event: Optional[Event]
    break_wall_gating: Optional[str]


@dataclass
class SnapshotProcessResult:
    ts: float
    snapshot: Snapshot
    dominance_event: Optional[Event]
    dep_bid_event: Optional[Event]
    dep_ask_event: Optional[Event]
    basis_pos_event: Optional[Event]
    basis_neg_event: Optional[Event]
    basis_mr_event: Optional[Event]
    metric_rows: Sequence[Tuple[str, float, int, str, float, Optional[str], str]]


class _AsyncpgAdapter:
    def __init__(self, pool: Any, loop: asyncio.AbstractEventLoop):
        self.pool = pool
        self.loop = loop

    def fetchrow(self, sql: str, *args: Any):
        async def _run():
            async with self.pool.acquire() as con:
                return await con.fetchrow(sql, *args)

        try:
            return self.loop.run_until_complete(_run())
        except Exception:
            return None

    def fetch(self, sql: str, *args: Any):
        async def _run():
            async with self.pool.acquire() as con:
                return await con.fetch(sql, *args)

        try:
            return self.loop.run_until_complete(_run())
        except Exception:
            return None


def _build_oi_cfg(rules: Dict[str, Any]) -> OISpikeCfg:
    det = (rules or {}).get("detectors", {}) or {}
    oi_cfg_raw = det.get("oi_spike") or {}
    cfg = OISpikeCfg()
    cfg.enabled = bool(oi_cfg_raw.get("enabled", cfg.enabled))
    cfg.poll_s = float(oi_cfg_raw.get("poll_s", cfg.poll_s))
    cfg.retrigger_s = float(oi_cfg_raw.get("retrigger_s", cfg.retrigger_s))
    cfg.oi_window_s = float(oi_cfg_raw.get("oi_window_s", cfg.oi_window_s))
    cfg.momentum_window_s = float(oi_cfg_raw.get("momentum_window_s", cfg.momentum_window_s))
    cfg.oi_warn_pct = float(oi_cfg_raw.get("oi_warn_pct", cfg.oi_warn_pct))
    cfg.oi_strong_pct = float(oi_cfg_raw.get("oi_strong_pct", cfg.oi_strong_pct))
    cfg.mom_warn_usd = float(oi_cfg_raw.get("mom_warn_usd", cfg.mom_warn_usd))
    cfg.mom_strong_usd = float(oi_cfg_raw.get("mom_strong_usd", cfg.mom_strong_usd))
    cfg.require_same_dir = bool(oi_cfg_raw.get("require_same_dir", cfg.require_same_dir))
    return cfg


def _build_liq_cfg(rules: Dict[str, Any]) -> LiqClusterCfg:
    det = (rules or {}).get("detectors", {}) or {}
    raw = det.get("liq_cluster") or {}
    cfg = LiqClusterCfg()
    cfg.enabled = bool(raw.get("enabled", cfg.enabled))
    cfg.poll_s = float(raw.get("poll_s", cfg.poll_s))
    cfg.window_s = float(raw.get("window_s", cfg.window_s))
    cfg.retrigger_s = float(raw.get("retrigger_s", cfg.retrigger_s))
    cfg.warn_usd = float(raw.get("warn_usd", cfg.warn_usd))
    cfg.strong_usd = float(raw.get("strong_usd", cfg.strong_usd))
    cfg.confirm_s = float(raw.get("confirm_s", cfg.confirm_s))
    cfg.momentum_window_s = float(raw.get("momentum_window_s", cfg.momentum_window_s))
    cfg.min_move_usd = float(raw.get("min_move_usd", cfg.min_move_usd))
    cfg.max_rebound_usd = float(raw.get("max_rebound_usd", cfg.max_rebound_usd))
    cfg.use_usd = bool(raw.get("use_usd", cfg.use_usd))
    cfg.clamp_usd = float(raw.get("clamp_usd", cfg.clamp_usd))
    return cfg


def _build_top_traders_cfg(rules: Dict[str, Any]) -> TopTradersCfg:
    det = (rules or {}).get("detectors", {}) or {}
    raw = det.get("top_traders") or {}
    cfg = TopTradersCfg()
    cfg.enabled = bool(raw.get("enabled", cfg.enabled))
    cfg.poll_s = float(raw.get("poll_s", cfg.poll_s))
    cfg.retrigger_s = float(raw.get("retrigger_s", cfg.retrigger_s))
    cfg.acc_warn = float(raw.get("acc_warn", cfg.acc_warn))
    cfg.acc_strong = float(raw.get("acc_strong", cfg.acc_strong))
    cfg.pos_warn = float(raw.get("pos_warn", cfg.pos_warn))
    cfg.pos_strong = float(raw.get("pos_strong", cfg.pos_strong))
    cfg.require_both = bool(raw.get("require_both", cfg.require_both))
    cfg.choose_by = str(raw.get("choose_by", cfg.choose_by))
    return cfg


def _build_basis_dislocation_cfg(rules: Dict[str, Any]) -> BasisDislocationCfg:
    det = (rules or {}).get("detectors", {}) or {}
    raw = det.get("basis_dislocation") or {}
    return BasisDislocationCfg(
        enabled=bool(raw.get("enabled", False)),
        poll_s=float(raw.get("poll_s", 10.0)),
        retrigger_s=float(raw.get("retrigger_s", 180.0)),
        metric_source=str(raw.get("metric_source", "auto")),
        basis_warn_bps=float(raw.get("basis_warn_bps", 60.0)),
        basis_strong_bps=float(raw.get("basis_strong_bps", 120.0)),
        vel_warn_bps_s=float(raw.get("vel_warn_bps_s", 1.0)),
        vel_strong_bps_s=float(raw.get("vel_strong_bps_s", 2.0)),
        require_funding_confirm=bool(raw.get("require_funding_confirm", True)),
        funding_window_s=float(raw.get("funding_window_s", 900.0)),
        funding_trend_warn=float(raw.get("funding_trend_warn", 0.00001)),
        funding_trend_strong=float(raw.get("funding_trend_strong", 0.00003)),
        allow_emit_without_funding=bool(raw.get("allow_emit_without_funding", False)),
        clamp_abs_basis_bps=float(raw.get("clamp_abs_basis_bps", 1000.0)),
        clamp_abs_vel_bps_s=float(raw.get("clamp_abs_vel_bps_s", 50.0)),
    )


def _build_skew_shock_cfg(rules: Dict[str, Any]) -> SkewShockCfg:
    det = (rules or {}).get("detectors", {}) or {}
    raw = det.get("skew_shock") or {}
    cfg = SkewShockCfg()
    cfg.enabled = bool(raw.get("enabled", cfg.enabled))
    cfg.poll_s = float(raw.get("poll_s", cfg.poll_s))
    cfg.retrigger_s = float(raw.get("retrigger_s", cfg.retrigger_s))
    cfg.underlying = str(raw.get("underlying", cfg.underlying))
    cfg.window_s = float(raw.get("window_s", cfg.window_s))
    cfg.delta_warn = float(raw.get("delta_warn", cfg.delta_warn))
    cfg.delta_strong = float(raw.get("delta_strong", cfg.delta_strong))
    cfg.vel_warn_per_s = float(raw.get("vel_warn_per_s", cfg.vel_warn_per_s))
    cfg.vel_strong_per_s = float(raw.get("vel_strong_per_s", cfg.vel_strong_per_s))
    cfg.clamp_abs_rr = float(raw.get("clamp_abs_rr", cfg.clamp_abs_rr))
    cfg.clamp_abs_delta = float(raw.get("clamp_abs_delta", cfg.clamp_abs_delta))
    cfg.clamp_abs_vel_per_s = float(raw.get("clamp_abs_vel_per_s", cfg.clamp_abs_vel_per_s))
    cfg.require_recent_past = bool(raw.get("require_recent_past", cfg.require_recent_past))
    return cfg


def _build_term_structure_invert_cfg(rules: Dict[str, Any]) -> TermStructureInvertCfg:
    det = (rules or {}).get("detectors", {}) or {}
    raw = det.get("term_structure_invert") or {}
    cfg = TermStructureInvertCfg()
    cfg.enabled = bool(raw.get("enabled", cfg.enabled))
    cfg.poll_s = float(raw.get("poll_s", cfg.poll_s))
    cfg.retrigger_s = float(raw.get("retrigger_s", cfg.retrigger_s))
    cfg.underlying = str(raw.get("underlying", cfg.underlying))
    cfg.lookback_s = float(raw.get("lookback_s", cfg.lookback_s))
    cfg.short_tenor_bucket = str(raw.get("short_tenor_bucket", cfg.short_tenor_bucket))
    cfg.long_tenor_bucket = str(raw.get("long_tenor_bucket", cfg.long_tenor_bucket))
    cfg.moneyness_bucket = str(raw.get("moneyness_bucket", cfg.moneyness_bucket))
    cfg.spread_warn = float(raw.get("spread_warn", cfg.spread_warn))
    cfg.spread_strong = float(raw.get("spread_strong", cfg.spread_strong))
    cfg.vel_warn_per_s = float(raw.get("vel_warn_per_s", cfg.vel_warn_per_s))
    cfg.vel_strong_per_s = float(raw.get("vel_strong_per_s", cfg.vel_strong_per_s))
    cfg.use_velocity_gate = bool(raw.get("use_velocity_gate", cfg.use_velocity_gate))
    cfg.clamp_abs_iv = float(raw.get("clamp_abs_iv", cfg.clamp_abs_iv))
    cfg.clamp_abs_spread = float(raw.get("clamp_abs_spread", cfg.clamp_abs_spread))
    cfg.clamp_abs_vel_per_s = float(raw.get("clamp_abs_vel_per_s", cfg.clamp_abs_vel_per_s))
    cfg.require_both_present = bool(raw.get("require_both_present", cfg.require_both_present))
    cfg.require_positive_inversion = bool(raw.get("require_positive_inversion", cfg.require_positive_inversion))
    return cfg


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
) -> None:
    rules = rules or {}

    def _apply_dep_cfg(
        detector: DepletionDetector,
        side: str,
        dep_cfg: Dict[str, Any],
        dep_root: Dict[str, Any],
        metrics_doc_cfg: Dict[str, Any],
    ) -> None:
        pct_candidates = [
            dep_cfg.get("pct_drop"),
            dep_cfg.get("pct"),
            dep_root.get("pct_drop"),
            dep_root.get(f"pct_drop_{side}"),
            dep_root.get(f"pct_{side}"),
            rules.get("depletion", {}).get(f"pct_drop_{side}"),
            rules.get("depletion", {}).get(f"pct_{side}"),
        ]
        pct_val = next((v for v in pct_candidates if v is not None), None)
        if pct_val is not None:
            detector.cfg.pct_drop = float(pct_val)

        hold_val = dep_cfg.get("hold_ms", dep_root.get("hold_ms", rules.get("depletion", {}).get("hold_ms")))
        if hold_val is not None:
            detector.cfg.hold_ms = int(hold_val)

        retrigger_val = dep_cfg.get("retrigger_s", dep_root.get("retrigger_s", rules.get("depletion", {}).get("retrigger_s")))
        if retrigger_val is not None:
            detector.cfg.retrigger_s = float(retrigger_val)

        enabled_val = dep_cfg.get("enabled", dep_root.get("enabled", rules.get("depletion", {}).get("enabled")))
        if enabled_val is not None:
            detector.cfg.enabled = bool(enabled_val)

        metric_source_val = dep_cfg.get("metric_source", dep_root.get("metric_source"))
        if metric_source_val is not None:
            detector.cfg.metric_source = str(metric_source_val)

        doc_cfg = dep_root.get("doc") or {}
        detector.cfg.doc_window_s = float(doc_cfg.get("window_s", metrics_doc_cfg.get("depletion_doc_window_s", detector.cfg.doc_window_s)))
        if "dv_warn" in doc_cfg:
            detector.cfg.dv_warn = float(doc_cfg["dv_warn"])
        if "dv_strong" in doc_cfg:
            detector.cfg.dv_strong = float(doc_cfg["dv_strong"])
        if "clamp_abs_dv" in doc_cfg:
            detector.cfg.clamp_abs_dv = float(doc_cfg["clamp_abs_dv"])
        if "severity_mode" in doc_cfg:
            detector.cfg.severity_mode = str(doc_cfg["severity_mode"])
        if "require_negative" in doc_cfg:
            detector.cfg.require_negative = bool(doc_cfg["require_negative"])

    det = (rules or {}).get("detectors", {}) or {}
    metrics_doc_cfg = det.get("metrics_doc") or {}

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
    det_slice_hit.cfg.require_equal = False

    # absorción
    a = det.get("absorption") or {}
    det_abs.cfg.dur_s = float(a.get("dur_s", det_abs.cfg.dur_s))
    det_abs.cfg.vol_btc = float(a.get("vol_btc", det_abs.cfg.vol_btc))
    det_abs.cfg.max_price_drift_ticks = int(a.get("max_price_drift_ticks", det_abs.cfg.max_price_drift_ticks))
    det_abs.cfg.tick_size = float(a.get("tick_size", det_abs.cfg.tick_size))

    # break wall
    b = det.get("break_wall") or {}
    det_bw.cfg.n_min = int(b.get("n_min", det_bw.cfg.n_min))
    det_bw.cfg.dep_pct = float(b.get("dep_pct", det_bw.cfg.dep_pct))
    det_bw.cfg.basis_vel_abs_bps_s = float(b.get("basis_vel_abs_bps_s", det_bw.cfg.basis_vel_abs_bps_s))
    det_bw.cfg.metric_source = str(b.get("metric_source", det_bw.cfg.metric_source))
    doc_bw = b.get("doc") or {}
    det_bw.cfg.doc_window_s = float(doc_bw.get("window_s", metrics_doc_cfg.get("depletion_doc_window_s", det_bw.cfg.doc_window_s)))
    if "dv_dep_warn" in doc_bw:
        det_bw.cfg.dv_dep_warn = float(doc_bw["dv_dep_warn"])
    if "dv_refill_max" in doc_bw:
        det_bw.cfg.dv_refill_max = float(doc_bw["dv_refill_max"])
    if "clamp_abs_dv" in doc_bw:
        det_bw.cfg.clamp_abs_dv = float(doc_bw["clamp_abs_dv"])
    if "require_negative" in doc_bw:
        det_bw.cfg.require_negative = bool(doc_bw["require_negative"])
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
    det_dom.cfg.dom_pct_doc = float(d.get("dom_pct_doc", det_dom.cfg.dom_pct_doc))
    det_dom.cfg.metric_source = str(d.get("metric_source", det_dom.cfg.metric_source))
    det_dom.cfg.max_spread_usd = float(d.get("max_spread_usd", det_dom.cfg.max_spread_usd))
    det_dom.cfg.levels = int(d.get("levels", det_dom.cfg.levels))
    det_dom.cfg.hold_ms = int(d.get("hold_ms", det_dom.cfg.hold_ms))
    det_dom.cfg.retrigger_s = int(d.get("retrigger_s", det_dom.cfg.retrigger_s))

    # spoofing
    sp = det.get("spoofing") or {}
    per_side = sp.get("per_side") or {}
    wall_global = sp.get("wall_size_btc")
    wall_bid = (per_side.get("bid") or {}).get("wall_size_btc")
    wall_ask = (per_side.get("ask") or {}).get("wall_size_btc")
    wall_candidate = wall_global if wall_global is not None else (wall_bid if wall_bid is not None else wall_ask)
    if wall_candidate is not None:
        det_spoof.cfg.wall_size_btc = float(wall_candidate)

    if "distance_ticks_min" in sp:
        det_spoof.cfg.distance_ticks_min = int(sp["distance_ticks_min"])
    elif "distance_ticks" in sp:
        det_spoof.cfg.distance_ticks_min = int(sp["distance_ticks"])

    if "window_s" in sp:
        det_spoof.cfg.window_s = float(sp["window_s"])
    elif "cancel_within_ms" in sp:
        det_spoof.cfg.window_s = float(sp["cancel_within_ms"]) / 1000.0

    if "cancel_rate_min" in sp:
        det_spoof.cfg.cancel_rate_min = float(sp["cancel_rate_min"])
    elif "max_cancel_ratio" in sp:
        det_spoof.cfg.cancel_rate_min = float(sp["max_cancel_ratio"])

    if "exec_tolerance_btc" in sp:
        det_spoof.cfg.exec_tolerance_btc = float(sp["exec_tolerance_btc"])
    elif "min_exec_ratio" in sp and det_spoof.cfg.wall_size_btc is not None:
        det_spoof.cfg.exec_tolerance_btc = float(sp["min_exec_ratio"]) * float(det_spoof.cfg.wall_size_btc)

    det_spoof.cfg.tick_size = float(sp.get("tick_size", det_spoof.cfg.tick_size))

    dep = det.get("depletion") or {}
    dep_bid = dep.get("bid") or {}
    dep_ask = dep.get("ask") or {}
    _apply_dep_cfg(dep_bid_det, "bid", dep_bid, dep, metrics_doc_cfg)
    _apply_dep_cfg(dep_ask_det, "ask", dep_ask, dep, metrics_doc_cfg)

    basis_cfg = det.get("basis") or {}
    bx = basis_cfg.get("extreme") or {}
    basis_pos_trig.cfg.threshold = float(bx.get("pos_bps", basis_pos_trig.cfg.threshold))
    basis_neg_trig.cfg.threshold = float(bx.get("neg_bps", basis_neg_trig.cfg.threshold))
    basis_pos_trig.cfg.metric_source = str(basis_cfg.get("metric_source", basis_pos_trig.cfg.metric_source))
    basis_neg_trig.cfg.metric_source = str(basis_cfg.get("metric_source", basis_neg_trig.cfg.metric_source))
    basis_pos_trig.cfg.doc_sign_mode = str(basis_cfg.get("doc_sign_mode", basis_pos_trig.cfg.doc_sign_mode))
    basis_neg_trig.cfg.doc_sign_mode = str(basis_cfg.get("doc_sign_mode", basis_neg_trig.cfg.doc_sign_mode))

    bmr = basis_cfg.get("mean_revert") or {}
    if bmr:
        basis_mr.cfg.window_s = float(bmr.get("window_s", basis_mr.cfg.window_s))
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
    basis_mr.cfg.metric_source = str(basis_cfg.get("metric_source", basis_mr.cfg.metric_source))
    basis_mr.cfg.doc_sign_mode = str(basis_cfg.get("doc_sign_mode", basis_mr.cfg.doc_sign_mode))

    tp = det.get("tape_pressure") or {}
    tape_det.cfg.window_s = float(tp.get("window_s", tape_det.cfg.window_s))
    tape_det.cfg.buy_thr = float(tp.get("buy_thr", tape_det.cfg.buy_thr))
    tape_det.cfg.sell_thr = float(tp.get("sell_thr", tape_det.cfg.sell_thr))
    tape_det.cfg.retrigger_s = int(tp.get("retrigger_s", tape_det.cfg.retrigger_s))

    logger.info(
        f"[cpu-worker] Rules applied: slicing={s} absorption={a} bw={b} pass={p} "
        f"dominance={d} spoof={sp} dep={dep} basis={basis_cfg} tape={tp}"
    )


class CPUWorkerProcess(mp.Process):
    def __init__(
        self,
        in_queue: mp.Queue,
        out_queue: mp.Queue,
        *,
        rules: Optional[Dict[str, Any]],
        instrument_id: str,
        profile: str,
        db_dsn: Optional[str] = None,
    ):
        super().__init__(daemon=True)
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.rules = rules or {}
        self.instrument_id = instrument_id
        self.profile = profile
        self.db_dsn = db_dsn

    def run(self) -> None:  # type: ignore[override]
        faulthandler = __import__("faulthandler")
        faulthandler.enable()
        det_rules = (self.rules or {}).get("detectors") or {}
        metrics_doc_cfg = det_rules.get("metrics_doc") or {}
        imbalance_doc_window_s = float(metrics_doc_cfg.get("imbalance_doc_window_s", 3.0))
        dominance_doc_window_s = float(metrics_doc_cfg.get("dominance_doc_window_s", 2.0))
        depletion_doc_window_s = float(metrics_doc_cfg.get("depletion_doc_window_s", 3.0))
        basis_doc_window_s = float(metrics_doc_cfg.get("basis_doc_window_s", 120.0))
        engine = MetricsEngine(
            top_n=1000,
            imbalance_doc_window_s=imbalance_doc_window_s,
            dominance_doc_window_s=dominance_doc_window_s,
            depletion_doc_window_s=depletion_doc_window_s,
            basis_doc_window_s=basis_doc_window_s,
        )

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

        _apply_rules_to_detectors(
            self.rules,
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
        )

        oi_cfg = _build_oi_cfg(self.rules)
        oi_detector = OISpikeDetector(oi_cfg, self.instrument_id)
        liq_cfg = _build_liq_cfg(self.rules)
        liq_detector = LiqClusterDetector(liq_cfg, self.instrument_id)
        top_traders_cfg = _build_top_traders_cfg(self.rules)
        top_traders_detector = TopTradersDetector(top_traders_cfg, self.instrument_id)
        basis_dislocation_cfg = _build_basis_dislocation_cfg(self.rules)
        basis_dislocation_detector = BasisDislocationDetector(basis_dislocation_cfg, self.instrument_id)
        skew_cfg = _build_skew_shock_cfg(self.rules)
        skew_detector = SkewShockDetector(skew_cfg, self.instrument_id)
        term_structure_cfg = _build_term_structure_invert_cfg(self.rules)
        term_structure_detector = TermStructureInvertDetector(term_structure_cfg, self.instrument_id)
        db_loop: Optional[asyncio.AbstractEventLoop] = None
        db_pool = None
        db_adapter: Optional[_AsyncpgAdapter] = None
        last_snapshot: Optional[Snapshot] = None
        last_oi_poll_ts = 0.0
        last_liq_poll_ts = 0.0
        last_tt_poll_ts = 0.0
        last_basis_poll_ts = 0.0
        last_skew_poll_ts = 0.0
        last_term_structure_poll_ts = 0.0

        def _close_db_resources() -> None:
            nonlocal db_pool, db_loop, db_adapter
            if db_loop is None:
                return
            try:
                if db_pool is not None:
                    try:
                        db_loop.run_until_complete(db_pool.close())
                    except Exception:
                        logger.warning("[cpu-worker] failed to close db pool cleanly", exc_info=True)
                pending = []
                try:
                    pending = [t for t in asyncio.all_tasks(loop=db_loop) if not t.done()]
                except Exception:
                    pending = []
                if pending:
                    for task in pending:
                        task.cancel()
                    try:
                        db_loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                    except Exception:
                        logger.debug("[cpu-worker] pending db tasks cleanup failed", exc_info=True)
                try:
                    db_loop.stop()
                except Exception:
                    logger.debug("[cpu-worker] failed to stop db loop", exc_info=True)
                try:
                    db_loop.close()
                except Exception:
                    logger.warning("[cpu-worker] failed to close db loop", exc_info=True)
            finally:
                db_pool = None
                db_loop = None
                db_adapter = None

        need_db = (
            oi_cfg.enabled
            or liq_cfg.enabled
            or top_traders_cfg.enabled
            or basis_dislocation_cfg.enabled
            or skew_cfg.enabled
            or term_structure_cfg.enabled
        )
        if need_db and self.db_dsn and asyncpg is not None:
            try:
                db_loop = asyncio.new_event_loop()
                db_pool = db_loop.run_until_complete(asyncpg.create_pool(dsn=self.db_dsn, min_size=1, max_size=2))
                db_adapter = _AsyncpgAdapter(db_pool, db_loop)
                logger.info("[cpu-worker] DB adapter enabled for macro detectors.")
            except Exception as e:
                logger.warning(f"[cpu-worker] Failed to init DB pool for macro detectors: {e!s}")
                _close_db_resources()
        elif need_db and not self.db_dsn:
            logger.warning("[cpu-worker] Macro detectors enabled but db_dsn not provided; skipping polls.")
        elif need_db and asyncpg is None:
            logger.warning("[cpu-worker] Macro detectors enabled but asyncpg not available; skipping polls.")

        try:
            while True:
                now_poll = time.time()
                if (
                    oi_detector
                    and oi_detector.cfg.enabled
                    and db_adapter is not None
                    and (now_poll - last_oi_poll_ts) >= float(oi_detector.cfg.poll_s)
                ):
                    poll_t0 = time.perf_counter()
                    try:
                        ev_macro = oi_detector.poll(now_poll, db_adapter, last_snapshot)
                    except Exception:
                        ev_macro = None
                        logger.exception("[cpu-worker] oi_spike poll failed")
                    if ev_macro is not None:
                        self._emit_result("macro", ev_macro, poll_t0, poll_t0)
                    last_oi_poll_ts = now_poll
                if (
                    liq_detector
                    and liq_detector.cfg.enabled
                    and db_adapter is not None
                    and (now_poll - last_liq_poll_ts) >= float(liq_detector.cfg.poll_s)
                ):
                    poll_t0 = time.perf_counter()
                    try:
                        ev_macro = liq_detector.poll(now_poll, db_adapter, self.instrument_id, last_snapshot)
                    except Exception:
                        ev_macro = None
                        logger.exception("[cpu-worker] liq_cluster poll failed")
                    if ev_macro is not None:
                        self._emit_result("macro", ev_macro, poll_t0, poll_t0)
                    last_liq_poll_ts = now_poll
                if (
                    top_traders_detector
                    and top_traders_detector.cfg.enabled
                    and db_adapter is not None
                    and (now_poll - last_tt_poll_ts) >= float(top_traders_detector.cfg.poll_s)
                ):
                    poll_t0 = time.perf_counter()
                    try:
                        ev_macro = top_traders_detector.poll(now_poll, db_adapter, self.instrument_id)
                    except Exception:
                        ev_macro = None
                        logger.exception("[cpu-worker] top_traders poll failed")
                    if ev_macro is not None:
                        self._emit_result("macro", ev_macro, poll_t0, poll_t0)
                    last_tt_poll_ts = now_poll
                if (
                    basis_dislocation_detector
                    and basis_dislocation_detector.cfg.enabled
                    and db_adapter is not None
                    and (now_poll - last_basis_poll_ts) >= float(basis_dislocation_detector.cfg.poll_s)
                ):
                    poll_t0 = time.perf_counter()
                    try:
                        ev_macro = basis_dislocation_detector.poll(now_poll, db_adapter, self.instrument_id)
                    except Exception:
                        ev_macro = None
                        logger.exception("[cpu-worker] basis_dislocation poll failed")
                    if ev_macro is not None:
                        self._emit_result("macro", ev_macro, poll_t0, poll_t0)
                    last_basis_poll_ts = now_poll
                if (
                    skew_detector
                    and skew_detector.cfg.enabled
                    and db_adapter is not None
                    and (now_poll - last_skew_poll_ts) >= float(skew_detector.cfg.poll_s)
                ):
                    poll_t0 = time.perf_counter()
                    try:
                        ev_macro = skew_detector.poll(now_poll, db_adapter, self.instrument_id)
                    except Exception:
                        ev_macro = None
                        logger.exception("[cpu-worker] skew_shock poll failed")
                    if ev_macro is not None:
                        self._emit_result("macro", ev_macro, poll_t0, poll_t0)
                    last_skew_poll_ts = now_poll
                if (
                    term_structure_detector
                    and term_structure_detector.cfg.enabled
                    and db_adapter is not None
                    and (now_poll - last_term_structure_poll_ts) >= float(term_structure_detector.cfg.poll_s)
                ):
                    poll_t0 = time.perf_counter()
                    try:
                        ev_macro = term_structure_detector.poll(now_poll, db_adapter, self.instrument_id)
                    except Exception:
                        ev_macro = None
                        logger.exception("[cpu-worker] term_structure_invert poll failed")
                    if ev_macro is not None:
                        self._emit_result("macro", ev_macro, poll_t0, poll_t0)
                    last_term_structure_poll_ts = now_poll

                try:
                    req: WorkerRequest = self.in_queue.get(timeout=0.1)
                except queue.Empty:
                    continue

                if req is None:
                    break
                t0 = time.perf_counter()
                try:
                    if req.kind == "depth":
                        res = self._process_depth(det_pass, det_spoof, engine, req.payload)
                        self._emit_result("depth", res, t0, req.enqueued_at)
                    elif req.kind == "trade":
                        res = self._process_trade(
                            det_slice_eq,
                            det_slice_hit,
                            det_abs,
                            det_spoof,
                            det_bw,
                            tape_det,
                            engine,
                            req.payload,
                        )
                        self._emit_result("trade", res, t0, req.enqueued_at)
                    elif req.kind == "mark":
                        self._process_mark(engine, req.payload)
                        self._emit_result("mark", None, t0, req.enqueued_at)
                    elif req.kind == "snapshot":
                        res = self._process_snapshot(
                            det_dom,
                            dep_bid_det,
                            dep_ask_det,
                            basis_pos_trig,
                            basis_neg_trig,
                            basis_mr,
                            engine,
                            req.payload,
                        )
                        last_snapshot = res.snapshot
                        try:
                            oi_detector.update_wmid(res.ts, getattr(res.snapshot, "wmid", None))
                        except Exception:
                            pass
                        try:
                            liq_detector.update_wmid(res.ts, getattr(res.snapshot, "wmid", None))
                        except Exception:
                            pass
                        self._emit_result("snapshot", res, t0, req.enqueued_at)
                    elif req.kind == "rules":
                        _apply_rules_to_detectors(
                            req.payload or {},
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
                        )
                        oi_cfg = _build_oi_cfg(req.payload or {})
                        oi_detector.cfg = oi_cfg
                        oi_detector.wmid_window.max_age_s = max(
                            oi_detector.cfg.momentum_window_s * 3.0, oi_detector.cfg.oi_window_s * 1.5
                        )
                        try:
                            oi_detector.wmid_window._trim(time.time())
                        except Exception:
                            pass
                        liq_cfg = _build_liq_cfg(req.payload or {})
                        liq_detector.cfg = liq_cfg
                        liq_detector.wmid_window.max_age_s = max(
                            liq_detector.cfg.momentum_window_s * 3.0, liq_detector.cfg.window_s * 1.5
                        )
                        try:
                            liq_detector.wmid_window._trim(time.time())
                        except Exception:
                            pass
                        top_traders_cfg = _build_top_traders_cfg(req.payload or {})
                        top_traders_detector.cfg = top_traders_cfg
                        basis_dislocation_cfg = _build_basis_dislocation_cfg(req.payload or {})
                        basis_dislocation_detector.cfg = basis_dislocation_cfg
                        skew_cfg = _build_skew_shock_cfg(req.payload or {})
                        skew_detector.cfg = skew_cfg
                        term_structure_cfg = _build_term_structure_invert_cfg(req.payload or {})
                        term_structure_detector.cfg = term_structure_cfg
                        need_db_after = (
                            oi_cfg.enabled
                            or liq_cfg.enabled
                            or top_traders_cfg.enabled
                            or basis_dislocation_cfg.enabled
                            or skew_cfg.enabled
                            or term_structure_cfg.enabled
                        )
                        if need_db_after and db_adapter is None and self.db_dsn and asyncpg is not None:
                            try:
                                db_loop = asyncio.new_event_loop()
                                db_pool = db_loop.run_until_complete(
                                    asyncpg.create_pool(dsn=self.db_dsn, min_size=1, max_size=2)
                                )
                                db_adapter = _AsyncpgAdapter(db_pool, db_loop)
                            except Exception:
                                logger.warning("[cpu-worker] failed to init db adapter after rules update")
                                _close_db_resources()
                        self._emit_result("rules", None, t0, req.enqueued_at)
                    else:
                        logger.warning(f"[cpu-worker] Unknown request type: {req.kind}")
                except Exception:
                    logger.exception("[cpu-worker] failed processing request %s", req.kind)
                    try:
                        self._emit_result(req.kind, None, t0, req.enqueued_at)
                    except Exception:
                        pass
        finally:
            _close_db_resources()

    def _emit_result(self, kind: str, payload: Any, t0: float, enqueued_at: float) -> None:
        processing_seconds = max(0.0, time.perf_counter() - t0)
        self.out_queue.put(
            WorkerResult(
                kind=kind,
                payload=payload,
                processing_seconds=processing_seconds,
                enqueued_at=enqueued_at,
            )
        )

    def _process_depth(
        self,
        det_pass: SlicingPassiveDetector,
        det_spoof: SpoofingDetector,
        engine: MetricsEngine,
        payload: Dict[str, Any],
    ) -> DepthProcessResult:
        engine.on_depth(payload["ts"], payload["side"], payload["action"], payload["price"], payload["qty"])
        passive_event: Optional[Event] = None
        if payload["action"] == "insert" and payload["qty"] > 0:
            passive_event = det_pass.on_depth(
                payload["ts"], payload["side"], payload["price"], payload["qty"]
            )

        bb, ba = engine.book.best()
        book_qty = engine.book.bids.get(payload["price"]) if payload["side"] == "buy" else engine.book.asks.get(payload["price"])
        spoof_qty = book_qty if book_qty is not None else payload["qty"]
        spoof_event = det_spoof.on_depth(
            payload["ts"], payload["side"], payload["action"], payload["price"], spoof_qty, bb, ba
        )
        return DepthProcessResult(passive_event=passive_event, spoof_event=spoof_event)

    def _process_trade(
        self,
        det_slice_eq: SlicingAggDetector,
        det_slice_hit: SlicingAggDetector,
        det_abs: AbsorptionDetector,
        det_spoof: SpoofingDetector,
        det_bw: BreakWallDetector,
        tape_det: TapePressureDetector,
        engine: MetricsEngine,
        payload: Dict[str, Any],
    ) -> TradeProcessResult:
        ts = payload["ts"]
        side = payload["side"]
        px = payload["price"]
        qty = payload["qty"]

        engine.on_trade(ts, side, px, qty)
        det_spoof.on_trade(ts, side, px, qty)

        ev1 = det_slice_eq.on_trade(ts, side, px, qty)
        ev2 = det_slice_hit.on_trade(ts, side, px, qty)

        bb, ba = engine.book.best()
        det_abs.on_best(bb, ba)
        ev_abs = det_abs.on_trade(ts, side, px, qty)

        snap = engine.get_snapshot(ts)
        ev_tp = tape_det.on_trade(ts, side, px, qty)

        bw_event: Optional[Event] = None
        bw_gating: Optional[str] = None
        if ev1 or ev2:
            bw_event, bw_gating = det_bw.on_slicing(ts, ev1 or ev2, snap)

        return TradeProcessResult(
            slice_equal=ev1,
            slice_hit=ev2,
            absorption=ev_abs,
            tape_pressure=ev_tp,
            snapshot=snap,
            break_wall_event=bw_event,
            break_wall_gating=bw_gating,
        )

    def _process_mark(self, engine: MetricsEngine, payload: Dict[str, Any]) -> None:
        engine.on_mark(payload["ts"], payload.get("mark_price"), payload.get("index_price"))

    def _process_snapshot(
        self,
        det_dom: DominanceDetector,
        dep_bid_det: DepletionDetector,
        dep_ask_det: DepletionDetector,
        basis_pos_trig: MetricTriggerDetector,
        basis_neg_trig: MetricTriggerDetector,
        basis_mr: BasisMeanRevertDetector,
        engine: MetricsEngine,
        payload: Dict[str, Any],
    ) -> SnapshotProcessResult:
        now_ts = payload["ts"]
        snap = engine.get_snapshot(now_ts)

        spread_usd = float(snap.spread_usd or 0.0) if snap.spread_usd is not None else None

        dom_event: Optional[Event] = None
        metric_source = (det_dom.cfg.metric_source or "legacy").lower()
        if metric_source in ("doc", "auto"):
            doc_bid = getattr(snap, "dominance_bid_doc", None)
            doc_ask = getattr(snap, "dominance_ask_doc", None)
            if spread_usd is not None and spread_usd <= det_dom.cfg.max_spread_usd:
                side_doc: Optional[str] = None
                dom_val: Optional[float] = None
                metric_used: Optional[str] = None
                if doc_bid is not None and doc_bid >= det_dom.cfg.dom_pct_doc:
                    side_doc = "buy"
                    dom_val = doc_bid
                    metric_used = "dominance_bid_doc"
                elif doc_ask is not None and doc_ask >= det_dom.cfg.dom_pct_doc:
                    side_doc = "sell"
                    dom_val = doc_ask
                    metric_used = "dominance_ask_doc"

                if side_doc and dom_val is not None:
                    price = snap.best_bid if side_doc == "buy" else snap.best_ask
                    dom_event = Event(
                        "dominance",
                        side_doc,
                        now_ts,
                        price or 0.0,
                        dom_val * 100.0,
                        {
                            "metric_used": metric_used,
                            "metric_source": "doc",
                            "dom_pct_doc": det_dom.cfg.dom_pct_doc,
                        },
                    )

        if dom_event is None and metric_source != "doc":
            dom_event = det_dom.maybe_emit(now_ts, spread_usd=spread_usd)
            if dom_event:
                dom_event.fields["metric_used"] = "dom_levels_legacy"
                dom_event.fields["metric_source"] = "legacy"
        ev_dep_bid = dep_bid_det.on_snapshot(now_ts, snap.__dict__ if hasattr(snap, "__dict__") else dict())
        ev_dep_ask = dep_ask_det.on_snapshot(now_ts, snap.__dict__ if hasattr(snap, "__dict__") else dict())

        ev_bpos = basis_pos_trig.on_snapshot(now_ts, snap.__dict__ if hasattr(snap, "__dict__") else dict())
        ev_bneg = basis_neg_trig.on_snapshot(now_ts, snap.__dict__ if hasattr(snap, "__dict__") else dict())
        ev_mr = basis_mr.on_snapshot(now_ts, snap.__dict__ if hasattr(snap, "__dict__") else dict())

        rows: list[Tuple[str, dt.datetime, int, str, float, Optional[str], str]] = []

        def add(name: str, val, window_s: int | float = 1):
            if val is None:
                return
            if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):  # type: ignore[name-defined]
                return
            rows.append(
                (
                    self.instrument_id,
                    dt.datetime.fromtimestamp(now_ts, tz=dt.timezone.utc),
                    int(window_s),
                    name,
                    float(val),
                    self.profile,
                    "{}",
                )
            )

        add("spread_usd", getattr(snap, "spread_usd", None))
        add("basis_bps", getattr(snap, "basis_bps", None))
        add("basis_vel_bps_s", getattr(snap, "basis_vel_bps_s", None))
        add("basis_bps_doc", getattr(snap, "basis_bps_doc", None), window_s=engine.basis_doc_window_s)
        add("basis_vel_bps_s_doc", getattr(snap, "basis_vel_bps_s_doc", None), window_s=engine.basis_doc_window_s)
        add("basis_accel_bps_s2_doc", getattr(snap, "basis_accel_bps_s2_doc", None), window_s=engine.basis_doc_window_s)
        add("dom_bid", getattr(snap, "dom_bid", None))
        add("dom_ask", getattr(snap, "dom_ask", None))
        add("dominance_bid_doc", getattr(snap, "dominance_bid_doc", None), window_s=engine.dominance_doc_window_s)
        add("dominance_ask_doc", getattr(snap, "dominance_ask_doc", None), window_s=engine.dominance_doc_window_s)
        add("imbalance", getattr(snap, "imbalance", None))
        add("imbalance_doc", getattr(snap, "imbalance_doc", None), window_s=engine.imbalance_doc_window_s)
        add("dep_bid", getattr(snap, "dep_bid", None))
        add("dep_ask", getattr(snap, "dep_ask", None))
        add("depletion_bid_doc", getattr(snap, "depletion_bid_doc", None), window_s=engine.depletion_doc_window_s)
        add("depletion_ask_doc", getattr(snap, "depletion_ask_doc", None), window_s=engine.depletion_doc_window_s)
        add("refill_bid_3s", getattr(snap, "refill_bid_3s", None))
        add("refill_ask_3s", getattr(snap, "refill_ask_3s", None))
        add("wmid", getattr(snap, "wmid", None), window_s=1)

        return SnapshotProcessResult(
            ts=now_ts,
            snapshot=snap,
            dominance_event=dom_event,
            dep_bid_event=ev_dep_bid,
            dep_ask_event=ev_dep_ask,
            basis_pos_event=ev_bpos,
            basis_neg_event=ev_bneg,
            basis_mr_event=ev_mr,
            metric_rows=rows,
        )


class CPUWorkerClient:
    def __init__(
        self,
        *,
        rules: Optional[Dict[str, Any]],
        instrument_id: str,
        profile: str,
        max_queue: int = 50_000,
        service_name: str,
        exported_service: str,
        db_dsn: Optional[str] = None,
    ):
        ctx = mp.get_context("spawn")
        self.in_queue: mp.Queue = ctx.Queue(maxsize=max_queue)
        self.out_queue: mp.Queue = ctx.Queue(maxsize=max_queue)
        self.process = CPUWorkerProcess(
            self.in_queue,
            self.out_queue,
            rules=rules,
            instrument_id=instrument_id,
            profile=profile,
            db_dsn=db_dsn,
        )
        self.rules = rules or {}
        self.loop = None
        self._result_queue: asyncio.Queue[WorkerResult]  # type: ignore
        self._forwarder: Optional[threading.Thread] = None  # type: ignore
        self._stop_forwarder = threading.Event()  # type: ignore
        self.metric_labels = {"service": service_name, "exported_service": exported_service}

    async def start(self) -> None:
        import asyncio  # local to keep fork-safety
        import threading

        self.loop = asyncio.get_running_loop()
        self._result_queue = asyncio.Queue()
        self.process.start()

        def _forward_results():
            while not self._stop_forwarder.is_set():
                try:
                    res = self.out_queue.get(timeout=0.1)
                except Exception:
                    continue
                self.loop.call_soon_threadsafe(self._result_queue.put_nowait, res)
                self._observe_queue_depths()

        self._forwarder = threading.Thread(target=_forward_results, name="cpu-worker-forwarder", daemon=True)
        self._forwarder.start()

    async def stop(self) -> None:
        self._stop_forwarder.set()
        try:
            self.in_queue.put_nowait(None)
        except Exception:
            pass
        if self.process.is_alive():
            self.process.join(timeout=2)
        if self._forwarder and self._forwarder.is_alive():
            self._forwarder.join(timeout=1)

    def _observe_queue_depths(self) -> None:
        labels = {"queue": "cpu_worker", **self.metric_labels}
        try:
            obs_metrics.worker_queue_in_depth.labels(**labels).set(float(self.in_queue.qsize()))
        except Exception:
            obs_metrics.worker_queue_in_depth.labels(**labels).set(0)
        try:
            obs_metrics.worker_queue_out_depth.labels(**labels).set(float(self.out_queue.qsize()))
        except Exception:
            obs_metrics.worker_queue_out_depth.labels(**labels).set(0)

    async def _enqueue(self, kind: str, payload: Any) -> None:
        req = WorkerRequest(kind=kind, payload=payload, enqueued_at=time.perf_counter())
        try:
            self.in_queue.put_nowait(req)
            obs_metrics.worker_events_total.labels(kind=kind, **self.metric_labels).inc()
        except Exception:
            obs_metrics.worker_drops_total.labels(reason="in_queue_full", **self.metric_labels).inc()
        self._observe_queue_depths()

    async def enqueue_trade(self, ts: float, side: str, price: float, qty: float) -> None:
        await self._enqueue("trade", {"ts": ts, "side": side, "price": price, "qty": qty})

    async def enqueue_depth(self, ts: float, side: str, action: str, price: float, qty: float) -> None:
        await self._enqueue("depth", {"ts": ts, "side": side, "action": action, "price": price, "qty": qty})

    async def enqueue_mark(self, ts: float, mark_price: Optional[float], index_price: Optional[float]) -> None:
        await self._enqueue("mark", {"ts": ts, "mark_price": mark_price, "index_price": index_price})

    async def enqueue_snapshot(self, ts: float) -> None:
        await self._enqueue("snapshot", {"ts": ts})

    async def update_rules(self, rules: Dict[str, Any]) -> None:
        self.rules = rules or {}
        await self._enqueue("rules", self.rules)

    async def get_result(self) -> WorkerResult:
        res = await self._result_queue.get()
        labels = {**self.metric_labels, "kind": res.kind}
        obs_metrics.worker_processing_seconds.labels(**labels).observe(res.processing_seconds)
        try:
            obs_metrics.worker_queue_out_depth.labels(queue="cpu_worker", **self.metric_labels).set(
                float(self.out_queue.qsize())
            )
        except Exception:
            obs_metrics.worker_queue_out_depth.labels(queue="cpu_worker", **self.metric_labels).set(0)
        return res
