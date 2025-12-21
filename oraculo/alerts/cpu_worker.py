from __future__ import annotations

import asyncio
import math
import multiprocessing as mp
import time
from dataclasses import dataclass
import datetime as dt
from typing import Any, Dict, Optional, Sequence, Tuple
import threading

from loguru import logger

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

    def _apply_dep_cfg(detector: DepletionDetector, side: str, dep_cfg: Dict[str, Any]) -> None:
        pct_candidates = [
            dep_cfg.get("pct_drop"),
            dep_cfg.get("pct"),
            rules.get("depletion", {}).get(f"pct_drop_{side}"),
            rules.get("depletion", {}).get(f"pct_{side}"),
        ]
        pct_val = next((v for v in pct_candidates if v is not None), None)
        if pct_val is not None:
            detector.cfg.pct_drop = float(pct_val)

        hold_val = dep_cfg.get("hold_ms", rules.get("depletion", {}).get("hold_ms"))
        if hold_val is not None:
            detector.cfg.hold_ms = int(hold_val)

        retrigger_val = dep_cfg.get("retrigger_s", rules.get("depletion", {}).get("retrigger_s"))
        if retrigger_val is not None:
            detector.cfg.retrigger_s = float(retrigger_val)

        enabled_val = dep_cfg.get("enabled", rules.get("depletion", {}).get("enabled"))
        if enabled_val is not None:
            detector.cfg.enabled = bool(enabled_val)

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
    _apply_dep_cfg(dep_bid_det, "bid", dep_bid)
    _apply_dep_cfg(dep_ask_det, "ask", dep_ask)

    basis_cfg = det.get("basis") or {}
    bx = basis_cfg.get("extreme") or {}
    basis_pos_trig.cfg.threshold = float(bx.get("pos_bps", basis_pos_trig.cfg.threshold))
    basis_neg_trig.cfg.threshold = float(bx.get("neg_bps", basis_neg_trig.cfg.threshold))

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
    ):
        super().__init__(daemon=True)
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.rules = rules or {}
        self.instrument_id = instrument_id
        self.profile = profile

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

        while True:
            req: WorkerRequest = self.in_queue.get()
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
                    self._emit_result("rules", None, t0, req.enqueued_at)
                else:
                    logger.warning(f"[cpu-worker] Unknown request type: {req.kind}")
            except Exception:
                logger.exception("[cpu-worker] failed processing request %s", req.kind)
                try:
                    self._emit_result(req.kind, None, t0, req.enqueued_at)
                except Exception:
                    pass

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

        evd = det_dom.maybe_emit(now_ts, spread_usd=float(snap.spread_usd or 0.0) if snap.spread_usd is not None else None)
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
            dominance_event=evd,
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
