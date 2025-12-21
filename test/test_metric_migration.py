import multiprocessing as mp
import sys
import types

import pytest

# Entorno de CI no siempre tiene loguru instalado; proveer stub mínimo para importar CPUWorkerProcess.
class _DummyLogger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def exception(self, *args, **kwargs):
        pass


sys.modules.setdefault("loguru", types.SimpleNamespace(logger=_DummyLogger()))
class _DummyMetric:
    def __init__(self, *args, **kwargs):
        pass

    def labels(self, *args, **kwargs):
        return self

    def observe(self, *args, **kwargs):
        return self

    def inc(self, *args, **kwargs):
        return self

    def set(self, *args, **kwargs):
        return self


sys.modules.setdefault(
    "prometheus_client",
    types.SimpleNamespace(
        Counter=_DummyMetric,
        Gauge=_DummyMetric,
        Summary=_DummyMetric,
        Histogram=_DummyMetric,
        start_http_server=lambda *args, **kwargs: None,
    ),
)

from oraculo.alerts.cpu_worker import CPUWorkerProcess
from oraculo.detect.detectors import (
    BasisMeanRevertDetector,
    BasisMRcfg,
    DepletionCfg,
    DepletionDetector,
    DominanceCfg,
    DominanceDetector,
    MetricTrigCfg,
    MetricTriggerDetector,
)
from oraculo.detect.metrics_engine import Snapshot


class _DummyBook:
    def __init__(self, bids, asks):
        self._bids = bids
        self._asks = asks

    def get_head(self, levels):
        return self._bids[:levels], self._asks[:levels]

    def best(self):
        return (self._bids[0][0] if self._bids else None, self._asks[0][0] if self._asks else None)


class _DummyEngine:
    def __init__(self, snap, book=None):
        self._snap = snap
        self.book = book or _DummyBook([], [])
        self.basis_doc_window_s = 120.0
        self.dominance_doc_window_s = 2.0
        self.imbalance_doc_window_s = 3.0
        self.depletion_doc_window_s = 3.0

    def get_snapshot(self, ts):
        return self._snap


def _worker():
    ctx = mp.get_context("spawn")
    return CPUWorkerProcess(ctx.Queue(), ctx.Queue(), rules=None, instrument_id="X", profile="test")


def test_metric_trigger_source_selection():
    cfg = MetricTrigCfg(
        metric="basis_bps",
        threshold=-50,
        direction="below",
        hold_ms=0,
        retrigger_s=0,
        metric_source="auto",
        doc_sign_mode="legacy",
    )
    det = MetricTriggerDetector(cfg)
    snap_doc = {"basis_bps_doc": 120.0, "basis_bps": -60.0}
    assert det.on_snapshot(1.0, snap_doc) is None
    ev_doc = det.on_snapshot(1.1, snap_doc)
    assert ev_doc is not None
    assert ev_doc.intensity == pytest.approx(-120.0)
    assert ev_doc.fields["metric_used"] == "basis_bps_doc"
    assert ev_doc.fields["metric_source"] == "doc"
    assert ev_doc.fields["doc_sign_mode"] == "legacy"

    det_fb = MetricTriggerDetector(cfg)
    snap_legacy = {"basis_bps": -80.0}
    det_fb.on_snapshot(2.0, snap_legacy)
    ev_fb = det_fb.on_snapshot(2.1, snap_legacy)
    assert ev_fb is not None
    assert ev_fb.intensity == pytest.approx(-80.0)
    assert ev_fb.fields["metric_used"] == "basis_bps"
    assert ev_fb.fields["metric_source"] == "legacy"


def test_basis_mr_source_selection():
    cfg_doc = BasisMRcfg(gate_abs_bps=5, vel_gate_abs=0.1, retrigger_s=0, metric_source="auto", doc_sign_mode="legacy")
    det_doc = BasisMeanRevertDetector(cfg_doc)
    snap1 = {"basis_bps_doc": 0.5, "basis_vel_bps_s_doc": -0.2}
    det_doc.on_snapshot(1.0, snap1)
    snap2 = {"basis_bps_doc": 0.5, "basis_vel_bps_s_doc": 0.2}
    ev_doc = det_doc.on_snapshot(1.2, snap2)
    assert ev_doc is not None
    assert ev_doc.fields["metric_source"] == "doc"
    assert ev_doc.fields["metric_used_basis"] == "basis_bps_doc"
    assert ev_doc.fields["metric_used_vel"] == "basis_vel_bps_s_doc"
    assert ev_doc.fields["doc_sign_mode"] == "legacy"

    cfg_leg = BasisMRcfg(gate_abs_bps=5, vel_gate_abs=0.1, retrigger_s=0, metric_source="auto")
    det_leg = BasisMeanRevertDetector(cfg_leg)
    snap3 = {"basis_bps": 1.0, "basis_vel_bps_s": 0.2}
    det_leg.on_snapshot(2.0, snap3)
    snap4 = {"basis_bps": -1.0, "basis_vel_bps_s": -0.2}
    ev_leg = det_leg.on_snapshot(2.2, snap4)
    assert ev_leg is not None
    assert ev_leg.fields["metric_source"] == "legacy"
    assert ev_leg.fields["metric_used_basis"] == "basis_bps"
    assert ev_leg.fields["metric_used_vel"] == "basis_vel_bps_s"


def test_dominance_doc_path():
    det_dom_doc = DominanceDetector(DominanceCfg(metric_source="auto", dom_pct_doc=0.60))
    snap_doc = Snapshot(
        spread_usd=1.0,
        dominance_bid_doc=0.65,
        dominance_ask_doc=0.35,
        best_bid=100.0,
        best_ask=101.0,
    )
    engine = _DummyEngine(snap_doc, _DummyBook([(100.0, 1.0)], [(101.0, 1.0)]))
    dep_bid = DepletionDetector(DepletionCfg(side="buy"))
    dep_ask = DepletionDetector(DepletionCfg(side="sell"))
    basis_pos_trig = MetricTriggerDetector(MetricTrigCfg(metric="basis_bps", threshold=0.0, hold_ms=0, retrigger_s=0))
    basis_neg_trig = MetricTriggerDetector(MetricTrigCfg(metric="basis_bps", threshold=0.0, hold_ms=0, retrigger_s=0))
    basis_mr = BasisMeanRevertDetector(BasisMRcfg(retrigger_s=0))

    res_doc = _worker()._process_snapshot(
        det_dom_doc, dep_bid, dep_ask, basis_pos_trig, basis_neg_trig, basis_mr, engine, {"ts": 10.0}
    )
    assert res_doc.dominance_event is not None
    assert res_doc.dominance_event.fields["metric_used"] == "dominance_bid_doc"
    assert res_doc.dominance_event.fields["metric_source"] == "doc"
    assert res_doc.dominance_event.side == "buy"

    det_dom_legacy = DominanceDetector(DominanceCfg(metric_source="auto", dom_pct=0.8))
    book = _DummyBook([(100.0, 1.0), (99.5, 1.0), (99.0, 1.0), (98.5, 1.0)], [(101.0, 1.0)])
    det_dom_legacy.attach_book(book)
    snap_legacy = Snapshot(spread_usd=1.0, best_bid=100.0, best_ask=101.0)
    engine2 = _DummyEngine(snap_legacy, book)
    res_leg = _worker()._process_snapshot(
        det_dom_legacy, dep_bid, dep_ask, basis_pos_trig, basis_neg_trig, basis_mr, engine2, {"ts": 20.0}
    )
    assert res_leg.dominance_event is not None
    assert res_leg.dominance_event.fields["metric_used"] == "dom_levels_legacy"
    assert res_leg.dominance_event.fields["metric_source"] == "legacy"
