import math
import pytest

from oraculo.detect.metrics_engine import MetricsEngine
from oraculo.detect.detectors import (
    DominanceCfg,
    DominanceDetector,
    SpoofingCfg,
    SpoofingDetector,
    SlicingPassConfig,
    SlicingPassiveDetector,
)


def test_metrics_engine_reconstructs_book_from_deltas():
    engine = MetricsEngine(top_n=10, tick_size=0.5)
    ts0 = 1_000.0

    engine.on_depth(ts0, "buy", "insert", 100.0, 5.0)
    engine.on_depth(ts0, "sell", "insert", 101.0, 3.0)

    snap = engine.get_snapshot()
    assert snap.best_bid == 100.0
    assert snap.best_ask == 101.0
    assert snap.imbalance == pytest.approx((5.0 - 3.0) / 8.0)
    assert snap.dep_bid == pytest.approx(0.0)
    assert snap.dep_ask == pytest.approx(0.0)

    engine.on_depth(ts0 + 0.5, "buy", "delete", 100.0, 2.0)
    snap2 = engine.get_snapshot()

    assert engine.book.bids[100.0] == pytest.approx(3.0)
    assert snap2.dep_bid == pytest.approx(2.0 / 7.0)
    assert snap2.refill_bid_3s == pytest.approx(1.0)


def test_dominance_detector_reads_reconstructed_book():
    engine = MetricsEngine(top_n=10)
    det = DominanceDetector(DominanceCfg(dom_pct=0.75, max_spread_usd=5.0), book=engine.book)

    ts = 10.0
    for i in range(4):
        engine.on_depth(ts, "buy", "insert", 100.0 - i * 0.5, 1.0)
    engine.on_depth(ts, "sell", "insert", 102.0, 1.0)

    snap = engine.get_snapshot()
    ev = det.maybe_emit(ts + 0.1, spread_usd=float(snap.spread_usd or 0.0))

    assert ev is not None
    assert ev.kind == "dominance"
    assert ev.side == "buy"
    assert ev.price == pytest.approx(100.0)


def test_spoofing_detector_handles_delta_wall_lifecycle():
    cfg = SpoofingCfg(
        wall_size_btc=5.0,
        distance_ticks_min=1,
        window_s=0.1,
        cancel_rate_min=0.6,
        exec_tolerance_btc=1.0,
        tick_size=1.0,
    )
    det = SpoofingDetector(cfg)

    best_bid = 100.0
    best_ask = 101.0
    ts0 = 0.0

    assert det.on_depth(ts0, "buy", "insert", 98.0, 12.0, best_bid, best_ask) is None
    assert det.on_depth(ts0 + 0.02, "buy", "delete", 98.0, 20.0, best_bid, best_ask) is None

    ev = det.on_depth(ts0 + 0.12, "buy", "update", 98.0, 0.0, best_bid, best_ask)
    assert ev is not None
    assert ev.kind == "spoofing"
    assert ev.side == "buy"


def test_slicing_passive_still_triggers_on_delta_inserts():
    det = SlicingPassiveDetector(SlicingPassConfig(gap_ms=200, k_min=3, qty_min=4.0))

    ts = 50.0
    assert det.on_depth(ts, "buy", 100.0, 1.5) is None
    assert det.on_depth(ts + 0.05, "buy", 100.0, 1.5) is None

    ev = det.on_depth(ts + 0.10, "buy", 100.0, 1.2)
    assert ev is not None
    assert ev.kind == "slicing_pass"
    assert ev.intensity == pytest.approx(4.2)
