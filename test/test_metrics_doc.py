import math
import pytest

from oraculo.detect.metrics_engine import MetricsEngine


def test_wmid_and_dominance_doc_mean():
    engine = MetricsEngine(
        top_n=5,
        imbalance_doc_window_s=2.0,
        dominance_doc_window_s=2.0,
        depletion_doc_window_s=2.0,
    )
    ts0 = 1_000.0
    engine.on_depth(ts0, "buy", "insert", 100.0, 5.0)
    engine.on_depth(ts0, "sell", "insert", 101.0, 5.0)

    snap = engine.get_snapshot(ts0)
    assert snap.wmid == pytest.approx(100.5)
    assert snap.dominance_bid_doc == pytest.approx(0.5)
    assert snap.dominance_ask_doc == pytest.approx(0.5)
    assert snap.imbalance_doc == pytest.approx(0.0)


def test_depletion_doc_delta_topn():
    engine = MetricsEngine(top_n=5, depletion_doc_window_s=2.0)
    ts0 = 10.0
    engine.on_depth(ts0, "buy", "insert", 100.0, 5.0)
    engine.on_depth(ts0, "sell", "insert", 101.0, 7.0)
    engine.get_snapshot(ts0)

    ts1 = ts0 + 2.0
    engine.on_depth(ts1, "buy", "delete", 100.0, 2.0)
    engine.on_depth(ts1, "sell", "insert", 101.0, 1.0)
    snap = engine.get_snapshot(ts1)

    assert snap.depletion_bid_doc == pytest.approx(3.0 - 5.0)
    assert snap.depletion_ask_doc == pytest.approx(8.0 - 7.0)


def test_basis_doc_derivatives():
    engine = MetricsEngine(basis_doc_window_s=60.0)
    engine.on_mark(0.0, 100.0, 101.0)
    engine.on_mark(60.0, 99.0, 101.0)
    engine.on_mark(120.0, 98.0, 101.0)

    snap = engine.get_snapshot(120.0)
    assert snap.basis_bps_doc == pytest.approx((101.0 - 98.0) / 98.0 * 10000.0)
    assert math.isfinite(snap.basis_vel_bps_s_doc)
    assert math.isfinite(snap.basis_accel_bps_s2_doc)
