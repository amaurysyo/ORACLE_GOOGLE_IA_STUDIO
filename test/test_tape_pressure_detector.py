import pytest

from oraculo.alerts.cpu_worker import _apply_rules_to_detectors
from oraculo.detect.detectors import (
    AbsorptionCfg,
    AbsorptionDetector,
    BasisMRcfg,
    BasisMeanRevertDetector,
    BreakWallCfg,
    BreakWallDetector,
    DepletionCfg,
    DepletionDetector,
    DominanceCfg,
    DominanceDetector,
    MetricTrigCfg,
    MetricTriggerDetector,
    SlicingAggConfig,
    SlicingAggDetector,
    SlicingPassConfig,
    SlicingPassiveDetector,
    SpoofingCfg,
    SpoofingDetector,
    TapePressureCfg,
    TapePressureDetector,
)
from oraculo.detect.metrics_engine import MetricsEngine


def test_tape_pressure_hold_mintrades_minqty_and_spread_gate():
    cfg = TapePressureCfg(
        window_s=5,
        buy_thr=0.9,
        sell_thr=0.1,
        min_trades=10,
        min_qty_btc=30,
        max_spread_usd=2.0,
        hold_ms=500,
        retrigger_s=20,
    )
    det = TapePressureDetector(cfg)
    ts = 1000.0

    for _ in range(9):
        ev = det.on_trade(ts, "buy", 100.0, 3.0)
        assert ev is None
        ts += 0.1

    ev = det.on_trade(ts, "buy", 100.0, 3.0)
    assert ev is None
    ts += 0.3

    ev = det.on_trade(ts, "buy", 100.0, 3.0)
    assert ev is None
    ts += 0.3

    ev = det.on_trade(ts, "buy", 100.0, 3.0)
    assert ev is not None
    assert ev.side == "buy"
    assert ev.fields["hold_ms"] == 500

    ts += 0.5
    ev = det.on_trade(ts, "buy", 100.0, 3.0)
    assert ev is None

    det_spread = TapePressureDetector(cfg)
    ts = 2000.0
    for _ in range(12):
        ev = det_spread.on_trade(ts, "buy", 100.0, 3.0, spread_usd=3.0)
        assert ev is None
        ts += 0.2


def test_apply_rules_maps_ofi_to_ratio_thresholds():
    engine = MetricsEngine(
        top_n=1000,
        imbalance_doc_window_s=3.0,
        dominance_doc_window_s=2.0,
        depletion_doc_window_s=3.0,
        basis_doc_window_s=120.0,
    )
    det_slice_eq = SlicingAggDetector(SlicingAggConfig(require_equal=True, equal_tol_pct=0.0, equal_tol_abs=0.0))
    det_slice_hit = SlicingAggDetector(SlicingAggConfig(require_equal=False))
    det_abs = AbsorptionDetector(AbsorptionCfg())
    det_bw = BreakWallDetector(BreakWallCfg())
    det_pass = SlicingPassiveDetector(SlicingPassConfig())
    det_dom = DominanceDetector(DominanceCfg(), book=engine.book)
    det_spoof = SpoofingDetector(SpoofingCfg())
    dep_bid_det = DepletionDetector(DepletionCfg(side="buy"))
    dep_ask_det = DepletionDetector(DepletionCfg(side="sell"))
    basis_pos_trig = MetricTriggerDetector(MetricTrigCfg(metric="basis_bps", threshold=100.0, direction="above"))
    basis_neg_trig = MetricTriggerDetector(MetricTrigCfg(metric="basis_bps", threshold=-100.0, direction="below"))
    basis_mr = BasisMeanRevertDetector(BasisMRcfg())
    tape_det = TapePressureDetector(TapePressureCfg())

    rules = {
        "detectors": {
            "tape_pressure": {
                "ofi_up": 0.65,
                "ofi_down": -0.65,
                "min_trades": 10,
                "min_qty_btc": 30,
                "max_spread_usd": 2.0,
                "hold_ms": 500,
                "retrigger_s": 20,
                "window_s": 5.0,
            }
        }
    }

    _apply_rules_to_detectors(
        rules,
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

    assert tape_det.cfg.buy_thr == pytest.approx(0.825)
    assert tape_det.cfg.sell_thr == pytest.approx(0.175)
    assert tape_det.cfg.min_trades == 10
    assert tape_det.cfg.min_qty_btc == pytest.approx(30.0)
    assert tape_det.cfg.max_spread_usd == pytest.approx(2.0)
    assert tape_det.cfg.hold_ms == 500
    assert tape_det.cfg.retrigger_s == 20
    assert tape_det.cfg.window_s == pytest.approx(5.0)
