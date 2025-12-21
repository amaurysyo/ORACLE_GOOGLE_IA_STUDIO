import datetime as dt
import math

from oraculo.detect.macro_detectors import LiqClusterCfg, LiqClusterDetector
from oraculo.detect.metrics_engine import Snapshot
from oraculo.rules.engine import RuleContext, eval_rules


class DBStub:
    def __init__(self, sell_v: float = 0.0, buy_v: float = 0.0, metric: str = "quote_qty_usd"):
        self.sell_v = sell_v
        self.buy_v = buy_v
        self.metric = metric

    def fetch(self, query, *args):  # noqa: ANN001
        self.last_query = query  # noqa: B018
        return [
            {"side": "sell", "v": self.sell_v},
            {"side": "buy", "v": self.buy_v},
        ]


def _mk_detector(**overrides):
    cfg = LiqClusterCfg(enabled=True)
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return LiqClusterDetector(cfg, "TEST")


def _run_rules(ev):
    return eval_rules(
        {"type": ev.kind, "side": ev.side, "price": ev.price, "intensity": ev.intensity, "fields": ev.fields},
        RuleContext("TEST"),
    )


def _seed_wmid(det: LiqClusterDetector, ts_now: float, wmid_then: float, wmid_now: float):
    det.update_wmid(ts_now - det.cfg.momentum_window_s, wmid_then)
    det.update_wmid(ts_now, wmid_now)


def test_sell_cluster_emits_and_maps_to_r32():
    det = _mk_detector(confirm_s=1.0, warn_usd=100.0, strong_usd=200.0, min_move_usd=10.0)
    ts = 1_000.0
    _seed_wmid(det, ts, 200.0, 170.0)  # momentum -30
    db = DBStub(sell_v=150.0, buy_v=10.0)
    snap = Snapshot(wmid=170.0)

    assert det.poll(ts, db, "TEST", snap) is None  # arm
    ev = det.poll(ts + 2.0, db, "TEST", snap)
    assert ev is not None
    assert ev.side == "sell"
    rules = _run_rules(ev)
    assert any(r["rule"] == "R32" for r in rules)
    assert math.isclose(ev.intensity, 0.5, rel_tol=1e-6)


def test_buy_cluster_emits_and_maps_to_r33():
    det = _mk_detector(confirm_s=0.5, warn_usd=50.0, strong_usd=150.0, min_move_usd=5.0)
    ts = 2_000.0
    _seed_wmid(det, ts, 100.0, 120.0)  # momentum +20
    db = DBStub(sell_v=10.0, buy_v=80.0)
    snap = Snapshot(wmid=120.0)

    assert det.poll(ts, db, "TEST", snap) is None
    ev = det.poll(ts + 1.0, db, "TEST", snap)
    assert ev is not None
    assert ev.side == "buy"
    rules = _run_rules(ev)
    assert any(r["rule"] == "R33" for r in rules)


def test_below_warn_does_not_emit():
    det = _mk_detector(confirm_s=0.0, warn_usd=100.0)
    ts = 3_000.0
    _seed_wmid(det, ts, 100.0, 130.0)
    db = DBStub(sell_v=90.0, buy_v=80.0)
    snap = Snapshot(wmid=130.0)
    assert det.poll(ts, db, "TEST", snap) is None
    assert det.poll(ts + 1.0, db, "TEST", snap) is None


def test_rebound_resets_and_blocks():
    det = _mk_detector(confirm_s=0.5, warn_usd=50.0, strong_usd=150.0, min_move_usd=5.0, max_rebound_usd=5.0)
    ts = 4_000.0
    _seed_wmid(det, ts, 100.0, 90.0)
    db = DBStub(sell_v=120.0, buy_v=10.0)
    snap = Snapshot(wmid=90.0)
    assert det.poll(ts, db, "TEST", snap) is None

    snap_rebound = Snapshot(wmid=100.5)  # rebound +10.5 (contra movimiento)
    assert det.poll(ts + 1.0, db, "TEST", snap_rebound) is None
    assert det.armed_side is None  # reset


def test_retrigger_blocks_duplicates():
    det = _mk_detector(confirm_s=0.1, warn_usd=50.0, strong_usd=100.0, min_move_usd=5.0, retrigger_s=60.0)
    ts = 5_000.0
    _seed_wmid(det, ts, 100.0, 70.0)
    db = DBStub(sell_v=80.0, buy_v=10.0)
    snap = Snapshot(wmid=70.0)

    assert det.poll(ts, db, "TEST", snap) is None
    ev = det.poll(ts + 1.0, db, "TEST", snap)
    assert ev is not None
    assert det.poll(ts + 2.0, db, "TEST", snap) is None


def test_audit_fields_present():
    det = _mk_detector(confirm_s=0.1, warn_usd=50.0, strong_usd=100.0, min_move_usd=5.0)
    ts = 6_000.0
    _seed_wmid(det, ts, 100.0, 70.0)
    db = DBStub(sell_v=80.0, buy_v=10.0)
    snap = Snapshot(wmid=70.0)

    assert det.poll(ts, db, "TEST", snap) is None
    ev = det.poll(ts + 1.0, db, "TEST", snap)
    assert ev is not None
    fields = ev.fields
    for key in [
        "window_s",
        "poll_s",
        "confirm_s",
        "momentum_window_s",
        "min_move_usd",
        "max_rebound_usd",
        "metric_used",
        "sell_v",
        "buy_v",
        "cluster_v",
        "warn_usd",
        "strong_usd",
        "wmid",
        "momentum_usd",
        "armed_anchor_wmid",
    ]:
        assert key in fields
