import datetime as dt

from oraculo.detect.macro_detectors import GammaFlipCfg, GammaFlipDetector
from oraculo.rules.engine import RuleContext, eval_rules


class GammaDBStub:
    def __init__(self, spot_price: float, rows: list[dict]):
        self.spot_price = spot_price
        self.rows = rows

    def fetchrow(self, query, *args):  # noqa: ANN001
        _ = query
        ts = args[0] if args else 0
        return {"underlying_price": self.spot_price, "event_time": dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)}

    def fetch(self, query, *args):  # noqa: ANN001
        _ = query
        _ = args
        return self.rows


def _row(
    option_type: str,
    gamma: float,
    oi: float = 1.0,
    delta: float = 0.5,
    ts: float = 0.0,
    expiration_ts: float = 10_000_000.0,
):
    return {
        "instrument_id": f"{option_type}-id",
        "event_time": dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc),
        "gamma": gamma,
        "open_interest": oi,
        "delta": delta,
        "underlying_price": 20_000.0,
        "option_type": option_type,
        "strike": 20_000.0,
        "expiration": dt.datetime.fromtimestamp(expiration_ts, tz=dt.timezone.utc),
    }


def _rules_for_event(ev):
    return eval_rules(
        {"type": ev.kind, "side": ev.side, "price": ev.price, "intensity": ev.intensity, "fields": ev.fields},
        RuleContext("TEST"),
    )


def _cfg(**kwargs):
    cfg = GammaFlipCfg(enabled=True)
    for k, v in kwargs.items():
        setattr(cfg, k, v)
    return cfg


def test_gamma_flip_initializes_sign_without_emitting():
    cfg = _cfg(min_instruments=1, min_abs_net_gex=1.0)
    ts_now = 1_000_000.0
    db = GammaDBStub(spot_price=20_000.0, rows=[_row("call", gamma=1.0, oi=1.0, ts=ts_now)])
    det = GammaFlipDetector(cfg, "TEST")

    ev = det.poll(ts_now, db, "TEST")
    assert ev is None


def test_gamma_flip_requires_stable_samples_before_emit():
    cfg = _cfg(min_instruments=1, min_abs_net_gex=1.0, require_stable_samples=2)
    ts_now = 1_000_000.0
    db = GammaDBStub(spot_price=20_000.0, rows=[_row("call", gamma=1.0, oi=1.0, ts=ts_now)])
    det = GammaFlipDetector(cfg, "TEST")

    det.poll(ts_now, db, "TEST")  # initialize prev_sign

    db.rows = [_row("put", gamma=1.0, oi=1.0, ts=ts_now + 30)]
    ev_pending = det.poll(ts_now + 30.0, db, "TEST")
    assert ev_pending is None

    db.rows = [_row("put", gamma=1.0, oi=1.0, ts=ts_now + 60)]
    ev = det.poll(ts_now + 60.0, db, "TEST")
    assert ev is not None
    assert ev.kind == "gamma_flip"
    assert ev.fields.get("prev_sign") == 1
    assert ev.fields.get("new_sign") == -1


def test_gamma_flip_cooldown_blocks_retrigger():
    cfg = _cfg(min_instruments=1, min_abs_net_gex=1.0, require_stable_samples=1, retrigger_s=300)
    ts_now = 2_000_000.0
    db = GammaDBStub(spot_price=20_000.0, rows=[_row("call", gamma=1.0, oi=1.0, ts=ts_now)])
    det = GammaFlipDetector(cfg, "TEST")

    det.poll(ts_now, db, "TEST")
    db.rows = [_row("put", gamma=1.0, oi=1.0, ts=ts_now + 30)]
    ev = det.poll(ts_now + 30.0, db, "TEST")
    assert ev is not None

    db.rows = [_row("call", gamma=1.0, oi=1.0, ts=ts_now + 50)]
    ev_cooldown = det.poll(ts_now + 50.0, db, "TEST")
    assert ev_cooldown is None


def test_gamma_flip_gates_on_minimums():
    cfg = _cfg(min_instruments=2, min_abs_net_gex=1e12)
    ts_now = 3_000_000.0
    db = GammaDBStub(spot_price=20_000.0, rows=[_row("call", gamma=1.0, oi=1.0, ts=ts_now)])
    det = GammaFlipDetector(cfg, "TEST")

    ev = det.poll(ts_now, db, "TEST")
    assert ev is None

    db.rows = [
        _row("call", gamma=1.0, oi=1.0, ts=ts_now + 40),
        _row("call", gamma=1.0, oi=1.0, ts=ts_now + 40),
    ]
    ev_after_threshold = det.poll(ts_now + 40.0, db, "TEST")
    assert ev_after_threshold is None


def test_gamma_flip_maps_to_r36_with_intensity():
    cfg = _cfg(
        min_instruments=1,
        min_abs_net_gex=1.0,
        require_stable_samples=1,
        intensity_warn=1.0,
        intensity_strong=2.0,
    )
    ts_now = 4_000_000.0
    db = GammaDBStub(spot_price=20_000.0, rows=[_row("call", gamma=1.0, oi=1.0, ts=ts_now)])
    det = GammaFlipDetector(cfg, "TEST")

    det.poll(ts_now, db, "TEST")
    db.rows = [_row("put", gamma=2.0, oi=1.0, ts=ts_now + 30)]
    ev = det.poll(ts_now + 30.0, db, "TEST")
    assert ev is not None
    rules = _rules_for_event(ev)
    assert any(r["rule"] == "R36" for r in rules)
    assert any(r["severity"] in ("MEDIUM", "HIGH") for r in rules)
