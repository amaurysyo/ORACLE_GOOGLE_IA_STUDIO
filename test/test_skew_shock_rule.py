import datetime as dt

from oraculo.detect.macro_detectors import SkewShockCfg, SkewShockDetector
from oraculo.rules.engine import RuleContext, eval_rules


class IVSurfaceDBStub:
    def __init__(self, row_now=None, row_past=None):
        self.row_now = row_now
        self.row_past = row_past

    def fetchrow(self, query, *args):  # noqa: ANN001
        if "iv_surface_1m" in query:
            if "bucket <=" in query:
                return self.row_past
            return self.row_now
        return None


def _rules_for_event(ev):
    return eval_rules(
        {"type": ev.kind, "side": ev.side, "price": ev.price, "intensity": ev.intensity, "fields": ev.fields},
        RuleContext("TEST"),
    )


def _default_cfg(**kwargs):
    base = SkewShockCfg(enabled=True)
    for k, v in kwargs.items():
        setattr(base, k, v)
    return base


def _row(ts: float, rr: float, bf: float = 0.0, iv: float = 0.0):
    return {
        "bucket": dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc),
        "rr_25d_avg": rr,
        "bf_25d_avg": bf,
        "iv_avg": iv,
    }


def test_skew_shock_emits_and_maps_r35():
    cfg = _default_cfg()
    det = SkewShockDetector(cfg, "TEST")
    now_ts = 1_000_000.0
    db = IVSurfaceDBStub(row_now=_row(now_ts, 0.05), row_past=_row(now_ts - cfg.window_s, 0.02))

    ev = det.poll(now_ts, db, "TEST")
    assert ev is not None
    assert ev.kind == "skew_shock"
    rules = _rules_for_event(ev)
    assert any(r["rule"] == "R35" for r in rules)
    assert ev.side == "bull"


def test_skew_shock_blocks_on_low_velocity():
    cfg = _default_cfg()
    det = SkewShockDetector(cfg, "TEST")
    now_ts = 2_000_000.0
    db = IVSurfaceDBStub(row_now=_row(now_ts, 0.05), row_past=_row(now_ts - 10_000, 0.0))
    ev = det.poll(now_ts, db, "TEST")
    assert ev is None


def test_skew_shock_retrigger_blocks_second_event():
    cfg = _default_cfg(retrigger_s=100.0)
    det = SkewShockDetector(cfg, "TEST")
    now_ts = 3_000_000.0
    db = IVSurfaceDBStub(row_now=_row(now_ts, 0.10), row_past=_row(now_ts - cfg.window_s, 0.05))
    ev1 = det.poll(now_ts, db, "TEST")
    assert ev1 is not None
    ev2 = det.poll(now_ts + 1, db, "TEST")
    assert ev2 is None


def test_skew_shock_missing_past_blocks_when_required():
    cfg = _default_cfg(require_recent_past=True)
    det = SkewShockDetector(cfg, "TEST")
    now_ts = 4_000_000.0
    db = IVSurfaceDBStub(row_now=_row(now_ts, 0.05), row_past=None)
    ev = det.poll(now_ts, db, "TEST")
    assert ev is None


def test_skew_shock_fields_audit_present():
    cfg = _default_cfg()
    det = SkewShockDetector(cfg, "TEST")
    now_ts = 5_000_000.0
    db = IVSurfaceDBStub(row_now=_row(now_ts, -0.03, bf=0.1, iv=0.5), row_past=_row(now_ts - cfg.window_s, -0.10, bf=0.2, iv=0.6))
    ev = det.poll(now_ts, db, "TEST")
    assert ev is not None
    f = ev.fields
    assert "rr_delta" in f and "rr_vel_per_s" in f
    assert "i_delta" in f and "i_vel" in f
    assert f.get("bucket_now") is not None and f.get("bucket_past") is not None
