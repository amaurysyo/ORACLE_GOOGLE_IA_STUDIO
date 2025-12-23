import datetime as dt

from oraculo.detect.macro_detectors import TermStructureInvertCfg, TermStructureInvertDetector
from oraculo.rules.engine import RuleContext, eval_rules


class SurfaceStub:
    def __init__(self, rows=None):
        self.rows = rows or {}

    def fetchrow(self, query, *args):  # noqa: ANN001
        if "options_iv_surface" not in query:
            return None
        tenor_bucket = args[1] if len(args) > 1 else None
        return self.rows.get(tenor_bucket)


def _row(ts: float, iv: float, rr: float = 0.0, bf: float = 0.0, n_used: int = 10):
    return {
        "event_time": dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc),
        "iv": iv,
        "rr_25d": rr,
        "bf_25d": bf,
        "n_used": n_used,
    }


def _rules_for_event(ev):
    return eval_rules(
        {"type": ev.kind, "side": ev.side, "price": ev.price, "intensity": ev.intensity, "fields": ev.fields},
        RuleContext("TEST"),
    )


def _default_cfg(**kwargs):
    base = TermStructureInvertCfg(enabled=True)
    for k, v in kwargs.items():
        setattr(base, k, v)
    return base


def test_term_structure_emits_and_maps_r37():
    cfg = _default_cfg()
    det = TermStructureInvertDetector(cfg, "TEST")
    now_ts = 1_000_000.0
    stub = SurfaceStub(
        {
            cfg.short_tenor_bucket: _row(now_ts, 0.30, rr=0.01, bf=0.02, n_used=50),
            cfg.long_tenor_bucket: _row(now_ts, 0.18, rr=0.005, bf=0.01, n_used=60),
        }
    )

    ev = det.poll(now_ts, stub, "TEST")
    assert ev is not None
    assert ev.kind == "term_structure_invert"
    rules = _rules_for_event(ev)
    assert any(r["rule"] == "R37" for r in rules)
    assert ev.side == "invert"


def test_term_structure_blocks_negative_spread_when_required():
    cfg = _default_cfg(require_positive_inversion=True)
    det = TermStructureInvertDetector(cfg, "TEST")
    now_ts = 2_000_000.0
    stub = SurfaceStub(
        {
            cfg.short_tenor_bucket: _row(now_ts, 0.10),
            cfg.long_tenor_bucket: _row(now_ts, 0.20),
        }
    )

    ev = det.poll(now_ts, stub, "TEST")
    assert ev is None


def test_term_structure_requires_both_legs_when_configured():
    cfg = _default_cfg(require_both_present=True)
    det = TermStructureInvertDetector(cfg, "TEST")
    now_ts = 3_000_000.0
    stub = SurfaceStub(
        {
            cfg.short_tenor_bucket: _row(now_ts, 0.12),
        }
    )

    ev = det.poll(now_ts, stub, "TEST")
    assert ev is None


def test_term_structure_retrigger_blocks_second_event():
    cfg = _default_cfg(retrigger_s=100.0)
    det = TermStructureInvertDetector(cfg, "TEST")
    now_ts = 4_000_000.0
    stub = SurfaceStub(
        {
            cfg.short_tenor_bucket: _row(now_ts, 0.30),
            cfg.long_tenor_bucket: _row(now_ts, 0.20),
        }
    )
    ev1 = det.poll(now_ts, stub, "TEST")
    assert ev1 is not None
    ev2 = det.poll(now_ts + 1, stub, "TEST")
    assert ev2 is None


def test_velocity_gate_blocks_and_emits_on_high_velocity():
    cfg = _default_cfg(use_velocity_gate=True)
    det = TermStructureInvertDetector(cfg, "TEST")
    now_ts = 5_000_000.0
    stub = SurfaceStub(
        {
            cfg.short_tenor_bucket: _row(now_ts, 0.30),
            cfg.long_tenor_bucket: _row(now_ts, 0.20),
        }
    )

    ev1 = det.poll(now_ts, stub, "TEST")
    assert ev1 is None  # falta velocidad inicial

    stub.rows[cfg.short_tenor_bucket] = _row(now_ts + 5, 0.3005)
    stub.rows[cfg.long_tenor_bucket] = _row(now_ts + 5, 0.200)
    ev2 = det.poll(now_ts + 5, stub, "TEST")
    assert ev2 is None  # velocidad por debajo del umbral

    stub.rows[cfg.short_tenor_bucket] = _row(now_ts + 10, 0.34)
    stub.rows[cfg.long_tenor_bucket] = _row(now_ts + 10, 0.200)
    ev3 = det.poll(now_ts + 10, stub, "TEST")
    assert ev3 is not None
    assert "vel_per_s" in ev3.fields


def test_term_structure_fields_audit_present():
    cfg = _default_cfg()
    det = TermStructureInvertDetector(cfg, "TEST")
    now_ts = 6_000_000.0
    stub = SurfaceStub(
        {
            cfg.short_tenor_bucket: _row(now_ts, 0.40, rr=0.02, bf=0.03, n_used=30),
            cfg.long_tenor_bucket: _row(now_ts, 0.25, rr=0.01, bf=0.02, n_used=40),
        }
    )
    ev = det.poll(now_ts, stub, "TEST")
    assert ev is not None
    f = ev.fields
    for key in ["iv_short", "iv_long", "spread", "ts_short", "ts_long", "rr_short", "bf_short", "rr_long", "bf_long"]:
        assert key in f
