import datetime as dt

from oraculo.detect.macro_detectors import BasisDislocationCfg, BasisDislocationDetector
from oraculo.rules.engine import RuleContext, eval_rules


class MetricsDBStub:
    def __init__(self):
        self.metric_rows = {}
        self.funding_now = None
        self.funding_past = None

    def add_metric(self, metric: str, window_s: int, ts: float, value: float) -> None:
        row = {"event_time": dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc), "value": value}
        self.metric_rows[(metric, int(window_s))] = [row]

    def set_funding(self, now_ts: float, now_rate: float, past_ts: float, past_rate: float) -> None:
        self.funding_now = {
            "event_time": dt.datetime.fromtimestamp(now_ts, tz=dt.timezone.utc),
            "funding_rate": now_rate,
        }
        self.funding_past = {
            "event_time": dt.datetime.fromtimestamp(past_ts, tz=dt.timezone.utc),
            "funding_rate": past_rate,
        }

    def fetchrow(self, query, *args):  # noqa: ANN001
        if "metrics_series" in query:
            metric, window_s, _instrument_id = args
            rows = self.metric_rows.get((metric, int(window_s))) or []
            return rows[-1] if rows else None
        if "mark_funding" in query:
            if len(args) == 1:
                return self.funding_now
            return self.funding_past
        return None


def _rules_for_event(ev):
    return eval_rules(
        {"type": ev.kind, "side": ev.side, "price": ev.price, "intensity": ev.intensity, "fields": ev.fields},
        RuleContext("TEST"),
    )


def _default_cfg(**kwargs):
    base = dict(
        enabled=True,
        poll_s=10.0,
        retrigger_s=180.0,
        metric_source="auto",
        basis_warn_bps=60.0,
        basis_strong_bps=120.0,
        vel_warn_bps_s=1.0,
        vel_strong_bps_s=2.0,
        require_funding_confirm=True,
        funding_window_s=900.0,
        funding_trend_warn=0.00001,
        funding_trend_strong=0.00003,
        allow_emit_without_funding=False,
        clamp_abs_basis_bps=1000.0,
        clamp_abs_vel_bps_s=50.0,
    )
    base.update(kwargs)
    return BasisDislocationCfg(**base)


def test_basis_dislocation_emits_and_maps_r34():
    cfg = _default_cfg()
    det = BasisDislocationDetector(cfg, "TEST")
    db = MetricsDBStub()
    now_ts = 1_000_000.0
    db.add_metric("basis_bps_doc", 120, now_ts, 150.0)
    db.add_metric("basis_vel_bps_s_doc", 120, now_ts, 2.5)
    db.set_funding(now_ts, 0.02, now_ts - cfg.funding_window_s, 0.0)

    ev = det.poll(now_ts, db, "TEST")
    assert ev is not None
    assert ev.kind == "basis_dislocation"
    rules = _rules_for_event(ev)
    assert any(r["rule"] == "R34" for r in rules)


def test_basis_dislocation_blocks_on_low_funding_trend():
    cfg = _default_cfg()
    det = BasisDislocationDetector(cfg, "TEST")
    db = MetricsDBStub()
    now_ts = 2_000_000.0
    db.add_metric("basis_bps_doc", 120, now_ts, 150.0)
    db.add_metric("basis_vel_bps_s_doc", 120, now_ts, 2.5)
    db.set_funding(now_ts, 0.0005, now_ts - cfg.funding_window_s, 0.0004)  # trend below warn
    ev = det.poll(now_ts, db, "TEST")
    assert ev is None


def test_basis_dislocation_emit_without_funding_when_allowed():
    cfg = _default_cfg(allow_emit_without_funding=True)
    det = BasisDislocationDetector(cfg, "TEST")
    db = MetricsDBStub()
    now_ts = 3_000_000.0
    db.add_metric("basis_bps_doc", 120, now_ts, 200.0)
    db.add_metric("basis_vel_bps_s_doc", 120, now_ts, 2.0)
    ev = det.poll(now_ts, db, "TEST")
    assert ev is not None
    assert ev.fields.get("funding_missing") is True


def test_basis_dislocation_doc_missing_metrics_returns_none():
    cfg = _default_cfg(metric_source="doc")
    det = BasisDislocationDetector(cfg, "TEST")
    db = MetricsDBStub()
    now_ts = 4_000_000.0
    ev = det.poll(now_ts, db, "TEST")
    assert ev is None


def test_basis_dislocation_retrigger_blocks_second_event():
    cfg = _default_cfg(retrigger_s=60.0)
    det = BasisDislocationDetector(cfg, "TEST")
    db = MetricsDBStub()
    now_ts = 5_000_000.0
    db.add_metric("basis_bps_doc", 120, now_ts, 200.0)
    db.add_metric("basis_vel_bps_s_doc", 120, now_ts, 3.0)
    db.set_funding(now_ts, 0.01, now_ts - cfg.funding_window_s, 0.0)
    ev1 = det.poll(now_ts, db, "TEST")
    assert ev1 is not None
    ev2 = det.poll(now_ts + 1, db, "TEST")
    assert ev2 is None


def test_basis_dislocation_fields_audit_and_intensity():
    cfg = _default_cfg()
    det = BasisDislocationDetector(cfg, "TEST")
    db = MetricsDBStub()
    now_ts = 6_000_000.0
    db.add_metric("basis_bps_doc", 120, now_ts, 100.0)
    db.add_metric("basis_vel_bps_s_doc", 120, now_ts, 1.5)
    db.set_funding(now_ts, 0.05, now_ts - cfg.funding_window_s, 0.0)
    ev = det.poll(now_ts, db, "TEST")
    assert ev is not None
    assert ev.fields.get("metric_used_basis") == "basis_bps_doc"
    assert ev.fields.get("metric_used_vel") == "basis_vel_bps_s_doc"
    assert ev.fields.get("i_basis") >= 0.0
    assert ev.fields.get("i_vel") >= 0.0
    assert 0.0 <= ev.intensity <= 1.0
    assert "funding_trend" in ev.fields
