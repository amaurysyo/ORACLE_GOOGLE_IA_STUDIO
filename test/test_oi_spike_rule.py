import datetime as dt

from oraculo.detect.macro_detectors import OISpikeCfg, OISpikeDetector
from oraculo.detect.metrics_engine import Snapshot
from oraculo.rules.engine import RuleContext, eval_rules


class DocDBStub:
    def __init__(self, event_time: dt.datetime, value: float):
        self.row = {"event_time": event_time, "value": value}

    def fetchrow(self, query, *args):  # noqa: ANN001
        if "metrics_series" in query and "oi_delta_pct_doc" in query:
            return self.row
        return None


class FallbackOIStub:
    def __init__(self, now_ts: float, oi_now: float, prev_ts: float, oi_prev: float):
        self.now_row = {
            "event_time": dt.datetime.fromtimestamp(now_ts, tz=dt.timezone.utc),
            "open_interest": oi_now,
        }
        self.prev_row = {
            "event_time": dt.datetime.fromtimestamp(prev_ts, tz=dt.timezone.utc),
            "open_interest": oi_prev,
        }

    def fetchrow(self, query, *args):  # noqa: ANN001
        if "metrics_series" in query and "oi_delta_pct_doc" in query:
            return None
        if "binance_futures.open_interest" in query:
            ts_limit = args[1].timestamp() if args and hasattr(args[1], "timestamp") else 0.0
            if ts_limit <= self.prev_row["event_time"].timestamp() + 1e-6:
                return self.prev_row
            return self.now_row
        return None


def _rules_for_event(ev):
    return eval_rules(
        {"type": ev.kind, "side": ev.side, "price": ev.price, "intensity": ev.intensity, "fields": ev.fields},
        RuleContext("TEST"),
    )


def test_oi_spike_buy_doc_series():
    cfg = OISpikeCfg(enabled=True, retrigger_s=30)
    det = OISpikeDetector(cfg, "TEST")
    now = 1_000.0
    det.update_wmid(now - cfg.momentum_window_s, 50_000.0)
    det.update_wmid(now, 50_012.0)
    db = DocDBStub(dt.datetime.fromtimestamp(now, tz=dt.timezone.utc), 1.2)
    ev = det.poll(now, db, Snapshot(wmid=50_012.0))
    assert ev is not None
    assert ev.side == "buy"
    rules = _rules_for_event(ev)
    assert any(r["rule"] == "R28" for r in rules)


def test_oi_spike_sell_doc_series():
    cfg = OISpikeCfg(enabled=True, retrigger_s=30)
    det = OISpikeDetector(cfg, "TEST")
    now = 2_000.0
    det.update_wmid(now - cfg.momentum_window_s, 50_000.0)
    det.update_wmid(now, 49_988.0)
    db = DocDBStub(dt.datetime.fromtimestamp(now, tz=dt.timezone.utc), 1.2)
    ev = det.poll(now, db, Snapshot(wmid=49_988.0))
    assert ev is not None
    assert ev.side == "sell"
    rules = _rules_for_event(ev)
    assert any(r["rule"] == "R29" for r in rules)


def test_oi_spike_requires_momentum_direction():
    cfg = OISpikeCfg(enabled=True, retrigger_s=30)
    det = OISpikeDetector(cfg, "TEST")
    now = 3_000.0
    det.update_wmid(now - cfg.momentum_window_s, 50_000.0)
    det.update_wmid(now, 50_003.0)  # below mom_warn_usd
    db = DocDBStub(dt.datetime.fromtimestamp(now, tz=dt.timezone.utc), 1.2)
    ev = det.poll(now, db, Snapshot(wmid=50_003.0))
    assert ev is None


def test_oi_spike_retrigger_blocks_duplicates():
    cfg = OISpikeCfg(enabled=True, retrigger_s=60)
    det = OISpikeDetector(cfg, "TEST")
    now = 4_000.0
    det.update_wmid(now - cfg.momentum_window_s, 50_000.0)
    det.update_wmid(now, 50_015.0)
    db = DocDBStub(dt.datetime.fromtimestamp(now, tz=dt.timezone.utc), 1.2)
    ev1 = det.poll(now, db, Snapshot(wmid=50_015.0))
    assert ev1 is not None
    ev2 = det.poll(now + 1, db, Snapshot(wmid=50_015.0))
    assert ev2 is None


def test_oi_spike_fallback_open_interest():
    cfg = OISpikeCfg(enabled=True, retrigger_s=30)
    det = OISpikeDetector(cfg, "TEST")
    now = 5_000.0
    det.update_wmid(now - cfg.momentum_window_s, 50_000.0)
    det.update_wmid(now, 50_020.0)
    db = FallbackOIStub(now, 102_000.0, now - cfg.oi_window_s, 100_000.0)
    ev = det.poll(now, db, Snapshot(wmid=50_020.0))
    assert ev is not None
    assert ev.fields.get("metric_used_oi") == "open_interest_fallback"
    rules = _rules_for_event(ev)
    assert any(r["rule"] == "R28" for r in rules)
