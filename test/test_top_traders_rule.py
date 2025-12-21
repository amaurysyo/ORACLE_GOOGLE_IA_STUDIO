import datetime as dt

from oraculo.detect.macro_detectors import TopTradersCfg, TopTradersDetector
from oraculo.rules.engine import RuleContext, eval_rules


def _mk_row(ts: float, long_ratio: float, short_ratio: float, meta=None):
    return {
        "event_time": dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc),
        "long_ratio": long_ratio,
        "short_ratio": short_ratio,
        "meta": meta or {},
    }


class TTDBStub:
    def __init__(self, acc_row=None, pos_row=None):
        self.acc_row = acc_row
        self.pos_row = pos_row

    def fetchrow(self, query, *args):  # noqa: ANN001
        if "top_trader_account_ratio" in query:
            return self.acc_row
        if "top_trader_position_ratio" in query:
            return self.pos_row
        return None


def _rules_for_event(ev):
    return eval_rules(
        {"type": ev.kind, "side": ev.side, "price": ev.price, "intensity": ev.intensity, "fields": ev.fields},
        RuleContext("TEST"),
    )


def test_top_traders_long_account_bias_maps_to_r30():
    cfg = TopTradersCfg(enabled=True, retrigger_s=1)
    det = TopTradersDetector(cfg, "TEST")
    now = 1_000.0
    acc_row = _mk_row(now, 0.65, 0.35)
    pos_row = _mk_row(now, 0.40, 0.50)
    db = TTDBStub(acc_row=acc_row, pos_row=pos_row)
    ev = det.poll(now, db, "TEST")
    assert ev is not None
    assert ev.side == "long"
    rules = _rules_for_event(ev)
    assert any(r["rule"] == "R30" for r in rules)


def test_top_traders_short_position_bias_maps_to_r31():
    cfg = TopTradersCfg(enabled=True, retrigger_s=1, choose_by="position_only")
    det = TopTradersDetector(cfg, "TEST")
    now = 2_000.0
    pos_row = _mk_row(now, 0.10, 0.85)
    db = TTDBStub(acc_row=None, pos_row=pos_row)
    ev = det.poll(now, db, "TEST")
    assert ev is not None
    assert ev.side == "short"
    rules = _rules_for_event(ev)
    assert any(r["rule"] == "R31" for r in rules)


def test_top_traders_require_both_blocks_when_only_one_metric_crosses():
    cfg = TopTradersCfg(enabled=True, retrigger_s=1, require_both=True)
    det = TopTradersDetector(cfg, "TEST")
    now = 3_000.0
    acc_row = _mk_row(now, 0.72, 0.20)
    pos_row = _mk_row(now, 0.50, 0.30)
    db = TTDBStub(acc_row=acc_row, pos_row=pos_row)
    ev = det.poll(now, db, "TEST")
    assert ev is None


def test_top_traders_retrigger_blocks_duplicates():
    cfg = TopTradersCfg(enabled=True, retrigger_s=30)
    det = TopTradersDetector(cfg, "TEST")
    now = 4_000.0
    acc_row = _mk_row(now, 0.75, 0.20)
    pos_row = _mk_row(now, 0.70, 0.25)
    db = TTDBStub(acc_row=acc_row, pos_row=pos_row)
    ev1 = det.poll(now, db, "TEST")
    assert ev1 is not None
    ev2 = det.poll(now + 1, db, "TEST")
    assert ev2 is None


def test_top_traders_choose_by_modes_select_expected_side():
    acc_row = _mk_row(5_000.0, 0.75, 0.55)
    pos_row = _mk_row(5_000.0, 0.35, 0.78)

    det_acc = TopTradersDetector(TopTradersCfg(enabled=True, choose_by="account_only", retrigger_s=1), "TEST")
    ev_acc = det_acc.poll(5_000.0, TTDBStub(acc_row=acc_row, pos_row=None), "TEST")
    assert ev_acc is not None
    assert ev_acc.side == "long"

    det_pos = TopTradersDetector(TopTradersCfg(enabled=True, choose_by="position_only", retrigger_s=1), "TEST")
    ev_pos = det_pos.poll(5_000.0, TTDBStub(acc_row=None, pos_row=pos_row), "TEST")
    assert ev_pos is not None
    assert ev_pos.side == "short"


def test_top_traders_fields_include_scores_and_ratios():
    cfg = TopTradersCfg(enabled=True, retrigger_s=1, choose_by="account_only")
    det = TopTradersDetector(cfg, "TEST")
    now = 6_000.0
    acc_row = _mk_row(now, 0.70, 0.20, meta={"source": "unit"})
    db = TTDBStub(acc_row=acc_row, pos_row=None)
    ev = det.poll(now, db, "TEST")
    assert ev is not None
    assert "score_acc_long" in ev.fields
    assert "score_acc_short" in ev.fields
    assert ev.fields["acc_long_ratio"] == 0.70
    assert ev.fields["acc_meta"] == {"source": "unit"}
