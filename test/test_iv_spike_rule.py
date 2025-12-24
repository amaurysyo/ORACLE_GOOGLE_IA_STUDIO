import datetime as dt
from typing import Any, Dict, List

from oraculo.detect.macro_detectors import IVSpikeCfg, IVSpikeDetector
from oraculo.rules.engine import RuleContext, eval_rules


def _ts(ts: float) -> dt.datetime:
    return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)


def _surface_row(ts: float, iv: float, n_used: int = 10) -> Dict[str, Any]:
    return {"event_time": _ts(ts), "iv": iv, "rr_25d": 0.0, "bf_25d": 0.0, "n_used": n_used}


def _ticker_row(ts: float, iv: float, delta: float = 0.5, underlying_price: float = 20_000.0) -> Dict[str, Any]:
    return {
        "event_time": _ts(ts),
        "mark_iv": iv,
        "delta": delta,
        "underlying_price": underlying_price,
    }


class IVSpikeDBStub:
    def __init__(
        self,
        *,
        surface_now: Dict[str, Any] | None = None,
        surface_prev: Dict[str, Any] | None = None,
        ticker_rows: List[Dict[str, Any]] | None = None,
    ):
        self.surface_now = surface_now
        self.surface_prev = surface_prev
        self.ticker_rows = ticker_rows or []

    def fetchrow(self, query: str, *args):  # noqa: ANN001
        if "options_iv_surface" in query:
            if "event_time <=" in query:
                return self.surface_prev
            return self.surface_now
        return None

    def fetch(self, query: str, *args):  # noqa: ANN001
        if "options_ticker" in query:
            ts_now = args[1] if len(args) > 1 else None
            lookback = args[2] if len(args) > 2 else None
            ts_cut = args[3] if len(args) > 3 else None
            out: List[Dict[str, Any]] = []
            for row in self.ticker_rows:
                ts_val = row["event_time"].timestamp()
                if ts_cut is not None and ts_val > ts_cut:
                    continue
                if ts_now is not None and lookback is not None and ts_val < (ts_now - lookback):
                    continue
                out.append(row)
            return out
        return []


def _rules_for_event(ev):
    return eval_rules(
        {"type": ev.kind, "side": ev.side, "price": ev.price, "intensity": ev.intensity, "fields": ev.fields},
        RuleContext("TEST"),
    )


def _cfg(**kwargs):
    base = IVSpikeCfg(enabled=True)
    for k, v in kwargs.items():
        setattr(base, k, v)
    return base


def test_iv_spike_surface_emits_and_maps_r19():
    now_ts = 1_000_000.0
    cfg = _cfg()
    det = IVSpikeDetector(cfg, "TEST")
    db = IVSpikeDBStub(surface_now=_surface_row(now_ts, 0.60), surface_prev=_surface_row(now_ts - cfg.window_s, 0.45))

    ev = det.poll(now_ts, db, "TEST")

    assert ev is not None
    assert ev.kind == "iv_spike"
    assert ev.fields.get("source") == "surface"
    rules = _rules_for_event(ev)
    r19 = next(r for r in rules if r["rule"] == "R19")
    assert r19["severity"] == "HIGH"


def test_iv_spike_requires_positive_move():
    now_ts = 2_000_000.0
    cfg = _cfg()
    det = IVSpikeDetector(cfg, "TEST")
    db = IVSpikeDBStub(surface_now=_surface_row(now_ts, 0.50), surface_prev=_surface_row(now_ts - cfg.window_s, 0.60))

    ev = det.poll(now_ts, db, "TEST")

    assert ev is None


def test_iv_spike_uses_fallback_when_prev_missing():
    now_ts = 3_000_000.0
    cfg = _cfg(ticker_min_instruments=1)
    det = IVSpikeDetector(cfg, "TEST")
    ticker_rows = [
        _ticker_row(now_ts, 0.60),
        _ticker_row(now_ts - cfg.window_s - 10, 0.45),
    ]
    db = IVSpikeDBStub(surface_now=_surface_row(now_ts, 0.55), surface_prev=None, ticker_rows=ticker_rows)

    ev = det.poll(now_ts, db, "TEST")

    assert ev is not None
    assert ev.fields.get("source") == "ticker_fallback"
    assert ev.fields.get("n_used_now") and ev.fields.get("n_used_now") >= 1


def test_iv_spike_does_not_emit_when_fallback_disabled():
    now_ts = 4_000_000.0
    cfg = _cfg(fallback_to_ticker=False)
    det = IVSpikeDetector(cfg, "TEST")
    db = IVSpikeDBStub(surface_now=_surface_row(now_ts, 0.55), surface_prev=None)

    ev = det.poll(now_ts, db, "TEST")

    assert ev is None


def test_iv_spike_respects_cooldown():
    now_ts = 5_000_000.0
    cfg = _cfg(retrigger_s=100.0)
    det = IVSpikeDetector(cfg, "TEST")
    db = IVSpikeDBStub(surface_now=_surface_row(now_ts, 0.65), surface_prev=_surface_row(now_ts - cfg.window_s, 0.50))

    ev1 = det.poll(now_ts, db, "TEST")
    ev2 = det.poll(now_ts + 1, db, "TEST")

    assert ev1 is not None
    assert ev2 is None
