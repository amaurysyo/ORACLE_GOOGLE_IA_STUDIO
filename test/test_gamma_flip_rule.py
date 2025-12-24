import datetime as dt
import math

from oraculo.detect.macro_detectors import GammaFlipCfg, GammaFlipDetector
from oraculo.rules.engine import RuleContext, eval_rules


class GammaDBStub:
    def __init__(self, spot_rows: list[dict], instruments: dict[str, dict], ticker_rows: list[dict]):
        self.spot_rows = spot_rows
        self.instruments = instruments
        self.ticker_rows = ticker_rows

    def fetchrow(self, query, *args):  # noqa: ANN001
        _ = query
        underlying = args[0]
        ts_now = args[1]
        lookback_s = args[2]
        cutoff = dt.datetime.fromtimestamp(ts_now, tz=dt.timezone.utc) - dt.timedelta(seconds=lookback_s)
        rows = [
            r
            for r in self.spot_rows
            if r.get("underlying") == underlying and r.get("event_time") and r["event_time"] >= cutoff and r.get("underlying_price") is not None
        ]
        rows.sort(key=lambda r: r["event_time"], reverse=True)
        return rows[0] if rows else None

    def fetch(self, query, *args):  # noqa: ANN001
        _ = query
        ts_now = args[0]
        lookback_s = args[1]

        args_idx = 2
        min_oi = None
        if len(args) in (7, 9):
            min_oi = args[args_idx]
            args_idx += 1

        underlying = args[args_idx]
        expiry_days = args[args_idx + 1]
        spot_ref = args[args_idx + 2]
        moneyness_abs = args[args_idx + 3]
        delta_gate = len(args) - args_idx > 4
        delta_min = args[args_idx + 4] if delta_gate else None
        delta_max = args[args_idx + 5] if delta_gate else None

        cutoff = dt.datetime.fromtimestamp(ts_now, tz=dt.timezone.utc) - dt.timedelta(seconds=lookback_s)
        expiry_hi = dt.datetime.fromtimestamp(ts_now, tz=dt.timezone.utc) + dt.timedelta(days=expiry_days)
        expiry_lo = dt.datetime.fromtimestamp(ts_now, tz=dt.timezone.utc)

        filtered = []
        for row in self.ticker_rows:
            if row.get("event_time") is None:
                continue
            if row["event_time"] < cutoff:
                continue
            if row.get("gamma") is None or row.get("open_interest") is None:
                continue
            if min_oi is not None and row.get("open_interest", 0) < min_oi:
                continue
            filtered.append(row)

        latest_by_inst: dict[str, dict] = {}
        for row in sorted(filtered, key=lambda r: r["event_time"], reverse=True):
            inst_id = row.get("instrument_id")
            if not inst_id or inst_id in latest_by_inst:
                continue
            latest_by_inst[inst_id] = row

        out = []
        for inst_id, row in latest_by_inst.items():
            inst = self.instruments.get(inst_id)
            if not inst:
                continue
            if inst.get("underlying") != underlying:
                continue
            expiration = inst.get("expiration")
            if expiration is None or expiration < expiry_lo or expiration > expiry_hi:
                continue
            strike = inst.get("strike")
            if strike is None:
                continue
            if not (spot_ref * (1 - moneyness_abs) <= strike <= spot_ref * (1 + moneyness_abs)):
                continue
            delta_val = row.get("delta")
            if delta_gate and (delta_val is None or not (delta_min <= abs(delta_val) <= delta_max)):
                continue
            out.append(
                {
                    "instrument_id": inst_id,
                    "event_time": row["event_time"],
                    "gamma": row.get("gamma"),
                    "open_interest": row.get("open_interest"),
                    "delta": row.get("delta"),
                    "underlying_price": row.get("underlying_price"),
                    "option_type": inst.get("option_type"),
                    "strike": strike,
                    "expiration": expiration,
                }
            )
        return out


def _spot_row(underlying: str, price: float, ts: float):
    return {
        "underlying": underlying,
        "underlying_price": price,
        "event_time": dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc),
    }


def _instrument(
    option_type: str,
    strike: float = 20_000.0,
    expiration_ts: float = 10_000_000.0,
    underlying: str = "BTC",
    instrument_id: str | None = None,
):
    return {
        "instrument_id": instrument_id or f"{option_type}-id",
        "option_type": option_type,
        "strike": strike,
        "expiration": dt.datetime.fromtimestamp(expiration_ts, tz=dt.timezone.utc),
        "underlying": underlying,
    }


def _ticker_row(
    instrument_id: str,
    gamma: float,
    oi: float = 1.0,
    delta: float = 0.5,
    ts: float = 0.0,
    underlying_price: float = 20_000.0,
):
    return {
        "instrument_id": instrument_id,
        "event_time": dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc),
        "gamma": gamma,
        "open_interest": oi,
        "delta": delta,
        "underlying_price": underlying_price,
    }


def _expiration_ts(base_ts: float, days: float = 10.0) -> float:
    return base_ts + 86_400.0 * days


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
    inst = _instrument("call", instrument_id="call-id", expiration_ts=_expiration_ts(ts_now))
    db = GammaDBStub(
        spot_rows=[_spot_row("BTC", 20_000.0, ts_now)],
        instruments={inst["instrument_id"]: inst},
        ticker_rows=[_ticker_row(inst["instrument_id"], gamma=1.0, oi=1.0, ts=ts_now)],
    )
    det = GammaFlipDetector(cfg, "TEST")

    ev = det.poll(ts_now, db, "TEST")
    assert ev is None


def test_gamma_flip_requires_stable_samples_before_emit():
    cfg = _cfg(min_instruments=1, min_abs_net_gex=1.0, require_stable_samples=2)
    ts_now = 1_000_000.0
    inst_call = _instrument("call", instrument_id="call-id", expiration_ts=_expiration_ts(ts_now))
    inst_put = _instrument("put", instrument_id="put-id", expiration_ts=_expiration_ts(ts_now))
    instruments = {inst_call["instrument_id"]: inst_call, inst_put["instrument_id"]: inst_put}
    db = GammaDBStub(
        spot_rows=[_spot_row("BTC", 20_000.0, ts_now)],
        instruments=instruments,
        ticker_rows=[_ticker_row(inst_call["instrument_id"], gamma=1.0, oi=1.0, ts=ts_now)],
    )
    det = GammaFlipDetector(cfg, "TEST")

    det.poll(ts_now, db, "TEST")  # initialize prev_sign

    db.ticker_rows = [_ticker_row(inst_put["instrument_id"], gamma=1.0, oi=1.0, ts=ts_now + 30)]
    ev_pending = det.poll(ts_now + 30.0, db, "TEST")
    assert ev_pending is None

    db.ticker_rows = [_ticker_row(inst_put["instrument_id"], gamma=1.0, oi=1.0, ts=ts_now + 60)]
    ev = det.poll(ts_now + 60.0, db, "TEST")
    assert ev is not None
    assert ev.kind == "gamma_flip"
    assert ev.fields.get("prev_sign") == 1
    assert ev.fields.get("new_sign") == -1


def test_gamma_flip_cooldown_blocks_retrigger():
    cfg = _cfg(min_instruments=1, min_abs_net_gex=1.0, require_stable_samples=1, retrigger_s=300)
    ts_now = 2_000_000.0
    inst_call = _instrument("call", instrument_id="call-id", expiration_ts=_expiration_ts(ts_now))
    inst_put = _instrument("put", instrument_id="put-id", expiration_ts=_expiration_ts(ts_now))
    instruments = {inst_call["instrument_id"]: inst_call, inst_put["instrument_id"]: inst_put}
    db = GammaDBStub(
        spot_rows=[_spot_row("BTC", 20_000.0, ts_now)],
        instruments=instruments,
        ticker_rows=[_ticker_row(inst_call["instrument_id"], gamma=1.0, oi=1.0, ts=ts_now)],
    )
    det = GammaFlipDetector(cfg, "TEST")

    det.poll(ts_now, db, "TEST")
    db.ticker_rows = [_ticker_row(inst_put["instrument_id"], gamma=1.0, oi=1.0, ts=ts_now + 30)]
    ev = det.poll(ts_now + 30.0, db, "TEST")
    assert ev is not None

    db.ticker_rows = [_ticker_row(inst_call["instrument_id"], gamma=1.0, oi=1.0, ts=ts_now + 50)]
    ev_cooldown = det.poll(ts_now + 50.0, db, "TEST")
    assert ev_cooldown is None


def test_gamma_flip_gates_on_minimums():
    cfg = _cfg(min_instruments=2, min_abs_net_gex=1e12)
    ts_now = 3_000_000.0
    inst_call = _instrument("call", instrument_id="call-id", expiration_ts=_expiration_ts(ts_now))
    db = GammaDBStub(
        spot_rows=[_spot_row("BTC", 20_000.0, ts_now)],
        instruments={inst_call["instrument_id"]: inst_call},
        ticker_rows=[_ticker_row(inst_call["instrument_id"], gamma=1.0, oi=1.0, ts=ts_now)],
    )
    det = GammaFlipDetector(cfg, "TEST")

    ev = det.poll(ts_now, db, "TEST")
    assert ev is None

    db.ticker_rows = [
        _ticker_row(inst_call["instrument_id"], gamma=1.0, oi=1.0, ts=ts_now + 40),
        _ticker_row(inst_call["instrument_id"], gamma=1.0, oi=1.0, ts=ts_now + 40),
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
    inst_call = _instrument("call", instrument_id="call-id", expiration_ts=_expiration_ts(ts_now))
    inst_put = _instrument("put", instrument_id="put-id", expiration_ts=_expiration_ts(ts_now))
    instruments = {inst_call["instrument_id"]: inst_call, inst_put["instrument_id"]: inst_put}
    db = GammaDBStub(
        spot_rows=[_spot_row("BTC", 20_000.0, ts_now)],
        instruments=instruments,
        ticker_rows=[_ticker_row(inst_call["instrument_id"], gamma=1.0, oi=1.0, ts=ts_now)],
    )
    det = GammaFlipDetector(cfg, "TEST")

    det.poll(ts_now, db, "TEST")
    db.ticker_rows = [_ticker_row(inst_put["instrument_id"], gamma=2.0, oi=1.0, ts=ts_now + 30)]
    ev = det.poll(ts_now + 30.0, db, "TEST")
    assert ev is not None
    rules = _rules_for_event(ev)
    assert any(r["rule"] == "R36" for r in rules)
    assert any(r["severity"] in ("MEDIUM", "HIGH") for r in rules)


def test_gamma_flip_spot_filters_by_underlying():
    cfg = _cfg()
    ts_now = 5_000_000.0
    inst = _instrument("call", instrument_id="call-id", underlying="BTC", expiration_ts=_expiration_ts(ts_now))
    db = GammaDBStub(
        spot_rows=[
            _spot_row("ETH", 1_500.0, ts_now + 5),
            _spot_row("BTC", 20_000.0, ts_now),
        ],
        instruments={inst["instrument_id"]: inst},
        ticker_rows=[],
    )
    det = GammaFlipDetector(cfg, "TEST")

    spot_row = det._spot_row(db, ts_now)
    assert spot_row is not None
    assert spot_row.get("underlying_price") == 20_000.0


def test_gamma_flip_uses_latest_tick_per_instrument():
    cfg = _cfg(delta_gate_enabled=False, min_instruments=1, min_abs_net_gex=1.0, require_stable_samples=1)
    ts_now = 6_000_000.0
    inst_call = _instrument("call", instrument_id="call-id", underlying="BTC", expiration_ts=_expiration_ts(ts_now))
    inst_put = _instrument("put", instrument_id="put-id", underlying="BTC", expiration_ts=_expiration_ts(ts_now))
    instruments = {inst_call["instrument_id"]: inst_call, inst_put["instrument_id"]: inst_put}
    spot_rows = [_spot_row("BTC", 20_000.0, ts_now)]
    db = GammaDBStub(
        spot_rows=spot_rows,
        instruments=instruments,
        ticker_rows=[_ticker_row(inst_call["instrument_id"], gamma=1.0, oi=1.0, ts=ts_now)],
    )
    det = GammaFlipDetector(cfg, "TEST")

    det.poll(ts_now, db, "TEST")  # initialize prev_sign with call-only snapshot

    db.ticker_rows = [
        _ticker_row(inst_call["instrument_id"], gamma=0.5, oi=1.0, ts=ts_now - 10),
        _ticker_row(inst_call["instrument_id"], gamma=1.5, oi=1.0, ts=ts_now + 10),
        _ticker_row(inst_put["instrument_id"], gamma=1.0, oi=1.0, ts=ts_now + 5),
        _ticker_row(inst_put["instrument_id"], gamma=2.0, oi=1.0, ts=ts_now + 20),
    ]
    ev = det.poll(ts_now + 20.0, db, "TEST")
    assert ev is not None
    assert ev.fields.get("n_used") == 2  # one per instrument_id
    expected_net_gex = (1.5 * 1.0 * 20_000.0 * 20_000.0) - (2.0 * 1.0 * 20_000.0 * 20_000.0)
    assert math.isclose(ev.fields.get("net_gex", 0.0), expected_net_gex)
