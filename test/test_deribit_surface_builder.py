import datetime as dt

from oraculo.deribit.surface_builder import DeribitSurfaceBuilder, SurfaceBuilderCfg


class StubDB:
    def __init__(self, rows_by_expiry, spot=20000.0):
        self.rows_by_expiry = rows_by_expiry
        self.executed = []
        self.spot = spot

    async def fetch(self, sql, *args):  # noqa: ANN001
        sql_l = sql.lower()
        if "options_ticker" in sql_l and "join deribit.options_instruments" in sql_l:
            expiry = args[1]
            return list(self.rows_by_expiry.get(expiry, []))
        if "from deribit.options_instruments" in sql_l:
            expiries = sorted(self.rows_by_expiry.keys())
            return [{"expiry": exp} for exp in expiries]
        if "underlying_price" in sql_l:
            return [{"underlying_price": self.spot}]
        raise AssertionError(f"Unexpected query: {sql_l}")

    async def execute_many(self, sql, rows):  # noqa: ANN001
        self.executed.extend(rows)


def _rows_for_expiry(
    expiry,
    *,
    call_iv=0.60,
    put_iv=0.70,
    atm_iv=0.55,
    call_oi=10,
    put_oi=20,
    atm_oi=30,
    call_strike=19500,
    put_strike=19500,
    atm_strike=20000,
    include_call=True,
    include_put=True,
    include_atm=True,
):
    base_ts = dt.datetime(2025, 1, 1, 12, 0, tzinfo=dt.timezone.utc)
    rows = []
    if include_call:
        rows.append(
            {
                "instrument_id": f"DERIBIT:OPTIONS:BTC-{expiry:%d%b%y}-C",
                "event_time": base_ts,
                "mark_iv": call_iv,
                "delta": 0.24,
                "open_interest": call_oi,
                "bid": 1.0,
                "ask": 1.1,
                "underlying_price": 20000.0,
                "strike": call_strike,
                "option_type": "C",
                "expiry": expiry,
            }
        )
    if include_put:
        rows.append(
            {
                "instrument_id": f"DERIBIT:OPTIONS:BTC-{expiry:%d%b%y}-P",
                "event_time": base_ts,
                "mark_iv": put_iv,
                "delta": -0.26,
                "open_interest": put_oi,
                "bid": 1.2,
                "ask": 1.3,
                "underlying_price": 20000.0,
                "strike": put_strike,
                "option_type": "P",
                "expiry": expiry,
            }
        )
    if include_atm:
        rows.append(
            {
                "instrument_id": f"DERIBIT:OPTIONS:BTC-{expiry:%d%b%y}-ATM",
                "event_time": base_ts,
                "mark_iv": atm_iv,
                "delta": 0.49,
                "open_interest": atm_oi,
                "bid": 1.4,
                "ask": 1.6,
                "underlying_price": 20000.0,
                "strike": atm_strike,
                "option_type": "C",
                "expiry": expiry,
            }
        )
    return rows


def test_surface_builder_aggregates_multiple_expiries():
    async def _run():
        cfg = SurfaceBuilderCfg(enabled=True, lag_s=0, poll_s=0.1)
        builder = DeribitSurfaceBuilder(cfg)
        expiry_a = dt.date(2025, 1, 10)
        expiry_b = dt.date(2025, 1, 17)
        rows_by_expiry = {
            expiry_a: _rows_for_expiry(
                expiry_a, call_iv=0.60, put_iv=0.70, atm_iv=0.55, call_oi=10, put_oi=20, atm_oi=30, atm_strike=20000
            ),
            expiry_b: _rows_for_expiry(
                expiry_b, call_iv=0.50, put_iv=0.65, atm_iv=0.52, call_oi=40, put_oi=10, atm_oi=50, atm_strike=20500
            ),
        }
        db = StubDB(rows_by_expiry)
        ts_now = dt.datetime(2025, 1, 1, 12, 0, tzinfo=dt.timezone.utc).timestamp()
        rows = await builder.run_once(db, ts_now=ts_now)

        assert rows == 3
        assert len(db.executed) == 3
        by_bucket = {(r[2], r[3]): r for r in db.executed}
        tenor_row = by_bucket.get(("7_30d", "NA"))
        assert tenor_row, "missing aggregated tenor row"
        assert abs(tenor_row[4] - 0.53125) < 1e-6  # iv_atm weighted across expiries
        assert abs(tenor_row[5] - (-0.13125)) < 1e-6  # rr weighted by OI per expiry
        assert abs(tenor_row[6] - 0.071875) < 1e-6  # bf weighted by OI per expiry
        tenor_meta = tenor_row[7]
        assert tenor_meta["n_expiries_used"] == 2
        assert tenor_meta["weights_used"] == "oi"

        bucket_iv = by_bucket.get(("7_30d", "0.95_1.00"))
        assert bucket_iv and abs(bucket_iv[4] - 0.58125) < 1e-6
        meta_iv = bucket_iv[7]
        assert meta_iv["n_expiries_used"] == 2
        assert meta_iv["expiries"]["count"] == 2

        bucket_atm = by_bucket.get(("7_30d", "1.00_1.05"))
        assert bucket_atm and abs(bucket_atm[4] - 0.53125) < 1e-6

    import asyncio

    asyncio.run(_run())


def test_surface_builder_skips_missing_components():
    async def _run():
        cfg = SurfaceBuilderCfg(enabled=True, lag_s=0, poll_s=0.1)
        builder = DeribitSurfaceBuilder(cfg)
        expiry_complete = dt.date(2025, 1, 10)
        expiry_missing = dt.date(2025, 1, 17)
        rows_by_expiry = {
            expiry_complete: _rows_for_expiry(expiry_complete, call_iv=0.60, put_iv=0.70, atm_iv=0.55),
            expiry_missing: _rows_for_expiry(
                expiry_missing, call_iv=0.50, put_iv=0.65, atm_iv=0.52, include_call=False
            ),
        }
        db = StubDB(rows_by_expiry)
        ts_now = dt.datetime(2025, 1, 1, 12, 0, tzinfo=dt.timezone.utc).timestamp()
        rows = await builder.run_once(db, ts_now=ts_now)

        assert rows == 3  # only one tenor + two moneyness buckets
        by_bucket = {(r[2], r[3]): r for r in db.executed}
        tenor_row = by_bucket[("7_30d", "NA")]
        tenor_meta = tenor_row[7]
        assert tenor_meta["n_expiries_used"] == 1
        assert tenor_meta["n_components_missing"]["call"] == 1
        assert abs(tenor_row[5] - (-0.10)) < 1e-6
        assert abs(tenor_row[6] - 0.10) < 1e-6

    import asyncio

    asyncio.run(_run())


def test_surface_builder_idempotent_same_bucket():
    async def _run():
        cfg = SurfaceBuilderCfg(enabled=True, lag_s=0, poll_s=0.1)
        builder = DeribitSurfaceBuilder(cfg)
        expiry = dt.date(2025, 1, 10)
        db = StubDB({expiry: _rows_for_expiry(expiry)})
        ts = dt.datetime(2025, 1, 1, 12, 0, tzinfo=dt.timezone.utc).timestamp()
        first = await builder.run_once(db, ts_now=ts)
        second = await builder.run_once(db, ts_now=ts)
        assert first == 3
        assert second == 0

    import asyncio

    asyncio.run(_run())
