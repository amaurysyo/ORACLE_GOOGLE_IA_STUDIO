import datetime as dt

from oraculo.deribit.surface_builder import DeribitSurfaceBuilder, SurfaceBuilderCfg


class StubDB:
    def __init__(self, rows_snapshot):
        self.rows_snapshot = rows_snapshot
        self.executed = []

    async def fetch(self, sql, *args):  # noqa: ANN001
        sql_l = sql.lower()
        if "options_ticker" in sql_l and "join deribit.options_instruments" in sql_l:
            return self.rows_snapshot
        if "from deribit.options_instruments" in sql_l:
            return [{"expiry": dt.date(2025, 1, 10)}]
        if "underlying_price" in sql_l:
            return [{"underlying_price": 20000.0}]
        raise AssertionError(f"Unexpected query: {sql_l}")

    async def execute_many(self, sql, rows):  # noqa: ANN001
        self.executed.extend(rows)


def _sample_rows():
    base_ts = dt.datetime(2025, 1, 1, 12, 0, tzinfo=dt.timezone.utc)
    return [
        {
            "instrument_id": "DERIBIT:OPTIONS:BTC-10JAN25-19500-C",
            "event_time": base_ts,
            "mark_iv": 0.60,
            "delta": 0.24,
            "open_interest": 10,
            "bid": 1.0,
            "ask": 1.1,
            "underlying_price": 20000.0,
            "strike": 19500,
            "option_type": "C",
            "expiry": dt.date(2025, 1, 10),
        },
        {
            "instrument_id": "DERIBIT:OPTIONS:BTC-10JAN25-19500-P",
            "event_time": base_ts,
            "mark_iv": 0.70,
            "delta": -0.26,
            "open_interest": 20,
            "bid": 1.2,
            "ask": 1.3,
            "underlying_price": 20000.0,
            "strike": 19500,
            "option_type": "P",
            "expiry": dt.date(2025, 1, 10),
        },
        {
            "instrument_id": "DERIBIT:OPTIONS:BTC-10JAN25-20000-C",
            "event_time": base_ts,
            "mark_iv": 0.55,
            "delta": 0.49,
            "open_interest": 30,
            "bid": 1.4,
            "ask": 1.6,
            "underlying_price": 20000.0,
            "strike": 20000,
            "option_type": "C",
            "expiry": dt.date(2025, 1, 10),
        },
    ]


def test_surface_builder_inserts_rr_bf_and_moneyness():
    async def _run():
        cfg = SurfaceBuilderCfg(enabled=True, lag_s=0, poll_s=0.1)
        builder = DeribitSurfaceBuilder(cfg)
        db = StubDB(_sample_rows())
        rows = await builder.run_once(
            db, ts_now=dt.datetime(2025, 1, 1, 12, 0, tzinfo=dt.timezone.utc).timestamp()
        )

        assert rows == 3
        assert len(db.executed) == 3
        tenor_rows = [r for r in db.executed if r[3] == "NA"]
        assert tenor_rows, "missing tenor row"
        tenor_row = tenor_rows[0]
        assert abs(tenor_row[5] - (-0.10)) < 1e-6  # rr_25d = 0.60 - 0.70
        assert abs(tenor_row[6] - 0.10) < 1e-6  # bf_25d = 0.5*(0.60+0.70)-0.55

        m_rows = [r for r in db.executed if r[3] != "NA"]
        buckets = {r[3]: r for r in m_rows}
        assert "0.95_1.00" in buckets and "1.00_1.05" in buckets
        assert abs(buckets["0.95_1.00"][4] - 0.6666667) < 1e-6  # OI weighted avg of 0.60 (10) and 0.70 (20)
        assert abs(buckets["1.00_1.05"][4] - 0.55) < 1e-6

    import asyncio

    asyncio.run(_run())


def test_surface_builder_idempotent_same_bucket():
    async def _run():
        cfg = SurfaceBuilderCfg(enabled=True, lag_s=0, poll_s=0.1)
        builder = DeribitSurfaceBuilder(cfg)
        db = StubDB(_sample_rows())
        ts = dt.datetime(2025, 1, 1, 12, 0, tzinfo=dt.timezone.utc).timestamp()
        first = await builder.run_once(db, ts_now=ts)
        second = await builder.run_once(db, ts_now=ts)
        assert first == 3
        assert second == 0

    import asyncio

    asyncio.run(_run())
