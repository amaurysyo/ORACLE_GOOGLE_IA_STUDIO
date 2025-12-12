import asyncio
import sys
import types

import pytest

if "aiohttp" not in sys.modules:
    sys.modules["aiohttp"] = types.SimpleNamespace(ClientSession=object, ClientTimeout=types.SimpleNamespace)
if "websockets" not in sys.modules:
    async def _dummy_connect(*_args, **_kwargs):
        raise RuntimeError("websocket stub should not be used in tests")

    sys.modules["websockets"] = types.SimpleNamespace(connect=_dummy_connect)
if "loguru" not in sys.modules:
    class _DummyLogger:
        def __getattr__(self, _name):
            return lambda *args, **kwargs: None

    sys.modules["loguru"] = types.SimpleNamespace(logger=_DummyLogger())
if "yaml" not in sys.modules:
    sys.modules["yaml"] = types.SimpleNamespace(safe_load=lambda *_args, **_kwargs: {})
if "dotenv" not in sys.modules:
    sys.modules["dotenv"] = types.SimpleNamespace(load_dotenv=lambda *_args, **_kwargs: None)
if "watchfiles" not in sys.modules:
    async def _dummy_awatch(*_args, **_kwargs):
        if False:
            yield None

    sys.modules["watchfiles"] = types.SimpleNamespace(awatch=_dummy_awatch)
if "pydantic" not in sys.modules:
    class _DummyBase:
        def __init__(self, *args, **kwargs):
            pass

        @classmethod
        def model_validate(cls, data):
            obj = cls.__new__(cls)
            obj.__dict__.update(data or {})
            return obj

    sys.modules["pydantic"] = types.SimpleNamespace(BaseModel=_DummyBase, ConfigDict=dict)
if "asyncpg" not in sys.modules:
    class _DummyPool:
        async def close(self):
            return None

    async def _create_pool(*_args, **_kwargs):
        return _DummyPool()

    sys.modules["asyncpg"] = types.SimpleNamespace(create_pool=_create_pool, Pool=_DummyPool, Record=dict)
if "prometheus_client" not in sys.modules:
    class _DummyMetric:
        def labels(self, *_args, **_kwargs):
            return self

        def inc(self, *_args, **_kwargs):
            return None

        def set(self, *_args, **_kwargs):
            return None

        def observe(self, *_args, **_kwargs):
            return None

    sys.modules["prometheus_client"] = types.SimpleNamespace(
        Counter=lambda *_args, **_kwargs: _DummyMetric(),
        Gauge=lambda *_args, **_kwargs: _DummyMetric(),
        Summary=lambda *_args, **_kwargs: _DummyMetric(),
        Histogram=lambda *_args, **_kwargs: _DummyMetric(),
        start_http_server=lambda *_args, **_kwargs: None,
    )

from oraculo.ingest.binance_orderbook_audit import (
    AuditOrderbookRunner,
    AuditOrderbookSettings,
    LocalOrderBook,
)


class _FakeDB:
    async def execute(self, *_args, **_kwargs):
        return None


class _FakeBatcher:
    def __init__(self) -> None:
        self.stmts = {}
        self.rows = []

    def register_stmt(self, key: str, sql: str) -> None:
        self.stmts[key] = sql

    def add(self, key: str, row: tuple) -> None:
        self.rows.append((key, row))

    async def flush_if_needed(self) -> None:
        return None


class _FakeCfgMgr:
    def __init__(self, cfg: object):
        self.cfg = cfg

    def subscribe(self, _cb):
        return None


def _run_async(coro):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()
        asyncio.set_event_loop(None)


def test_local_orderbook_rebuilds_from_diffs():
    book = LocalOrderBook(max_levels_per_side=2)
    book.apply_snapshot(100, [["100.0", "1"], ["99.5", "2"]], [["100.5", "1.5"], ["101.0", "3"]])

    event = {
        "u": 101,
        "E": 1699990000000,
        "pu": 100,
        "b": [["100.0", "1.2"], ["99.5", "0"]],
        "a": [["100.5", "0"], ["100.7", "1"]],
    }
    book.apply_diff(event)

    best_bid, best_ask = book.best_bid_ask()
    assert best_bid == 100.0
    assert best_ask == 100.7
    bid_prices, bid_qtys, ask_prices, ask_qtys = book.to_snapshot_arrays()
    assert bid_prices == [100.0, 0.0]
    assert bid_qtys[0] == 1.2
    assert ask_prices[0] == 100.7
    assert ask_qtys[0] == 1.0


def test_snapshot_arrays_are_padded():
    book = LocalOrderBook(max_levels_per_side=3)
    book.apply_snapshot(10, [[100, 1]], [[101, 2]])
    bid_prices, bid_qtys, ask_prices, ask_qtys = book.to_snapshot_arrays()
    assert bid_prices == [100.0, 0.0, 0.0]
    assert bid_qtys == [1.0, 0.0, 0.0]
    assert ask_prices == [101.0, 0.0, 0.0]
    assert ask_qtys == [2.0, 0.0, 0.0]


def test_config_change_triggers_resubscribe():
    async def _run() -> None:
        starts: list[AuditOrderbookSettings] = []

        async def fake_stream(settings: AuditOrderbookSettings, _book: LocalOrderBook) -> None:
            starts.append(settings)
            try:
                while True:
                    await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                raise

        cfg1 = AuditOrderbookSettings(enabled=True, stream_speed="100ms")
        cfg2 = AuditOrderbookSettings(enabled=True, stream_speed="500ms")
        runner = AuditOrderbookRunner(
            db=_FakeDB(), batcher=_FakeBatcher(), cfg_mgr=_FakeCfgMgr(cfg1), stream_factory=fake_stream
        )

        await runner._apply_settings(cfg1)
        await asyncio.sleep(0.02)
        assert len(starts) == 1

        await runner._apply_settings(cfg2)
        await asyncio.sleep(0.02)
        assert len(starts) == 2

        if runner._task:
            runner._task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await runner._task

    _run_async(_run())

