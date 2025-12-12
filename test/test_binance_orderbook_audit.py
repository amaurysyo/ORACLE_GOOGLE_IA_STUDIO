import asyncio
import sys
import types

import pytest

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
    DepthCache,
    DepthCacheView,
)


class _FakeDB:
    async def execute(self, *_args, **_kwargs):
        return None

    async def execute_many(self, *_args, **_kwargs):
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


class _FakeDepthCache(DepthCache):
    def __init__(self, view: DepthCacheView) -> None:
        self.view = view
        self.started = False
        self.stopped = False

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    def snapshot(self) -> DepthCacheView:
        return self.view


class _FactoryCounter:
    def __init__(self, view: DepthCacheView):
        self.view = view
        self.created: list[_FakeDepthCache] = []

    def __call__(self, _settings: AuditOrderbookSettings) -> _FakeDepthCache:
        cache = _FakeDepthCache(self.view)
        self.created.append(cache)
        return cache


def test_snapshot_arrays_and_row_written():
    async def _run() -> None:
        view = DepthCacheView(
            bids=[(100.0, 1.2), (99.5, 0.5)],
            asks=[(100.5, 1.5)],
            last_update_id=1234,
            event_time_ms=1_700_000_000_000,
        )
        factory = _FactoryCounter(view)
        batcher = _FakeBatcher()
        runner = AuditOrderbookRunner(
            db=_FakeDB(),
            batcher=batcher,
            cfg_mgr=_FakeCfgMgr(AuditOrderbookSettings(enabled=True)),
            depth_cache_factory=factory,
        )

        settings = AuditOrderbookSettings(
            enabled=True,
            max_levels_per_side=3,
            snapshot_interval_ms=10,
            instrument_id="BINANCE:PERP:BTCUSDT",
        )

        await runner._apply_settings(settings)
        await asyncio.sleep(0.05)
        await runner._stop_snapshot_task()
        await runner._stop_cache()

        assert factory.created, "depth cache factory was invoked"
        assert factory.created[0].started is True
        assert batcher.rows, "snapshot rows should be enqueued"
        key, row = batcher.rows[0]
        assert key == "audit_orderbook_snapshot"
        (
            instrument_id,
            event_time_ms,
            last_update_id,
            best_bid,
            best_ask,
            spread,
            bid_prices,
            bid_qtys,
            ask_prices,
            ask_qtys,
            _meta,
        ) = row
        assert instrument_id == "BINANCE:PERP:BTCUSDT"
        assert event_time_ms == 1_700_000_000_000
        assert last_update_id == 1234
        assert best_bid == 100.0
        assert best_ask == 100.5
        assert spread == 0.5
        assert bid_prices == [100.0, 99.5, 0.0]
        assert bid_qtys == [1.2, 0.5, 0.0]
        assert ask_prices == [100.5, 0.0, 0.0]
        assert ask_qtys == [1.5, 0.0, 0.0]

    asyncio.run(_run())


def test_hot_reload_recreates_depth_cache():
    async def _run() -> None:
        view = DepthCacheView(bids=[(1.0, 1.0)], asks=[(2.0, 2.0)], last_update_id=1, event_time_ms=None)
        factory = _FactoryCounter(view)
        batcher = _FakeBatcher()
        runner = AuditOrderbookRunner(
            db=_FakeDB(), batcher=batcher, cfg_mgr=_FakeCfgMgr(AuditOrderbookSettings(enabled=True)), depth_cache_factory=factory
        )

        cfg1 = AuditOrderbookSettings(enabled=True, stream_speed="100ms", snapshot_interval_ms=5)
        cfg2 = AuditOrderbookSettings(enabled=True, stream_speed="500ms", snapshot_interval_ms=5)

        await runner._apply_settings(cfg1)
        await asyncio.sleep(0.02)
        await runner._apply_settings(cfg2)
        await asyncio.sleep(0.02)

        await runner._stop_snapshot_task()
        await runner._stop_cache()

        assert len(factory.created) == 2
        assert factory.created[0].stopped is True or factory.created[0].started is True
        assert factory.created[1].started is True

    asyncio.run(_run())


def test_disable_stops_snapshot_loop():
    async def _run() -> None:
        view = DepthCacheView(bids=[(1.0, 1.0)], asks=[(2.0, 2.0)], last_update_id=1, event_time_ms=None)
        factory = _FactoryCounter(view)
        batcher = _FakeBatcher()
        runner = AuditOrderbookRunner(
            db=_FakeDB(), batcher=batcher, cfg_mgr=_FakeCfgMgr(AuditOrderbookSettings(enabled=True)), depth_cache_factory=factory
        )

        cfg_enabled = AuditOrderbookSettings(enabled=True, snapshot_interval_ms=5)
        cfg_disabled = AuditOrderbookSettings(enabled=False)

        await runner._apply_settings(cfg_enabled)
        await asyncio.sleep(0.02)
        await runner._apply_settings(cfg_disabled)
        await asyncio.sleep(0.02)

        assert runner._task is None
        assert runner._cache is None
        assert factory.created[0].stopped is True

    asyncio.run(_run())
