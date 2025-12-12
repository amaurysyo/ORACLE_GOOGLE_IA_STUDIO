"""Binance Futures orderbook audit pipeline backed by unicorn local depth cache."""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import json
from dataclasses import dataclass
from typing import Any, Callable, Iterable, List, Optional, Protocol

from loguru import logger

from ..config_hot import ConfigManager
from ..db import DB
from .batch_writer import AsyncBatcher

try:  # pragma: no cover - optional dependency
    from unicorn_binance_local_depth_cache import BinanceLocalDepthCacheManager  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    BinanceLocalDepthCacheManager = None


@dataclass
class AuditOrderbookSettings:
    enabled: bool = False
    symbol: str = "BTCUSDT"
    instrument_id: str = "BINANCE:PERP:BTCUSDT"
    stream_speed: str = "100ms"
    max_levels_per_side: int = 500
    snapshot_interval_ms: int = 250

    @classmethod
    def from_config(cls, cfg: Any) -> "AuditOrderbookSettings":
        raw = getattr(cfg, "audit_orderbook", {}) or {}
        return cls(
            enabled=bool(raw.get("enabled", False)),
            symbol=str(raw.get("symbol", "BTCUSDT")),
            instrument_id=str(raw.get("instrument_id", "BINANCE:PERP:BTCUSDT")),
            stream_speed=str(raw.get("stream_speed", "100ms")),
            max_levels_per_side=int(raw.get("max_levels_per_side", 500)),
            snapshot_interval_ms=int(raw.get("snapshot_interval_ms", 250)),
        )

    def requires_restart(self, other: "AuditOrderbookSettings") -> bool:
        return (
            self.symbol.lower() != other.symbol.lower()
            or self.stream_speed != other.stream_speed
            or self.max_levels_per_side != other.max_levels_per_side
        )


@dataclass
class DepthCacheView:
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]
    last_update_id: int | None
    event_time_ms: int | None


class DepthCache(Protocol):
    async def start(self) -> None:
        """Initialize underlying subscriptions/cache."""

    async def stop(self) -> None:
        """Release any resources held by the cache."""

    def snapshot(self) -> DepthCacheView:
        """Return the latest orderbook snapshot (already sorted)."""


class UnicornDepthCache:
    """Adapter around `unicorn_binance_local_depth_cache` for a single symbol."""

    def __init__(self, settings: AuditOrderbookSettings) -> None:
        if BinanceLocalDepthCacheManager is None:  # pragma: no cover - runtime guard
            raise RuntimeError("unicorn-binance-local-depth-cache not available")
        self._settings = settings
        self._mgr: Any | None = None

    async def start(self) -> None:  # pragma: no cover - exercised in integration
        def _start() -> Any:
            return BinanceLocalDepthCacheManager(
                exchange="binance.com-futures",
                symbols=[self._settings.symbol.lower()],
                depth_cache_size=max(self._settings.max_levels_per_side * 2, 20),
                websocket_depth_socket_type=self._settings.stream_speed or "",
            )

        self._mgr = await asyncio.to_thread(_start)
        logger.info(
            "[audit-ob] unicorn depth cache started symbol=%s speed=%s levels=%s",
            self._settings.symbol,
            self._settings.stream_speed,
            self._settings.max_levels_per_side,
        )

    async def stop(self) -> None:  # pragma: no cover - exercised in integration
        if self._mgr is None:
            return

        def _stop() -> None:
            with contextlib.suppress(Exception):
                self._mgr.stop_manager_with_all_streams()

        await asyncio.to_thread(_stop)
        self._mgr = None

    def snapshot(self) -> DepthCacheView:  # pragma: no cover - exercised in integration
        if self._mgr is None:
            raise RuntimeError("Depth cache not started")

        cache = self._mgr.get_depth_cache(symbol=self._settings.symbol.lower())
        bids = list(cache["bids"])
        asks = list(cache["asks"])
        bids.sort(key=lambda x: x[0], reverse=True)
        asks.sort(key=lambda x: x[0])
        last_update_id = int(cache.get("last_update_id") or 0)
        event_time_ms = int(cache.get("last_update_time") or 0)
        return DepthCacheView(bids=bids, asks=asks, last_update_id=last_update_id, event_time_ms=event_time_ms)


class AuditOrderbookRunner:
    def __init__(
        self,
        db: DB,
        batcher: AsyncBatcher,
        cfg_mgr: ConfigManager,
        depth_cache_factory: Optional[Callable[[AuditOrderbookSettings], DepthCache]] = None,
    ) -> None:
        self._db = db
        self._batcher = batcher
        self._cfg_mgr = cfg_mgr
        self._depth_cache_factory = depth_cache_factory or (lambda settings: UnicornDepthCache(settings))
        self._settings: Optional[AuditOrderbookSettings] = None
        self._cache: DepthCache | None = None
        self._task: Optional[asyncio.Task[None]] = None
        self._config_queue: "asyncio.Queue[AuditOrderbookSettings]" = asyncio.Queue()
        self._snapshot_interval_ms: int = 0

        self._batcher.register_stmt(
            "audit_orderbook_snapshot",
            """
            INSERT INTO oraculo_audit.orderbook_snapshots(
                instrument_id, event_time, last_update_id,
                best_bid, best_ask, spread_usd,
                bid_prices, bid_qtys, ask_prices, ask_qtys,
                meta, inserted_at
            ) VALUES (
                $1, to_timestamp($2/1000.0), $3,
                $4, $5, $6,
                $7, $8, $9, $10,
                $11::jsonb, now()
            ) ON CONFLICT DO NOTHING
            """,
        )

    async def run(self) -> None:
        await self._ensure_schema()
        self._cfg_mgr.subscribe(self._on_config)
        await self._on_config(self._cfg_mgr.cfg)
        while True:
            settings = await self._config_queue.get()
            await self._apply_settings(settings)

    async def _ensure_schema(self) -> None:
        await self._db.execute("CREATE SCHEMA IF NOT EXISTS oraculo_audit;")
        await self._db.execute(
            """
            CREATE TABLE IF NOT EXISTS oraculo_audit.orderbook_snapshots (
                instrument_id   instrument_id_t      NOT NULL,
                event_time      timestamptz          NOT NULL,
                last_update_id  bigint               NOT NULL,
                best_bid        double precision     NOT NULL,
                best_ask        double precision     NOT NULL,
                spread_usd      double precision     NOT NULL,
                bid_prices      double precision[]   NOT NULL,
                bid_qtys        double precision[]   NOT NULL,
                ask_prices      double precision[]   NOT NULL,
                ask_qtys        double precision[]   NOT NULL,
                meta            jsonb                NOT NULL DEFAULT '{}'::jsonb,
                inserted_at     timestamptz          NOT NULL DEFAULT now(),
                PRIMARY KEY (instrument_id, event_time)
            );
            """,
        )

    async def _on_config(self, cfg: Any) -> None:
        settings = AuditOrderbookSettings.from_config(cfg)
        await self._config_queue.put(settings)

    async def _apply_settings(self, new: AuditOrderbookSettings) -> None:
        prev = self._settings
        self._settings = new
        self._snapshot_interval_ms = new.snapshot_interval_ms

        if not new.enabled:
            await self._stop_snapshot_task()
            await self._stop_cache()
            logger.info("Audit orderbook disabled by config")
            return

        needs_restart = prev is None or new.requires_restart(prev) or self._task is None or self._cache is None
        if needs_restart:
            await self._stop_snapshot_task()
            await self._stop_cache()
            await self._start_cache(new)
            self._task = asyncio.create_task(self._snapshot_loop())
            logger.info(
                "Audit orderbook started for %s speed=%s levels=%s",
                new.symbol,
                new.stream_speed,
                new.max_levels_per_side,
            )
        else:
            logger.info(
                "Audit orderbook soft-update snapshot_interval_ms=%s",
                new.snapshot_interval_ms,
            )

    async def _start_cache(self, settings: AuditOrderbookSettings) -> None:
        cache = self._depth_cache_factory(settings)
        try:
            await cache.start()
        except Exception:
            logger.exception("Failed to start depth cache for %s", settings.symbol)
            raise
        self._cache = cache

    async def _stop_cache(self) -> None:
        if not self._cache:
            return
        with contextlib.suppress(Exception):
            await self._cache.stop()
        self._cache = None

    async def _stop_snapshot_task(self) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        self._task = None

    async def _snapshot_loop(self) -> None:
        assert self._settings is not None
        assert self._cache is not None
        settings = self._settings
        cache = self._cache

        while True:
            try:
                await asyncio.sleep(self._snapshot_interval_ms / 1000.0)
                view = cache.snapshot()
                await self._emit_snapshot(settings, view)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Audit orderbook snapshot error: {}", exc)
                await asyncio.sleep(1.0)

    async def _emit_snapshot(self, settings: AuditOrderbookSettings, view: DepthCacheView) -> None:
        if not view.bids or not view.asks:
            return
        bid_prices, bid_qtys, ask_prices, ask_qtys = _to_snapshot_arrays(
            view.bids, view.asks, settings.max_levels_per_side
        )
        best_bid = bid_prices[0]
        best_ask = ask_prices[0]
        if best_bid == 0.0 or best_ask == 0.0:
            return
        spread = best_ask - best_bid
        evt_time_ms = view.event_time_ms or int(dt.datetime.now(tz=dt.timezone.utc).timestamp() * 1000)
        last_update_id = view.last_update_id or 0
        row = (
            settings.instrument_id,
            evt_time_ms,
            last_update_id,
            best_bid,
            best_ask,
            spread,
            bid_prices,
            bid_qtys,
            ask_prices,
            ask_qtys,
            json.dumps({}),
        )
        self._batcher.add("audit_orderbook_snapshot", row)
        await self._batcher.flush_if_needed()


def _pad_side(levels: Iterable[tuple[float, float]], max_levels: int, reverse: bool) -> tuple[List[float], List[float]]:
    levels_list = list(levels)
    prices = [p for p, _ in sorted(levels_list, key=lambda x: x[0], reverse=reverse)[:max_levels]]
    qtys = []
    for p in prices:
        for price, qty in levels_list:
            if price == p:
                qtys.append(qty)
                break
    while len(prices) < max_levels:
        prices.append(0.0)
        qtys.append(0.0)
    return prices, qtys


def _to_snapshot_arrays(
    bids: Iterable[tuple[float, float]], asks: Iterable[tuple[float, float]], max_levels: int
) -> tuple[List[float], List[float], List[float], List[float]]:
    bid_prices, bid_qtys = _pad_side(bids, max_levels, True)
    ask_prices, ask_qtys = _pad_side(asks, max_levels, False)
    return bid_prices, bid_qtys, ask_prices, ask_qtys


__all__ = [
    "AuditOrderbookRunner",
    "AuditOrderbookSettings",
    "DepthCacheView",
    "DepthCache",
    "UnicornDepthCache",
]
