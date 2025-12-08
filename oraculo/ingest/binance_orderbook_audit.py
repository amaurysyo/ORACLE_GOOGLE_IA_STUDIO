"""Binance Futures orderbook audit pipeline with hot-reload support."""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import json
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional

import aiohttp
import websockets
from loguru import logger

from ..config_hot import ConfigManager
from ..db import DB
from .batch_writer import AsyncBatcher


REST_BASE = "https://fapi.binance.com"
WS_BASE = "wss://fstream.binance.com/stream"


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


class LocalOrderBook:
    """In-memory orderbook that applies Binance depth diffs."""

    def __init__(self, max_levels_per_side: int) -> None:
        self.max_levels_per_side = max_levels_per_side
        self.last_update_id: int = 0
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.last_event_time_ms: int = 0

    def _trim(self) -> None:
        def _trim_side(side: Dict[float, float]) -> None:
            if len(side) <= self.max_levels_per_side:
                return
            prices = sorted(side.keys(), reverse=True)
            if side is self.asks:
                prices = list(reversed(prices))
            for p in prices[self.max_levels_per_side :]:
                side.pop(p, None)

        _trim_side(self.bids)
        _trim_side(self.asks)

    def apply_snapshot(self, last_update_id: int, bids: Iterable[Iterable[str | float]], asks: Iterable[Iterable[str | float]]) -> None:
        self.last_update_id = last_update_id
        self.bids = {float(p): float(q) for p, q in bids}
        self.asks = {float(p): float(q) for p, q in asks}
        self._trim()

    def apply_diff(self, event: Dict[str, Any]) -> None:
        self.last_update_id = int(event["u"])
        self.last_event_time_ms = int(event.get("E") or 0)
        for price, qty in event.get("b", []):
            p = float(price)
            q = float(qty)
            if q == 0:
                self.bids.pop(p, None)
            else:
                self.bids[p] = q
        for price, qty in event.get("a", []):
            p = float(price)
            q = float(qty)
            if q == 0:
                self.asks.pop(p, None)
            else:
                self.asks[p] = q
        self._trim()

    def best_bid_ask(self) -> tuple[float, float]:
        bids_sorted = sorted(self.bids.keys(), reverse=True)
        asks_sorted = sorted(self.asks.keys())
        if not bids_sorted or not asks_sorted:
            raise ValueError("Orderbook missing best bid/ask")
        return bids_sorted[0], asks_sorted[0]

    def to_snapshot_arrays(self) -> tuple[List[float], List[float], List[float], List[float]]:
        def _pad(side: Dict[float, float], reverse: bool) -> tuple[List[float], List[float]]:
            prices = sorted(side.keys(), reverse=reverse)
            prices = prices[: self.max_levels_per_side]
            qtys = [side[p] for p in prices]
            # rellenamos con ceros
            while len(prices) < self.max_levels_per_side:
                prices.append(0.0)
                qtys.append(0.0)
            return prices, qtys

        bid_prices, bid_qtys = _pad(self.bids, True)
        ask_prices, ask_qtys = _pad(self.asks, False)
        return bid_prices, bid_qtys, ask_prices, ask_qtys


class AuditOrderbookRunner:
    def __init__(
        self,
        db: DB,
        batcher: AsyncBatcher,
        cfg_mgr: ConfigManager,
        stream_factory: Optional[Callable[[AuditOrderbookSettings, LocalOrderBook], asyncio.Future]] = None,
    ) -> None:
        self._db = db
        self._batcher = batcher
        self._cfg_mgr = cfg_mgr
        self._stream_factory = stream_factory
        self._settings: Optional[AuditOrderbookSettings] = None
        self._book: Optional[LocalOrderBook] = None
        self._task: Optional[asyncio.Task] = None
        self._config_queue: "asyncio.Queue[AuditOrderbookSettings]" = asyncio.Queue()
        self._snapshot_interval_ms: int = 0

        self._batcher.register_stmt(
            "audit_orderbook_snapshot",
            """
            INSERT INTO oraculo_audit.orderbook_snapshots(
                instrument_id, event_time, last_update_id,
                best_bid, best_ask, spread_usd,
                bid_prices, bid_qtys, ask_prices, ask_qtys,
                meta
            ) VALUES (
                $1, to_timestamp($2/1000.0), $3,
                $4, $5, $6,
                $7, $8, $9, $10,
                $11::jsonb
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
                PRIMARY KEY (instrument_id, event_time)
            );
            """
        )

    async def _on_config(self, cfg: Any) -> None:
        settings = AuditOrderbookSettings.from_config(cfg)
        await self._config_queue.put(settings)

    async def _apply_settings(self, new: AuditOrderbookSettings) -> None:
        prev = self._settings
        self._settings = new
        self._snapshot_interval_ms = new.snapshot_interval_ms

        if not new.enabled:
            if self._task:
                self._task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._task
            self._task = None
            logger.info("Audit orderbook disabled by config")
            return

        if prev is None or new.requires_restart(prev) or self._task is None:
            if self._task:
                self._task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._task
            self._book = LocalOrderBook(new.max_levels_per_side)
            self._task = asyncio.create_task(self._stream_loop(new, self._book))
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

    async def _stream_loop(self, settings: AuditOrderbookSettings, book: LocalOrderBook) -> None:
        if self._stream_factory:
            await self._stream_factory(settings, book)
            return
        while True:
            try:
                await self._run_once(settings, book)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("Audit orderbook stream error: %s", exc)
                await asyncio.sleep(1.0)

    async def _run_once(self, settings: AuditOrderbookSettings, book: LocalOrderBook) -> None:
        channel = f"{settings.symbol.lower()}@depth"
        if settings.stream_speed:
            channel = f"{channel}@{settings.stream_speed}"
        url = f"{WS_BASE}?streams={channel}"
        limit = max(5, settings.max_levels_per_side * 2)

        async with aiohttp.ClientSession() as http:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                buffer: list[Dict[str, Any]] = []
                queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()

                async def _reader() -> None:
                    async for raw in ws:
                        msg = json.loads(raw)
                        evt = msg.get("data", msg)
                        if evt.get("e") != "depthUpdate":
                            continue
                        await queue.put(evt)

                reader_task = asyncio.create_task(_reader())
                try:
                    # 1) snapshot REST
                    async with http.get(
                        f"{REST_BASE}/fapi/v1/depth",
                        params={"symbol": settings.symbol.upper(), "limit": limit},
                        timeout=aiohttp.ClientTimeout(total=5),
                    ) as r:
                        r.raise_for_status()
                        snap = await r.json()
                    book.apply_snapshot(int(snap["lastUpdateId"]), snap.get("bids", []), snap.get("asks", []))

                    # 2) vaciar buffer inicial
                    while not queue.empty():
                        buffer.append(await queue.get())
                    buffer.sort(key=lambda e: e.get("u", 0))
                    # 3) descartar viejos
                    buffer = [e for e in buffer if int(e.get("u", 0)) >= book.last_update_id]
                    # 4) procesar primer evento válido
                    for evt in list(buffer):
                        U = int(evt.get("U", 0))
                        u = int(evt.get("u", 0))
                        if U <= book.last_update_id <= u:
                            book.apply_diff(evt)
                            buffer = [e for e in buffer if int(e.get("u", 0)) > u]
                            break
                    # 5) resto de eventos en buffer
                    for evt in buffer:
                        await self._apply_or_resync(evt, book)

                    # 6) bucle principal
                    next_snapshot = asyncio.get_event_loop().time() + self._snapshot_interval_ms / 1000.0
                    while True:
                        timeout = max(0.0, next_snapshot - asyncio.get_event_loop().time())
                        try:
                            evt = await asyncio.wait_for(queue.get(), timeout=timeout)
                            await self._apply_or_resync(evt, book)
                        except asyncio.TimeoutError:
                            await self._emit_snapshot(settings, book)
                            next_snapshot = asyncio.get_event_loop().time() + self._snapshot_interval_ms / 1000.0
                finally:
                    reader_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await reader_task

    async def _apply_or_resync(self, event: Dict[str, Any], book: LocalOrderBook) -> None:
        pu = int(event.get("pu", 0))
        if book.last_update_id and pu != book.last_update_id:
            raise RuntimeError(f"Gap detected pu={pu} last={book.last_update_id}")
        book.apply_diff(event)

    async def _emit_snapshot(self, settings: AuditOrderbookSettings, book: LocalOrderBook) -> None:
        if not book.bids or not book.asks:
            return
        best_bid, best_ask = book.best_bid_ask()
        bid_prices, bid_qtys, ask_prices, ask_qtys = book.to_snapshot_arrays()
        spread = best_ask - best_bid
        evt_time_ms = book.last_event_time_ms or int(dt.datetime.now(tz=dt.timezone.utc).timestamp() * 1000)
        row = (
            settings.instrument_id,
            evt_time_ms,
            book.last_update_id,
            best_bid,
            best_ask,
            spread,
            bid_prices,
            bid_qtys,
            ask_prices,
            ask_qtys,
            {},
        )
        self._batcher.add("audit_orderbook_snapshot", row)
        await self._batcher.flush_if_needed()


__all__ = [
    "AuditOrderbookRunner",
    "AuditOrderbookSettings",
    "LocalOrderBook",
]

