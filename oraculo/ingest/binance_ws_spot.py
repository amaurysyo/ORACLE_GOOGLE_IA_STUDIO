from __future__ import annotations

import asyncio
import datetime as dt
import json
from typing import Any, Optional

import aiohttp
from loguru import logger

from ..obs.metrics import ws_msgs_total, ws_resync_total, ws_heartbeat_miss_total,ws_heartbeat_miss_total, ws_last_msg_age_s
from ..db import DB
from .batch_writer import AsyncBatcher

try:
    from unicorn_binance_websocket_api import BinanceWebSocketApiManager  # type: ignore
except Exception:
    try:
        from unicorn_binance_websocket_api.manager import BinanceWebSocketApiManager  # type: ignore
    except Exception:
        from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import (  # type: ignore
            BinanceWebSocketApiManager,
        )

BINANCE_SPOT_INST = "BINANCE:SPOT:BTCUSDT"
API_BASE = "https://api.binance.com"


def _ms_to_ts(ms: int) -> dt.datetime:
    return dt.datetime.fromtimestamp(ms / 1000.0, tz=dt.timezone.utc)


def _normalize(raw: str | dict[str, Any]) -> dict[str, Any]:
    if isinstance(raw, str):
        try:
            obj = json.loads(raw)
        except Exception:
            return {}
    else:
        obj = raw
    return obj.get("data", obj)


class SpotWSRunner:
    """SPOT WS con recreación dinámica, gap-detect y heartbeat configurable."""

    def __init__(
        self,
        db: DB,
        batcher: AsyncBatcher,
        symbol: str,
        depth_levels: int,
        depth_ms: int,
        use_agg_trade: bool,
        changes_queue: asyncio.Queue[dict] | None = None,
        hb_ref: dict[str, float] | None = None,
    ) -> None:
        self._db = db
        self._batcher = batcher
        self._symbol = symbol
        self._depth_levels = depth_levels
        self._depth_ms = depth_ms
        self._use_agg_trade = use_agg_trade
        self._changes = changes_queue or asyncio.Queue()
        self._hb = hb_ref or {"trade": 20.0, "depth": 5.0}
        self._mgr: Optional[BinanceWebSocketApiManager] = None
        self._sid_trade: Optional[str] = None
        self._sid_depth: Optional[str] = None
        self._last_msg_ts: dict[str, float] = {"trade": 0.0, "depth": 0.0}
        self._last_depth_u: int = 0

    async def run(self) -> None:
        await self._create_streams()
        session: Optional[aiohttp.ClientSession] = None
        try:
            session = aiohttp.ClientSession(headers={"User-Agent": "Oraculo/1.0"})
            while True:
                await self._apply_pending_changes_nonblocking()
                await self._check_heartbeats()
                raw = self._mgr.pop_stream_data_from_stream_buffer() if self._mgr else None
                if raw is None:
                    await asyncio.sleep(0.01)
                    continue

                p = _normalize(raw)
                if not p or "e" not in p:
                    continue

                et = p["e"]
                ws_msgs_total.labels(et).inc()
                now = asyncio.get_event_loop().time()
                key = "depth" if et == "depthUpdate" else "trade"
                self._last_msg_ts[key] = now

                if et == "aggTrade":
                    await self._handle_agg_trade(p)
                elif et == "trade":
                    await self._handle_trade(p)
                elif et == "depthUpdate":
                    pu = int(p.get("pu") or 0)
                    if self._last_depth_u and pu != self._last_depth_u:
                        logger.warning(f"[spot-depth] gap detected: pu={pu} last={self._last_depth_u} -> resync")
                        ws_resync_total.labels("spot", "depth").inc()
                        await self._resync_depth(session)
                        continue
                    await self._handle_depth(p)

                await self._batcher.flush_if_needed()
        finally:
            if session:
                await session.close()
            await self._stop_all()

    async def request_reconfigure(
        self,
        *,
        symbol: str | None = None,
        depth_levels: int | None = None,
        depth_ms: int | None = None,
        use_agg_trade: bool | None = None,
    ) -> None:
        await self._changes.put(
            {"symbol": symbol, "depth_levels": depth_levels, "depth_ms": depth_ms, "use_agg_trade": use_agg_trade}
        )

    async def _apply_pending_changes_nonblocking(self) -> None:
        changed = False
        new = {
            "symbol": self._symbol,
            "depth_levels": self._depth_levels,
            "depth_ms": self._depth_ms,
            "use_agg_trade": self._use_agg_trade,
        }
        while not self._changes.empty():
            item = await self._changes.get()
            for k in ("symbol", "depth_levels", "depth_ms", "use_agg_trade"):
                if item.get(k) is not None and item[k] != new[k]:
                    new[k] = item[k]
                    changed = True
        if changed:
            logger.info(f"[spot-ws] Reconfig: {new}")
            self._symbol = str(new["symbol"])
            self._depth_levels = int(new["depth_levels"])
            self._depth_ms = int(new["depth_ms"])
            self._use_agg_trade = bool(new["use_agg_trade"])
            await self._recreate_streams()

    async def _create_streams(self) -> None:
        self._mgr = BinanceWebSocketApiManager(exchange="binance.com")
        depth_channel = f"depth{self._depth_levels}@{self._depth_ms}ms"
        trade_channel = "aggTrade" if self._use_agg_trade else "trade"
        self._sid_trade = self._mgr.create_stream([trade_channel], [self._symbol])
        self._sid_depth = self._mgr.create_stream([depth_channel], [self._symbol])
        self._last_depth_u = 0
        logger.info(f"[spot-ws] Streams up: {trade_channel}, {depth_channel}")

    async def _recreate_streams(self) -> None:
        await self._stop_streams()
        await self._create_streams()

    async def _stop_streams(self) -> None:
        if not self._mgr:
            return
        for sid in (self._sid_trade, self._sid_depth):
            if not sid:
                continue
            try:
                self._mgr.stop_stream(sid)
            except Exception:
                pass
        self._sid_trade = None
        self._sid_depth = None

    async def _stop_all(self) -> None:
        if self._mgr:
            try:
                self._mgr.stop_manager_with_all_streams()
            except Exception:
                pass
            self._mgr = None

    async def _resub(self, stream: str) -> None:
        if not self._mgr:
            return
        try:
            if stream == "trade":
                if self._sid_trade:
                    self._mgr.stop_stream(self._sid_trade)
                trade_channel = "aggTrade" if self._use_agg_trade else "trade"
                self._sid_trade = self._mgr.create_stream([trade_channel], [self._symbol])
            elif stream == "depth":
                if self._sid_depth:
                    self._mgr.stop_stream(self._sid_depth)
                depth_channel = f"depth{self._depth_levels}@{self._depth_ms}ms"
                self._sid_depth = self._mgr.create_stream([depth_channel], [self._symbol])
            ws_resync_total.labels("spot", stream).inc()
            logger.info(f"[spot-ws] resubscribed stream={stream}")
        except Exception as e:
            logger.warning(f"[spot-ws] resub {stream} fallo {type(e).__name__}: {e!s}")

    async def _resync_depth(self, session: aiohttp.ClientSession) -> None:
        await self._resub("depth")
        try:
            async with session.get(f"{API_BASE}/api/v3/depth", params={"symbol": self._symbol.upper(), "limit": 1000}) as r:
                r.raise_for_status()
                data = await r.json()
                self._last_depth_u = int(data.get("lastUpdateId") or 0)
                logger.info(f"[spot-depth] resynced snapshot lastUpdateId={self._last_depth_u}")
        except Exception as e:
            logger.warning(f"[spot-depth] snapshot failed {type(e).__name__}: {e!s}")

    async def _check_heartbeats(self) -> None:
        now = asyncio.get_event_loop().time()
        any_last = max(self._last_msg_ts.values() or [0.0])
        for stream, ts in list(self._last_msg_ts.items()):
            if ts == 0.0:
                continue
            age = now - ts
            ws_last_msg_age_s.labels("binance_spot", stream).set(age)
            th = float(self._hb.get(stream, 10.0))
            eff = th * (2.0 if (now - any_last) < 3.0 else 1.0)
            if age > eff:
                ws_heartbeat_miss_total.labels("spot", stream).inc()
                logger.warning(f"[spot-ws] heartbeat miss stream={stream} idle={now-ts:.1f}s thr={eff:.1f}s -> resub")
                await self._resub(stream)
                self._last_msg_ts[stream] = now

    # --- Handlers ---
    async def _handle_agg_trade(self, p: dict[str, Any]) -> None:
        event_time = _ms_to_ts(p["E"])
        trade_id_ext = int(p["a"])
        price = float(p["p"])
        qty = float(p["q"])
        side = "sell" if p.get("m", False) else "buy"
        is_best = bool(p.get("M", False))
        buyer_oid = None
        seller_oid = None
        meta = json.dumps({"first_id": p.get("f"), "last_id": p.get("l")})
        row = (BINANCE_SPOT_INST, event_time, trade_id_ext, price, qty, side, is_best, buyer_oid, seller_oid, meta)
        self._batcher.add("bspot_trades", row)

    async def _handle_trade(self, p: dict[str, Any]) -> None:
        event_time = _ms_to_ts(p["E"])
        trade_id_ext = int(p["t"])
        price = float(p["p"])
        qty = float(p["q"])
        side = "sell" if p.get("m", False) else "buy"
        is_best = False
        buyer_oid = int(p["b"]) if p.get("b") is not None else None
        seller_oid = int(p["a"]) if p.get("a") is not None else None
        meta = json.dumps({})
        row = (BINANCE_SPOT_INST, event_time, trade_id_ext, price, qty, side, is_best, buyer_oid, seller_oid, meta)
        self._batcher.add("bspot_trades", row)

    async def _handle_depth(self, p: dict[str, Any]) -> None:
        event_time = _ms_to_ts(p["E"])
        seq = int(p.get("u", 0))
        meta_common = json.dumps({"U": p.get("U"), "u": p.get("u"), "pu": p.get("pu")})
        for price_str, qty_str in p.get("b", []):
            price = float(price_str)
            qty = float(qty_str)
            action = "delete" if qty == 0.0 else "update"
            row = (BINANCE_SPOT_INST, event_time, seq, "buy", action, price, qty, meta_common)
            self._batcher.add("bspot_depth", row)
        for price_str, qty_str in p.get("a", []):
            price = float(price_str)
            qty = float(qty_str)
            action = "delete" if qty == 0.0 else "update"
            row = (BINANCE_SPOT_INST, event_time, seq, "sell", action, price, qty, meta_common)
            self._batcher.add("bspot_depth", row)
        self._last_depth_u = seq


async def run_binance_spot_ingest(
    db: DB,
    batcher: AsyncBatcher,
    symbol: str = "btcusdt",
    depth_levels: int = 20,
    depth_ms: int = 100,
    use_agg_trade: bool = True,
    changes_queue: asyncio.Queue[dict] | None = None,
    hb_ref: dict[str, float] | None = None,
) -> None:
    runner = SpotWSRunner(
        db=db,
        batcher=batcher,
        symbol=symbol,
        depth_levels=depth_levels,
        depth_ms=depth_ms,
        use_agg_trade=use_agg_trade,
        changes_queue=changes_queue,
        hb_ref=hb_ref,
    )
    await runner.run()
