#=======================================
# file:  oraculo/ingest/binance_ws.py
#=======================================
from __future__ import annotations

import asyncio
import datetime as dt
import json
from typing import Any, Optional

import aiohttp
from loguru import logger

from ..obs.metrics import ws_msgs_total, ws_resync_total, ws_heartbeat_miss_total, ws_last_msg_age_s
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

BINANCE_FUT_INST = "BINANCE:PERP:BTCUSDT"
FAPI_BASE = "https://fapi.binance.com"


def _ms_to_ts(ms: int) -> dt.datetime:
    return dt.datetime.fromtimestamp(ms / 1000.0, tz=dt.timezone.utc)


def _to_float(s: str | float | int | None) -> float | None:
    if s is None:
        return None
    return float(s)


def _normalize_payload(raw: str | dict[str, Any]) -> dict[str, Any]:
    if isinstance(raw, str):
        try:
            msg = json.loads(raw)
        except Exception:
            return {}
    else:
        msg = raw
    return msg.get("data", msg)


class FuturesWSRunner:
    """Futures WS con gap-detect/resync depth y heartbeat configurable."""

    def __init__(
        self,
        db: DB,
        batcher: AsyncBatcher,
        depth_levels: int,
        depth_ms: int,
        hb_ref: dict[str, float] | None = None,
    ) -> None:
        self._db = db
        self._batcher = batcher
        self._depth_levels = depth_levels
        self._depth_ms = depth_ms
        self._hb = hb_ref or {"trade": 20.0, "depth": 5.0, "mark": 15.0, "forceOrder": 60.0}
        self._mgr: Optional[BinanceWebSocketApiManager] = None
        self._sid_trade: Optional[str] = None
        self._sid_depth: Optional[str] = None
        self._sid_mark: Optional[str] = None
        self._sid_liq: Optional[str] = None
        self._last_depth_u: int = 0
        self._last_msg_ts: dict[str, float] = {"trade": 0.0, "depth": 0.0, "mark": 0.0, "forceOrder": 0.0}

        # Track simple OB state to derive insert/delete/update deltas (for detectors that
        # differentiate entre adds y retiros).
        self._depth_book: dict[str, dict[float, float]] = {"buy": {}, "sell": {}}

        # --- NUEVO: control para evitar bucles de resync en depth ---
        # si el gap en ids es enorme, asumimos "hemos estado ciegos" y
        # rebasamos baseline sin seguir reintentando.
        self._huge_gap_rebase: int = 500_000  # ids de update, ajustable
        self._depth_gap_streak: int = 0       # nº de gaps seguidos manejados con resync
        self._max_depth_gap_streak: int = 5   # a partir de aquí, dejamos de resync y rebasamos

    async def run(self) -> None:
        await self._create_streams()
        session: Optional[aiohttp.ClientSession] = None
        try:
            session = aiohttp.ClientSession(headers={"User-Agent": "Oraculo/1.0"})
            while True:
                await self._check_heartbeats()
                raw = self._mgr.pop_stream_data_from_stream_buffer() if self._mgr else None
                if raw is None:
                    await asyncio.sleep(0.01)
                    continue
                p = _normalize_payload(raw)
                if not p or "e" not in p:
                    continue
                et = p["e"]
                ws_msgs_total.labels(et).inc()
                now = asyncio.get_event_loop().time()
                key = "depth" if et == "depthUpdate" else ("mark" if et in ("markPriceUpdate", "24hrMiniTicker", "24hrTicker") else et)
                if key in self._last_msg_ts:
                    self._last_msg_ts[key] = now

                if et == "trade":
                    await self._handle_trade(p)

                elif et == "depthUpdate":
                    pu = int(p.get("pu") or 0)

                    # 1) Sin baseline aún: aceptar primer mensaje que entre
                    if not self._last_depth_u:
                        self._depth_gap_streak = 0
                        await self._handle_depth(p)
                        continue

                    # 2) Normal: en sync
                    if pu == self._last_depth_u:
                        self._depth_gap_streak = 0
                        await self._handle_depth(p)
                        continue

                    # 3) Caso pu < last_depth_u -> mensaje viejo / reordenado: lo ignoramos sin resync
                    gap = pu - self._last_depth_u
                    if gap < 0:
                        logger.debug(
                            "[fut-depth] stale/out-of-order depthUpdate pu=%s < last=%s -> skip",
                            pu,
                            self._last_depth_u,
                        )
                        continue

                    # 4) Gap gigante: típico tras caída larga de red.
                    # No tiene sentido intentar "coser" millones de updates perdidos:
                    # rebasamos baseline y seguimos.
                    if gap >= self._huge_gap_rebase:
                        logger.warning(
                            "[fut-depth] HUGE gap detected pu=%s last=%s (gap=%s) -> rebase baseline without resub",
                            pu,
                            self._last_depth_u,
                            gap,
                        )
                        self._last_depth_u = pu
                        self._depth_gap_streak = 0
                        await self._handle_depth(p)
                        continue

                    # 5) Gap "normal" (relativamente pequeño): intentamos resync, pero
                    # con límite de reintentos para no entrar en bucle infinito.
                    self._depth_gap_streak += 1
                    logger.warning(
                        "[fut-depth] gap detected pu=%s last=%s (gap=%s) streak=%s -> resync",
                        pu,
                        self._last_depth_u,
                        gap,
                        self._depth_gap_streak,
                    )

                    if self._depth_gap_streak <= self._max_depth_gap_streak:
                        ws_resync_total.labels("futures", "depth").inc()
                        await self._resync_depth(session)
                    else:
                        logger.error(
                            "[fut-depth] too many resync attempts (%s), "
                            "disabling strict gap detect and rebasing on current pu",
                            self._depth_gap_streak,
                        )
                        self._last_depth_u = pu
                        self._depth_gap_streak = 0
                        await self._handle_depth(p)

                    continue

                elif et in ("markPriceUpdate", "24hrMiniTicker", "24hrTicker"):
                    await self._handle_mark(p)
                elif et == "forceOrder":
                    await self._handle_liq(p)

                await self._batcher.flush_if_needed()
        finally:
            if session:
                await session.close()
            await self._stop_all()

    async def _create_streams(self) -> None:
        self._mgr = BinanceWebSocketApiManager(exchange="binance.com-futures")
        symbol = "btcusdt"
        depth_channel = f"depth{self._depth_levels}@{self._depth_ms}ms"
        self._sid_trade = self._mgr.create_stream(["trade"], [symbol])
        self._sid_depth = self._mgr.create_stream([depth_channel], [symbol])
        self._sid_mark = self._mgr.create_stream(["markPrice@1s"], [symbol])
        self._sid_liq = self._mgr.create_stream(["forceOrder"], [symbol])
        logger.info(f"[fut-ws] Streams up: trade, {depth_channel}, markPrice@1s, forceOrder")

    async def _resub(self, stream: str) -> None:
        if not self._mgr:
            return
        symbol = "btcusdt"
        try:
            if stream == "trade":
                if self._sid_trade:
                    self._mgr.stop_stream(self._sid_trade)
                self._sid_trade = self._mgr.create_stream(["trade"], [symbol])
            elif stream == "depth":
                if self._sid_depth:
                    self._mgr.stop_stream(self._sid_depth)
                depth_channel = f"depth{self._depth_levels}@{self._depth_ms}ms"
                self._sid_depth = self._mgr.create_stream([depth_channel], [symbol])
            elif stream == "mark":
                if self._sid_mark:
                    self._mgr.stop_stream(self._sid_mark)
                self._sid_mark = self._mgr.create_stream(["markPrice@1s"], [symbol])
            elif stream == "forceOrder":
                if self._sid_liq:
                    self._mgr.stop_stream(self._sid_liq)
                self._sid_liq = self._mgr.create_stream(["forceOrder"], [symbol])
            ws_resync_total.labels("futures", stream).inc()
            logger.info(f"[fut-ws] resubscribed stream={stream}")
        except Exception as e:
            logger.warning(f"[fut-ws] resub {stream} fallo {type(e).__name__}: {e!s}")

    async def _resync_depth(self, session: aiohttp.ClientSession) -> None:
        """
        Resincronización "soft" del depth:
        - Resetea la referencia de secuencia.
        - Resuscribe el stream.
        - Pide snapshot solo como comprobación/log, pero NO lo usa para _last_depth_u.
        """
        # resetear baseline; el primer depthUpdate posterior entra limpio
        self._last_depth_u = 0
        self._depth_gap_streak = 0

        await self._resub("depth")

        try:
            async with session.get(f"{FAPI_BASE}/fapi/v1/depth", params={"symbol": "BTCUSDT", "limit": 1000}) as r:
                r.raise_for_status()
                data = await r.json()
                last_u = int(data.get("lastUpdateId") or 0)
                logger.info(
                    "[fut-depth] fetched snapshot lastUpdateId=%s (solo informativo, no usado para seq)",
                    last_u,
                )
        except Exception as e:
            logger.warning(f"[fut-depth] snapshot failed {type(e).__name__}: {e!s}")

    async def _check_heartbeats(self) -> None:
        now = asyncio.get_event_loop().time()
        any_last = max(self._last_msg_ts.values() or [0.0])
        for stream, ts in list(self._last_msg_ts.items()):
            if ts == 0.0:
                continue
            age = now - ts
            ws_last_msg_age_s.labels("binance_futures", stream).set(age)

            th = float(self._hb.get(stream, 10.0))
            eff = th * (2.0 if (now - any_last) < 3.0 else 1.0)
            if age > eff:
                ws_heartbeat_miss_total.labels("futures", stream).inc()
                logger.warning(
                    f"[fut-ws] heartbeat miss stream={stream} idle={age:.1f}s thr={eff:.1f}s -> resub"
                )
                await self._resub(stream)
                self._last_msg_ts[stream] = now

    async def _stop_all(self) -> None:
        if self._mgr:
            try:
                self._mgr.stop_manager_with_all_streams()
            except Exception:
                pass
            self._mgr = None

    # --- Handlers ---
    async def _handle_trade(self, p: dict[str, Any]) -> None:
        event_time = _ms_to_ts(p["E"])
        price = float(p["p"])
        qty = float(p["q"])
        trade_id = int(p["t"])
        side = "sell" if p.get("m", False) else "buy"
        meta = json.dumps({"is_buyer_maker": p.get("m", None)})
        row = (BINANCE_FUT_INST, event_time, trade_id, price, qty, side, meta)
        self._batcher.add("bfut_trades", row)

    async def _handle_depth(self, p: dict[str, Any]) -> None:
        event_time = _ms_to_ts(p["E"])
        seq = int(p.get("u", 0))
        meta_common = json.dumps({"U": p.get("U"), "u": p.get("u"), "pu": p.get("pu")})
        for price_str, qty_str in p.get("b", []):
            price = float(price_str)
            qty = float(qty_str)
            action, eff_qty = self._normalize_depth_action("buy", price, qty)
            if action:
                row = (BINANCE_FUT_INST, event_time, seq, "buy", action, price, eff_qty, meta_common)
                self._batcher.add("bfut_depth", row)
        for price_str, qty_str in p.get("a", []):
            price = float(price_str)
            qty = float(qty_str)
            action, eff_qty = self._normalize_depth_action("sell", price, qty)
            if action:
                row = (BINANCE_FUT_INST, event_time, seq, "sell", action, price, eff_qty, meta_common)
                self._batcher.add("bfut_depth", row)
        self._last_depth_u = seq

    def _normalize_depth_action(self, side: str, price: float, new_qty: float) -> tuple[str | None, float]:
        """
        Binance depth update envía cantidades absolutas. Para los detectores necesitamos
        diferenciar entre inserciones (qty aumenta) y retiros (qty disminuye).

        Devuelve (action, qty_effective) donde:
        - "insert": qty_effective es el aumento respecto al nivel previo (o total si no existía).
        - "delete": qty_effective es la cantidad retirada (nivel a cero o reducción parcial).
        - None: sin cambio real.
        """
        book = self._depth_book[side]
        prev = book.get(price, 0.0)

        if new_qty == prev:
            return None, 0.0

        if new_qty <= 0.0:
            # nivel eliminado
            if prev == 0.0:
                return None, 0.0
            book.pop(price, None)
            return "delete", prev

        if prev == 0.0:
            # nuevo nivel
            book[price] = new_qty
            return "insert", new_qty

        if new_qty > prev:
            # aumento de cantidad existente
            book[price] = new_qty
            return "insert", new_qty - prev

        # reducción parcial
        book[price] = new_qty
        return "delete", prev - new_qty

    async def _handle_mark(self, p: dict[str, Any]) -> None:
        event_time = _ms_to_ts(p.get("E") or p.get("eventTime", 0))
        mark = _to_float(p.get("p") or p.get("markPrice"))
        index = _to_float(p.get("i") or p.get("indexPrice"))
        fr = _to_float(p.get("r") or p.get("fundingRate"))
        nfund_ms = p.get("T") or p.get("nextFundingTime")
        next_funding = _ms_to_ts(nfund_ms) if isinstance(nfund_ms, int) else None
        basis_bps = ((mark - index) / index * 10000.0) if (mark is not None and index not in (None, 0.0)) else None
        meta = json.dumps({})
        row = (BINANCE_FUT_INST, event_time, mark, index, fr, next_funding, basis_bps, meta)
        self._batcher.add("bfut_mark", row)

    async def _handle_liq(self, p: dict[str, Any]) -> None:
        o = p.get("o", {})
        ts_ms = int(o.get("T") or p.get("E") or 0)
        event_time = _ms_to_ts(ts_ms)
        side = str(o.get("S", "SELL")).lower()
        price = float(o.get("ap") or o.get("p") or 0.0)
        qty = float(o.get("z") or o.get("q") or 0.0)
        quote = price * qty if (price and qty) else None
        external_id = f"{o.get('s','')}-{o.get('S','')}-{int(o.get('T',0))}-{o.get('p','')}-{o.get('z','')}"
        meta = json.dumps({"status": o.get("X"), "type": o.get("o"), "tif": o.get("f"), "last_filled": o.get("l")})
        row = (BINANCE_FUT_INST, event_time, side, price, qty, quote, external_id, meta)
        self._batcher.add("bfut_liq", row)


async def run_binance_ingest(
    db: DB,
    batcher: AsyncBatcher,
    depth_levels: int,
    depth_ms: int,
    hb_ref: dict[str, float] | None = None,
) -> None:
    runner = FuturesWSRunner(db, batcher, depth_levels=depth_levels, depth_ms=depth_ms, hb_ref=hb_ref)
    await runner.run()
