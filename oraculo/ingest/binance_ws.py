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

from ..obs.metrics import (
    ws_msgs_total,
    ws_resync_total,
    ws_heartbeat_miss_total,
    ws_last_msg_age_s,
)
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
        # Último u aplicado de depth
        self._last_depth_u: int = 0
        # Flag de si ya estamos alineados tras el último snapshot REST
        self._depth_synced: bool = False
        self._last_msg_ts: dict[str, float] = {
            "trade": 0.0,
            "depth": 0.0,
            "mark": 0.0,
            "forceOrder": 0.0,
        }

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
                key = (
                    "depth"
                    if et == "depthUpdate"
                    else ("mark" if et in ("markPriceUpdate", "24hrMiniTicker", "24hrTicker") else et)
                )
                if key in self._last_msg_ts:
                    self._last_msg_ts[key] = now

                if et == "trade":
                    await self._handle_trade(p)

                elif et == "depthUpdate":
                    # --- LÓGICA NUEVA DE ALINEACIÓN + GAP DETECTION CORRECTA ---
                    try:
                        U = int(p.get("U") or 0)
                        u = int(p.get("u") or 0)
                        pu = int(p.get("pu") or 0)
                    except Exception:
                        # Si algo raro, mejor log y seguir con el siguiente mensaje
                        logger.warning("[fut-depth] malformed depthUpdate payload: {}", p)
                        continue

                    last = self._last_depth_u

                    if last:
                        if not self._depth_synced:
                            # FASE DE ALINEACIÓN TRAS SNAPSHOT REST
                            # 1) Eventos antiguos (u < last) => se ignoran
                            if u < last:
                                logger.debug(
                                    "[fut-depth] skip old depth event U={} u={} pu={} last={}",
                                    U,
                                    u,
                                    pu,
                                    last,
                                )
                                continue

                            # 2) Evento que cruza el snapshot: U <= last <= u
                            if U <= last <= u:
                                logger.info(
                                    "[fut-depth] depth stream aligned: snapshot={} matched by U={} u={}",
                                    last,
                                    U,
                                    u,
                                )
                                self._depth_synced = True
                                # Deja pasar este evento a _handle_depth para aplicar el diff
                            else:
                                # Ni viejo ni cruza el snapshot, esperamos al siguiente
                                logger.debug(
                                    "[fut-depth] waiting align: snapshot={} vs U={} u={} pu={}",
                                    last,
                                    U,
                                    u,
                                    pu,
                                )
                                continue
                        else:
                            # FASE NORMAL: ya alineados, aquí sí exigimos continuidad por pu
                            if pu != last:
                                ws_resync_total.labels("futures", "depth").inc()
                                logger.warning(
                                    "[fut-depth] gap detected after sync: pu={} last={} U={} u={} -> resync",
                                    pu,
                                    last,
                                    U,
                                    u,
                                )
                                await self._resync_depth(session)
                                # Después del resync, volvemos a fase de alineación
                                continue

                    # Si last == 0 (inicio) o estamos alineados (o nos acabamos de alinear),
                    # aplicamos el diff normalmente.
                    await self._handle_depth(p)

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
        # En un resync de depth siempre volvemos a estado "no alineado"
        self._depth_synced = False
        await self._resub("depth")
        try:
            async with session.get(
                f"{FAPI_BASE}/fapi/v1/depth", params={"symbol": "BTCUSDT", "limit": 1000}
            ) as r:
                r.raise_for_status()
                data = await r.json()
                self._last_depth_u = int(data.get("lastUpdateId") or 0)
                logger.info(
                    "[fut-depth] resynced snapshot lastUpdateId={} (depth_synced={})",
                    self._last_depth_u,
                    self._depth_synced,
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
            action = "delete" if qty == 0.0 else "update"
            row = (BINANCE_FUT_INST, event_time, seq, "buy", action, price, qty, meta_common)
            self._batcher.add("bfut_depth", row)

        for price_str, qty_str in p.get("a", []):
            price = float(price_str)
            qty = float(qty_str)
            action = "delete" if qty == 0.0 else "update"
            row = (BINANCE_FUT_INST, event_time, seq, "sell", action, price, qty, meta_common)
            self._batcher.add("bfut_depth", row)

        # Actualizamos el último u aplicado
        self._last_depth_u = seq

    async def _handle_mark(self, p: dict[str, Any]) -> None:
        event_time = _ms_to_ts(p.get("E") or p.get("eventTime", 0))
        mark = _to_float(p.get("p") or p.get("markPrice"))
        index = _to_float(p.get("i") or p.get("indexPrice"))
        fr = _to_float(p.get("r") or p.get("fundingRate"))
        nfund_ms = p.get("T") or p.get("nextFundingTime")
        next_funding = _ms_to_ts(nfund_ms) if isinstance(nfund_ms, int) else None
        basis_bps = (
            (mark - index) / index * 10000.0 if (mark is not None and index not in (None, 0.0)) else None
        )
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
        meta = json.dumps(
            {"status": o.get("X"), "type": o.get("o"), "tif": o.get("f"), "last_filled": o.get("l")}
        )
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
