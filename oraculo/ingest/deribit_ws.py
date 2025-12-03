# =========================================================
# file: oraculo/ingest/deribit_ws.py
# =========================================================
from __future__ import annotations

import asyncio
import datetime as dt
import json
import time
from typing import Any, Dict, List, Optional

import aiohttp
import websockets
from loguru import logger

from oraculo.db import DB
from oraculo.ingest.batch_writer import AsyncBatcher
from oraculo.obs.metrics import  (ws_msgs_total, ws_last_msg_age_s, ws_heartbeat_miss_total, ws_resync_total,)

DERI_WS = "wss://www.deribit.com/ws/api/v2"
DERI_HTTP = "https://www.deribit.com/api/v2"


def canon_id(instr: str) -> str:
    return f"DERIBIT:OPTIONS:{instr}"


def _sanitize_params(p: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in p.items():
        if isinstance(v, bool):
            out[k] = "true" if v else "false"
        else:
            out[k] = v
    return out


class DeribitRunner:
    """WS Deribit Opciones: trades, book (gap-detect+resync), ticker, markprice."""

    def __init__(
        self,
        db: DB,
        batcher: AsyncBatcher,
        ws_url: str,
        index_name_mark: str,
        interval_trades: str,
        interval_ticker: str,
        interval_book: str,
        expiries_front: int,
        strikes_around_atm: int,
        hb_timeout_s: int = 30,
        use_trades_by_instrument: bool = False,
        auth_client_id: Optional[str] = None,
        auth_client_secret: Optional[str] = None,
        trades_backstop_agg: bool = True,
    ) -> None:
        self.db = db
        self.batcher = batcher
        self.ws_url = ws_url or DERI_WS
        self.index_name_mark = index_name_mark
        self.interval_trades = interval_trades
        self.interval_ticker = interval_ticker
        self.interval_book = interval_book
        self.expiries_front = max(1, int(expiries_front))
        self.strikes_around_atm = max(1, int(strikes_around_atm))
        self.hb_timeout_s = hb_timeout_s
        self.use_trades_by_instrument = use_trades_by_instrument
        self.auth_client_id = auth_client_id
        self.auth_client_secret = auth_client_secret
        self.trades_backstop_agg = trades_backstop_agg

        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._http: Optional[aiohttp.ClientSession] = None
        # último mensaje recibido en cualquier canal (para watchdog global)
        self._last_msg = asyncio.get_event_loop().time()
        # último mensaje por tipo de canal (para métricas Prometheus)
        self._last_msg_ts: Dict[str, float] = {
            "trades": 0.0,
            "ticker": 0.0,
            "book": 0.0,
            "mark": 0.0,
        }
        self._subscribed_instr: set[str] = set()
        self._book_last_change: Dict[str, int] = {}
        self._access_token: Optional[str] = None

    async def run(self) -> None:
        self._http = aiohttp.ClientSession(trust_env=True)
        try:
            while True:
                try:
                    await self._run_once()
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning(f"[deribit] loop error: {e}; backoff")
                    await asyncio.sleep(2.0)
        finally:
            await self._http.close()

    async def _run_once(self) -> None:
        async with websockets.connect(self.ws_url, ping_interval=None, max_queue=None) as ws:
            self._ws = ws
            self._last_msg = asyncio.get_event_loop().time()

            await self._maybe_authenticate()

            instruments = await self._discover_instruments()

            channels: set[str] = set()
            channels.add(f"markprice.options.{self.index_name_mark}")

            if self.use_trades_by_instrument and instruments and self._access_token:
                channels.update({f"trades.{instr}.raw" for instr in instruments})
            if self.trades_backstop_agg:
                channels.add("trades.option.BTC." + self.interval_trades)

            for instr in instruments:
                channels.add(f"ticker.{instr}.{self.interval_ticker}")
                channels.add(f"book.{instr}.{self.interval_book}")

            await self._subscribe(sorted(channels))
            logger.info(f"[deribit] subscribed: {len(channels)} channels")

            try:
                await ws.send(
                    json.dumps(
                        {
                            "jsonrpc": "2.0",
                            "method": "public/set_heartbeat",
                            "id": 1,
                            "params": {"interval": 10},
                        }
                    )
                )
            except Exception:
                pass

            reader = asyncio.create_task(self._reader(), name="deri-reader")
            refresher = asyncio.create_task(self._refresher(), name="deri-refresh")
            watchdog = asyncio.create_task(self._watchdog(), name="deri-watchdog")
            done, _ = await asyncio.wait(
                {reader, refresher, watchdog},
                return_when=asyncio.FIRST_COMPLETED,
            )

            # Consume task exceptions para evitar "Task exception was never retrieved"
            # y tratar el cierre del websocket por heartbeat como una condición normal.
            for t in done:
                exc = t.exception()
                if exc is None:
                    continue
                # Deribit puede cerrar el socket con un código tipo 4000 ("heartbeat close").
                # Lo tratamos como un cierre limpio: lo logueamos y dejamos que el bucle run() reintente.
                if isinstance(exc, websockets.exceptions.ConnectionClosed):
                    code = getattr(exc, "code", None)
                    reason = getattr(exc, "reason", None)
                    logger.warning(
                        f"[deribit] ws closed ({t.get_name()}): code={code} reason={reason}"
                    )
                else:
                    # Cualquier otra excepción sube hacia run() para que aplique backoff.
                    raise exc

            for t in {reader, refresher, watchdog} - done:
                t.cancel()

    async def _maybe_authenticate(self) -> None:
        if not (self.auth_client_id and self.auth_client_secret) or self._ws is None:
            return
        req = {
            "jsonrpc": "2.0",
            "id": 0,
            "method": "public/auth",
            "params": {
                "grant_type": "client_credentials",
                "client_id": self.auth_client_id,
                "client_secret": self.auth_client_secret,
            },
        }
        await self._ws.send(json.dumps(req))
        try:
            resp = json.loads(await asyncio.wait_for(self._ws.recv(), timeout=5))
        except Exception as e:
            logger.error(f"[deribit] auth timeout: {e}")
            return
        if "result" in resp and resp["result"].get("access_token"):
            self._access_token = resp["result"]["access_token"]
            logger.info("[deribit] auth OK (client_credentials)")
        else:
            logger.error(f"[deribit] auth FAIL: {resp}")

    async def _reader(self) -> None:
        assert self._ws is not None
        async for raw in self._ws:
            self._last_msg = asyncio.get_event_loop().time()
            now = asyncio.get_event_loop().time()
            self._last_msg = now
            try:
                msg = json.loads(raw)
            except Exception:
                continue

            if msg.get("method") == "heartbeat":
                p = msg.get("params") or {}
                if p.get("type") == "test_request":
                    rid = int(time.time() * 1000) % 1_000_000
                    await self._ws.send(
                        json.dumps(
                            {
                                "jsonrpc": "2.0",
                                "id": rid,
                                "method": "public/test",
                                "params": {},
                            }
                        )
                    )
                continue

            if msg.get("method") != "subscription":
                continue
            p = msg["params"]
            chan = p["channel"]
            data = p["data"]
                            
            # Métricas por tipo de canal  última actividad
            if chan.startswith("trades."):
                self._last_msg_ts["trades"] = now
                ws_msgs_total.labels("deribit_trades").inc()
                await self._on_trades(data)
            elif chan.startswith("ticker."):
                self._last_msg_ts["ticker"] = now
                ws_msgs_total.labels("deribit_ticker").inc()
                await self._on_ticker(data)
            elif chan.startswith("book."):
                self._last_msg_ts["book"] = now
                ws_msgs_total.labels("deribit_book").inc()
                await self._on_book(data)
            elif chan.startswith("markprice.options."):
                self._last_msg_ts["mark"] = now
                ws_msgs_total.labels("deribit_mark").inc()
                await self._on_markprice(data)

    async def _refresher(self) -> None:
        while True:
            await asyncio.sleep(60)
            try:
                new = set(await self._discover_instruments())
                add = new - self._subscribed_instr
                if add:
                    subs = [
                        f"ticker.{i}.{self.interval_ticker}" for i in add
                    ] + [f"book.{i}.{self.interval_book}" for i in add]
                    if self.use_trades_by_instrument and self._access_token:
                        subs += [f"trades.{i}.raw" for i in add]
                    await self._subscribe(subs)
                    self._subscribed_instr |= add
                    logger.info(f"[deribit] +{len(add)} instruments subscribed")
            except Exception as e:
                logger.warning(f"[deribit] refresh fail: {e}")

    async def _watchdog(self) -> None:
        while True:
            await asyncio.sleep(1)
            
            now = asyncio.get_event_loop().time()

            # Actualizamos el gauge de "edad del último mensaje" por tipo de stream
            for stream, ts in self._last_msg_ts.items():
                if ts <= 0.0:
                    continue
                age = now - ts
                ws_last_msg_age_s.labels("deribit", stream).set(age)

            # Heartbeat global: tiempo desde el último mensaje de cualquier canal
            age_global = now - self._last_msg
            if age_global > self.hb_timeout_s:
                ws_heartbeat_miss_total.labels("deribit", "main").inc()
                logger.warning(f"[deribit] heartbeat miss -> reconnect (idle={age_global:.1f}s)")
                raise RuntimeError("deribit ws stalled")

    # -------- REST helpers --------
    async def _rest(self, method: str, params: Dict[str, Any]) -> Any:
        assert self._http is not None
        url = f"{DERI_HTTP}/{method}"
        sp = _sanitize_params(params)
        async with self._http.get(
            url, params=sp, timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            r.raise_for_status()
            js = await r.json()
            return js["result"]

    async def _discover_instruments(self) -> List[str]:
        idx = await self._rest(
            "public/get_index_price", {"index_name": self.index_name_mark}
        )
        atm = float(idx["index_price"])
        res = await self._rest(
            "public/get_instruments",
            {"currency": "BTC", "kind": "option", "expired": "false"},
        )
        live = [x for x in res if x.get("is_active")]
        live.sort(key=lambda x: x["expiration_timestamp"])
        expiries = sorted({x["expiration_timestamp"] for x in live})[
            : self.expiries_front
        ]
        cand = [x for x in live if x["expiration_timestamp"] in expiries]
        for x in cand:
            x["_dist"] = abs(float(x["strike"]) - atm)
        cand.sort(key=lambda x: (x["expiration_timestamp"], x["_dist"]))

        by_exp: Dict[int, List[dict]] = {}
        for x in cand:
            by_exp.setdefault(x["expiration_timestamp"], []).append(x)

        selected: List[dict] = []
        for _, arr in by_exp.items():
            if not arr:
                continue
            center = min(range(len(arr)), key=lambda i: arr[i]["_dist"])
            lo = max(0, center - self.strikes_around_atm)
            hi = min(len(arr), center + self.strikes_around_atm + 1)
            selected.extend(arr[lo:hi])

        names = [x["instrument_name"] for x in selected]
        if names:
            await self._upsert_instruments(selected)
        self._subscribed_instr = set(names)
        return names

    async def _upsert_instruments(self, rows: List[dict]) -> None:
        sql1 = """
        INSERT INTO deribit.options_instruments (instrument_id, underlying, expiry, strike, option_type, meta)
        VALUES ($1,$2,$3,$4,$5,$6)
        ON CONFLICT (instrument_id) DO UPDATE
        SET meta=EXCLUDED.meta, updated_at=now();
        """
        sql2 = """
        INSERT INTO oraculo.instrument_catalog(
          instrument_id, exchange, market_type, symbol, underlying, expiry, strike, option_type, tick_size, lot_size, meta)
        VALUES($1,'DERIBIT','OPTIONS',$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT (instrument_id) DO UPDATE
        SET meta=oraculo.instrument_catalog.meta || EXCLUDED.meta, updated_at=now();
        """
        rows1, rows2 = [], []
        for x in rows:
            instr = x["instrument_name"]
            iid = canon_id(instr)
            und = x.get("base_currency", "BTC")
            exp = dt.datetime.utcfromtimestamp(
                x["expiration_timestamp"] / 1000.0
            ).date()
            strike = float(x["strike"])
            opt_type = "C" if str(x["option_type"]).lower().startswith("c") else "P"
            tick = float(x.get("tick_size") or 0) or None
            lot = float(x.get("min_trade_amount") or 0) or None
            meta = {
                "contract_size": x.get("contract_size"),
                "option_type_raw": x.get("option_type"),
            }
            rows1.append([iid, und, exp, strike, opt_type, json.dumps(meta)])
            rows2.append(
                [
                    iid,
                    instr,
                    und,
                    exp,
                    strike,
                    opt_type,
                    tick,
                    lot,
                    json.dumps({"kind": "option"}),
                ]
            )
        if rows1:
            await self.db.execute_many(sql1, rows1)
        if rows2:
            await self.db.execute_many(sql2, rows2)

    async def _subscribe(self, channels: List[str]) -> None:
        if not channels or self._ws is None:
            return
        params: Dict[str, Any] = {"channels": channels}
        if self._access_token:
            params["access_token"] = self._access_token
        req = {
            "jsonrpc": "2.0",
            "method": "public/subscribe",
            "id": 42,
            "params": params,
        }
        await self._ws.send(json.dumps(req))

    # -------- handlers --------
    async def _on_trades(self, data: Any) -> None:
        arr = data if isinstance(data, list) else [data]
        for t in arr:
            instr = t["instrument_name"]
            iid = canon_id(instr)
            ts_ms = int(t.get("timestamp") or t.get("time") or 0)
            trade_id = str(t.get("trade_id") or t.get("trade_seq"))
            price = float(t["price"])
            qty = float(t["amount"])
            side = "buy" if t.get("direction") == "buy" else "sell"
            und_px = (
                float(t.get("index_price")) if t.get("index_price") is not None else None
            )
            meta = {"iv": t.get("iv"), "mark_price": t.get("mark_price")}
            self.batcher.add(
                "deriv_trades",
                [iid, ts_ms, trade_id, price, qty, side, und_px, json.dumps(meta)],
            )

    def _to_action(self, action: str) -> str:
        a = (action or "").lower()
        if a in ("new", "insert", "i", "snapshot"):
            return "insert"
        if a in ("change", "update", "u"):
            return "update"
        if a in ("delete", "remove", "d"):
            return "delete"
        return "update"

    async def _on_book(self, d: Dict[str, Any]) -> None:
        instr = d["instrument_name"]
        iid = canon_id(instr)
        ts_ms = int(d["timestamp"])
        change_id = int(d.get("change_id") or 0)
        prev = d.get("prev_change_id")
        last = self._book_last_change.get(instr)
        if (
            str(d.get("type", "")).lower() == "change"
            and last is not None
            and prev is not None
            and int(prev) != int(last)
        ):
            logger.warning(
                f"[deribit] gap {instr}: prev={prev} last={last} -> resync"
            )
            await self._resync_book(instr)
        self._book_last_change[instr] = change_id

        def emit(side: str, entry: Any) -> None:
            if isinstance(entry, dict):
                action = self._to_action(str(entry.get("action")))
                price = float(entry["price"])
                qty = float(entry["amount"])
            elif (
                isinstance(entry, list)
                and len(entry) == 3
                and isinstance(entry[0], str)
            ):
                action = self._to_action(entry[0])
                price = float(entry[1])
                qty = float(entry[2])
            elif isinstance(entry, list) and len(entry) >= 2:
                action = "update"
                price = float(entry[0])
                qty = float(entry[1])
            else:
                return
            self.batcher.add(
                "deriv_book",
                [iid, ts_ms, change_id, side, action, price, qty, json.dumps({})],
            )

        for e in d.get("bids", []):
            emit("buy", e)
        for e in d.get("asks", []):
            emit("sell", e)

    async def _resync_book(self, instrument_name: str) -> None:
        try:
            ob = await self._rest(
                "public/get_order_book",
                {"instrument_name": instrument_name, "depth": 50},
            )
            iid = canon_id(instrument_name)
            ts_ms = int(ob["timestamp"])
            ch_id = int(ob.get("change_id") or 0)
            for side, key in (("buy", "bids"), ("sell", "asks")):
                for price, qty in ob.get(key, []):
                    self.batcher.add(
                        "deriv_book",
                        [
                            iid,
                            ts_ms,
                            ch_id,
                            side,
                            "insert",
                            float(price),
                            float(qty),
                            json.dumps({"snapshot": True}),
                        ],
                    )
            self._book_last_change[instrument_name] = ch_id
            # Métrica de resync de libro en Deribit
            ws_resync_total.labels("deribit", "book").inc()
            
        except Exception as e:
            logger.error(f"[deribit] resync failed {instrument_name}: {e}")

    async def _on_ticker(self, d: Dict[str, Any]) -> None:
        instr = d["instrument_name"]
        iid = canon_id(instr)
        ts_ms = int(d.get("timestamp") or d.get("creation_timestamp") or 0)
        mark_iv = d.get("mark_iv")
        greeks = d.get("greeks") or {}
        bid = d.get("best_bid_price")
        ask = d.get("best_ask_price")
        und = d.get("underlying_price") or d.get("index_price")

        # -------- PATCH: open_interest vía WS --------
        # Preferimos el campo directo; si no existe, hacemos fallback a stats.open_interest
        stats = d.get("stats") or {}
        open_interest = d.get("open_interest")
        if open_interest is None:
            open_interest = stats.get("open_interest")

        # Row para el batcher "deriv_ticker":
        # instrument_id, event_time(ms), mark_iv, delta, gamma, vega, theta,
        # bid, ask, underlying_price, meta(jsonb), open_interest
        self.batcher.add(
            "deriv_ticker",
            [
                iid,
                ts_ms,
                mark_iv,
                greeks.get("delta"),
                greeks.get("gamma"),
                greeks.get("vega"),
                greeks.get("theta"),
                bid,
                ask,
                und,
                json.dumps({}),  # meta
                open_interest,
            ],
        )

    async def _on_markprice(self, arr: List[Dict[str, Any]]) -> None:
        for x in arr:
            instr = x["instrument_name"]
            iid = canon_id(instr)
            ts_ms = int(x["timestamp"])
            mark = x.get("mark_price")
            iv = x.get("iv")
            und = None
            self.batcher.add(
                "deriv_mark", [iid, ts_ms, mark, und, iv, json.dumps({})]
            )
