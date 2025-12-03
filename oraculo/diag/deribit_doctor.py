# oraculo/diag/deribit_doctor.py
from __future__ import annotations

import asyncio
import datetime as dt
import json
from typing import Any, Dict, List

import aiohttp
from aiohttp import WSMsgType
from loguru import logger

# Permite ejecución directa si se corre desde subcarpetas (Spyder)
try:
    from oraculo.db import DB  # type: ignore
except ModuleNotFoundError:
    import sys
    from pathlib import Path
    PROJ_ROOT = Path(__file__).resolve().parents[2]
    if str(PROJ_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJ_ROOT))
    from oraculo.db import DB  # type: ignore


def _ms_to_dt(ms: int | float | None) -> dt.datetime:
    ms = int(ms or 0)
    return dt.datetime.fromtimestamp(ms / 1000.0, tz=dt.timezone.utc)


def _inst_id(instr_name: str) -> str:
    return f"DERIBIT:OPTIONS:{instr_name.upper()}"


def _normalize_params(params: Dict[str, Any]) -> Dict[str, str]:
    """Forzar bool->'true'/'false' y todo a str para yarl/aiohttp."""
    out: Dict[str, str] = {}
    for k, v in (params or {}).items():
        if isinstance(v, bool):
            out[k] = "true" if v else "false"
        elif v is None:
            continue
        else:
            out[k] = str(v)
    return out


async def _rest_get(session: aiohttp.ClientSession, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = f"https://www.deribit.com/api/v2/{method}"
    q = _normalize_params(params)
    async with session.get(url, params=q, timeout=aiohttp.ClientTimeout(total=7)) as r:
        r.raise_for_status()
        return await r.json()


async def _pick_btc_options(count: int = 3) -> List[Dict[str, Any]]:
    """Elige ~count opciones BTC cercanas a ATM en expiraciones próximas."""
    headers = {"User-Agent": "Oraculo/1.0"}
    async with aiohttp.ClientSession(headers=headers) as s:
        idx = await _rest_get(s, "public/get_index_price", {"index_name": "btc_usd"})  # enum minúsculas
        px = float(idx.get("result", {}).get("index_price") or 0.0)

        ins = await _rest_get(
            s, "public/get_instruments",
            {"currency": "BTC", "kind": "option", "expired": False}
        )
        arr: List[Dict[str, Any]] = ins.get("result") or []
        if not arr:
            return []

        by_exp: Dict[int, List[Dict[str, Any]]] = {}
        for it in arr:
            by_exp.setdefault(int(it["expiration_timestamp"]), []).append(it)
        expiries = sorted(by_exp.keys())[:3]

        chosen: List[Dict[str, Any]] = []
        for ex in expiries:
            bucket = by_exp[ex]
            if px > 0:
                strikes = sorted({float(it["strike"]) for it in bucket})
                atm = min(strikes, key=lambda k: abs(k - px))
                for it in bucket:
                    if float(it["strike"]) == atm and it.get("option_type") in ("call", "put"):
                        chosen.append(it)
                        if len(chosen) >= count:
                            return chosen
            for it in bucket:
                if len(chosen) < count:
                    chosen.append(it)
        return chosen[:count]


async def run_deribit_ws_doctor(count: int = 3) -> Dict[str, Any]:
    """
    WS prueba rápida: ticker.<instrument>.raw (N) + markprice.options.BTC.
    Devuelve { instruments: [...], seen: {ticker:{}, mark:{}} }.
    * Maneja timeouts sin excepción (continúa esperando hasta 'end').
    """
    instruments = await _pick_btc_options(count=count)
    if not instruments:
        logger.warning("[doctor.deribit] no se encontraron instrumentos")
        return {"instruments": [], "seen": {}}

    headers = {"User-Agent": "Oraculo/1.0"}
    async with aiohttp.ClientSession(headers=headers) as sess:
        ws = await sess.ws_connect("wss://www.deribit.com/ws/api/v2", heartbeat=20.0)
        try:
            await ws.send_str(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "public/set_heartbeat", "params": {"interval": 10}}))

            ch = [f"ticker.{it['instrument_name']}.raw" for it in instruments]
            ch.append("markprice.options.BTC")
            await ws.send_str(json.dumps({"jsonrpc": "2.0", "id": 2, "method": "public/subscribe", "params": {"channels": ch}}))

            seen_ticker = {it["instrument_name"]: False for it in instruments}
            seen_mark = {it["instrument_name"]: False for it in instruments}

            end = asyncio.get_event_loop().time() + 20.0  # ventana más amplia
            while asyncio.get_event_loop().time() < end and (not all(seen_ticker.values()) or not any(seen_mark.values())):
                try:
                    msg = await ws.receive(timeout=2.0)  # timeouts cortos -> seguir
                except Exception:  # TimeoutError / asyncio.TimeoutError
                    continue
                if msg.type == WSMsgType.CLOSED or msg.type == WSMsgType.ERROR:
                    break
                if msg.type != WSMsgType.TEXT:
                    continue
                obj = json.loads(msg.data)
                if obj.get("method") != "subscription":
                    continue
                chan = obj["params"]["channel"]
                data = obj["params"]["data"]
                if chan.startswith("ticker."):
                    name = data.get("instrument_name")
                    if name in seen_ticker and not seen_ticker[name]:
                        seen_ticker[name] = True
                        bid = data.get("best_bid_price") or data.get("best_bid")
                        ask = data.get("best_ask_price") or data.get("best_ask")
                        logger.info(f"[doctor.deribit] ticker OK {name} bid={bid} ask={ask}")
                elif chan == "markprice.options.BTC":
                    items = data if isinstance(data, list) else [data]
                    by_name = {it.get("instrument_name") or it.get("i"): it for it in items if isinstance(it, dict)}
                    for it in instruments:
                        nm = it["instrument_name"]
                        if nm in by_name and not seen_mark[nm]:
                            seen_mark[nm] = True
                            mp = by_name[nm].get("mark_price") or by_name[nm].get("mp")
                            logger.info(f"[doctor.deribit] mark OK {nm} mark={mp}")

            if not any(seen_ticker.values()):
                logger.warning("[doctor.deribit] no llegó ningún ticker en la ventana de prueba")
            if not any(seen_mark.values()):
                logger.warning("[doctor.deribit] no llegó ningún markprice en la ventana de prueba")

            return {"instruments": instruments, "seen": {"ticker": seen_ticker, "mark": seen_mark}}
        finally:
            await ws.close()


async def upsert_instrument_catalog(db: DB, instruments: List[Dict[str, Any]]) -> None:
    """Upsert en oraculo.instrument_catalog para Deribit options."""
    sql = (
        "INSERT INTO oraculo.instrument_catalog("
        " instrument_id, exchange, market_type, symbol, underlying, expiry, strike, option_type,"
        " tick_size, lot_size, active, meta)"
        " VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,true,$11::jsonb)"
        " ON CONFLICT (instrument_id) DO UPDATE SET"
        "  tick_size=EXCLUDED.tick_size, lot_size=EXCLUDED.lot_size,"
        "  active=EXCLUDED.active, meta=EXCLUDED.meta, updated_at=now()"
    )
    for it in instruments:
        name: str = it["instrument_name"]
        underlying = str(it.get("base_currency") or "BTC").upper()
        expiry = dt.datetime.utcfromtimestamp(int(it["expiration_timestamp"]) / 1000.0).date()
        strike = float(it.get("strike") or 0.0)
        opt_type = "C" if it.get("option_type") == "call" else "P"
        tick_size = float(it.get("tick_size") or 0.0)
        lot_size = float(it.get("min_trade_amount") or it.get("contract_size") or 1.0)
        meta = {
            "settlement": it.get("settlement_currency"),
            "kind": it.get("kind"),
            "is_active": it.get("is_active"),
        }
        await db.execute(
            sql,
            _inst_id(name), "DERIBIT", "OPTIONS", name, underlying, expiry, strike, opt_type,
            tick_size, lot_size, json.dumps(meta)
        )
        logger.info(f"[catalog] upsert {name} -> {_inst_id(name)}")


if __name__ == "__main__":
    async def _main() -> None:
        res = await run_deribit_ws_doctor(count=3)
        print("Instruments:", [i["instrument_name"] for i in res.get("instruments", [])])
        print("Seen:", res.get("seen", {}))
    asyncio.run(_main())
