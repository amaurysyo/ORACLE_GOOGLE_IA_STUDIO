# oraculo/catalog/refresher.py
from __future__ import annotations

import asyncio
import datetime as dt
import json
from typing import Any, Dict, Iterable, List, Optional

import aiohttp
from loguru import logger

from oraculo.db import DB


def _inst_id(instr_name: str) -> str:
    return f"DERIBIT:OPTIONS:{instr_name.upper()}"


def _normalize_params(params: Dict[str, Any]) -> Dict[str, str]:
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
    async with session.get(url, params=q, timeout=aiohttp.ClientTimeout(total=10)) as r:
        r.raise_for_status()
        return await r.json()


async def fetch_deribit_instruments(underlyings: Iterable[str]) -> List[Dict[str, Any]]:
    """Lista completa (no expirados) por underlying para OPTIONS."""
    headers = {"User-Agent": "Oraculo/1.0"}
    out: List[Dict[str, Any]] = []
    async with aiohttp.ClientSession(headers=headers) as s:
        for ul in underlyings:
            res = await _rest_get(s, "public/get_instruments", {"currency": ul.upper(), "kind": "option", "expired": False})
            out.extend(res.get("result") or [])
    return out


async def upsert_instrument_catalog_deribit(db: DB, instruments: List[Dict[str, Any]]) -> int:
    """Upsert masivo en oraculo.instrument_catalog para Deribit options."""
    sql = (
        "INSERT INTO oraculo.instrument_catalog("
        " instrument_id, exchange, market_type, symbol, underlying, expiry, strike, option_type,"
        " tick_size, lot_size, active, meta)"
        " VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,true,$11::jsonb)"
        " ON CONFLICT (instrument_id) DO UPDATE SET"
        "  tick_size=EXCLUDED.tick_size, lot_size=EXCLUDED.lot_size,"
        "  active=EXCLUDED.active, meta=EXCLUDED.meta, updated_at=now()"
    )
    n = 0
    for it in instruments:
        try:
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
            n += 1
        except Exception as e:
            logger.debug(f"[catalog.deribit] skip {type(e).__name__}: {e!s}")
    return n


async def run_inprocess_catalog_refresh(
    db: DB,
    cfg_provider,  # ConfigManager con .cfg
    deribit_changes_queue: Optional[asyncio.Queue[dict]] = None,
) -> None:
    """
    Tarea periódica in-process: refresca catalog (Deribit) y dispara hot-reload.
    - Intervalo: cfg.catalog.refresh_hours (default 24).
    """
    logger.info("[catalog] refresher iniciado")
    first = True
    while True:
        try:
            cfg = cfg_provider.cfg
            hours = (getattr(cfg, "catalog", {}) or {}).get("refresh_hours", 24)
            hours = max(1, int(hours))
            dcfg = getattr(cfg, "deribit", {}) or {}
            if dcfg.get("enabled", False):
                underlyings = dcfg.get("underlyings", ["BTC"])
                items = await fetch_deribit_instruments(underlyings)
                n = await upsert_instrument_catalog_deribit(db, items)
                logger.info(f"[catalog] deribit upsert={n} instrumentos")
                if deribit_changes_queue is not None:
                    await deribit_changes_queue.put({
                        "underlyings": underlyings,
                        "book_interval": dcfg.get("book_interval", "100ms"),
                        "filters": dcfg.get("filters", {"expiries_front": 3, "strikes_around_atm": 5, "depth_levels": 10}),
                    })
                    logger.info("[catalog] hot-reload deribit solicitado")

            sleep_s = 1 if first else hours * 3600  # primera pasada inmediata
            first = False
            await asyncio.sleep(sleep_s)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"[catalog] refresher error {type(e).__name__}: {e!s}; retry en 60s")
            await asyncio.sleep(60)
