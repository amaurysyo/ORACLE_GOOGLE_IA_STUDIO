#=======================================
# file:  oraculo/ingest/binance_rest.py
#=======================================

from __future__ import annotations

import asyncio
import datetime as dt
import random
import time
from typing import Any, Mapping, Optional

import aiohttp
from aiohttp import ClientConnectorError, ClientResponseError
from loguru import logger

from .batch_writer import AsyncBatcher
from ..db import DB
from ..obs.metrics import http_fail_total, http_latency_ms

BINANCE_FAPI = "https://fapi.binance.com"


def _ms_to_ts(ms: int) -> dt.datetime:
    return dt.datetime.fromtimestamp(ms / 1000.0, tz=dt.timezone.utc)


class TokenBucket:
    """Token-bucket simple (h/s)."""

    def __init__(self, rate_per_sec: float, capacity: float | None = None) -> None:
        self.rate = max(0.001, float(rate_per_sec))
        self.capacity = float(capacity if capacity is not None else max(1.0, self.rate * 2))
        self.tokens = self.capacity
        self.ts = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self.ts
        self.ts = now
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)

    async def acquire(self, amount: float = 1.0) -> None:
        self._refill()
        if self.tokens >= amount:
            self.tokens -= amount
            return
        deficit = amount - self.tokens
        wait = deficit / self.rate
        await asyncio.sleep(wait)
        self.tokens = 0.0


def _mk_session_headers(settings: dict) -> dict:
    return {"User-Agent": settings.get("ua", "Oraculo/1.0")}


def _mk_connector(settings: dict) -> Optional[aiohttp.TCPConnector]:
    insecure = bool(settings.get("insecure_ssl", False))
    return aiohttp.TCPConnector(ssl=not insecure)


async def _get_json(
    session: aiohttp.ClientSession,
    path: str,
    params: Mapping[str, Any] | None = None,
    *,
    op: str | None = None,
) -> Any:
    """Wrapper para GET JSON con métricas de latencia y errores.

    - Expone http_latency_ms_bucket para service="binance_rest", op=<op>.
    - Incrementa http_fail_total para errores de HTTP/timeout/conexión.
    """
    url = BINANCE_FAPI + path
    op_label = op or path
    start = time.perf_counter()
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=7.0)) as r:
            r.raise_for_status()
            data = await r.json()
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            http_latency_ms.labels(service="binance_rest", op=op_label).observe(elapsed_ms)
            return data
    except ClientResponseError as e:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        # http_fail_total{service="binance_rest",reason="http_<status>"}
        http_fail_total.labels(service="binance_rest", reason=f"http_{e.status}").inc()
        http_latency_ms.labels(service="binance_rest", op=op_label).observe(elapsed_ms)
        logger.warning(f"[REST] HTTP {e.status} {e.message!r} url={url} params={dict(params or {})}")
        raise
    except ClientConnectorError as e:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        http_fail_total.labels(service="binance_rest", reason="connect_error").inc()
        http_latency_ms.labels(service="binance_rest", op=op_label).observe(elapsed_ms)
        logger.warning(f"[REST] CONNECT {e.os_error!r} url={url}")
        raise
    except asyncio.TimeoutError:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        http_fail_total.labels(service="binance_rest", reason="timeout").inc()
        http_latency_ms.labels(service="binance_rest", op=op_label).observe(elapsed_ms)
        logger.warning(f"[REST] TIMEOUT url={url}")
        raise


async def run_open_interest_poller(
    batcher: AsyncBatcher,
    settings: dict,
    instrument_id: str = "BINANCE:PERP:BTCUSDT",
    *,
    db: Optional[DB] = None,
) -> None:
    """
    GET /fapi/v1/openInterest -> binance_futures.open_interest
    - Intervalo base: settings['open_interest_ms'] (default 1000)
    - Token-bucket: limits.open_interest_rps (default 1.0)
    - Jitter: ±10%
    """
    symbol = "BTCUSDT"
    headers = _mk_session_headers(settings)
    connector = _mk_connector(settings)
    limits = (settings.get("limits") or {})
    rps = float(limits.get("open_interest_rps", 1.0))
    bucket = TokenBucket(rate_per_sec=rps)
    async with aiohttp.ClientSession(headers=headers, connector=connector, trust_env=True) as session:
        backoff = 0.5
        base_sleep = max(0.001, float(settings.get("open_interest_ms", settings.get("oi_ms", 1000))) / 1000.0)
        oi_window_s = float(settings.get("oi_doc_window_s", settings.get("oi_window_s", 120)))
        while True:
            try:
                await bucket.acquire(1.0)
                data = await _get_json(
                    session,
                    "/fapi/v1/openInterest",
                    {"symbol": symbol},
                    op="open_interest",
                )
                ts = _ms_to_ts(int(data["time"]))
                oi = float(data["openInterest"])
                batcher.add("bfut_oi", (instrument_id, ts, oi, "{}"))
                if db is not None:
                    try:
                        prev_cutoff = ts - dt.timedelta(seconds=oi_window_s)
                        prev_oi = await db.fetchval(
                            """
                            SELECT open_interest
                            FROM binance_futures.open_interest
                            WHERE instrument_id = $1 AND event_time <= $2
                            ORDER BY event_time DESC
                            LIMIT 1
                            """,
                            instrument_id,
                            prev_cutoff,
                        )
                        if prev_oi not in (None, 0):
                            delta_pct = (oi - float(prev_oi)) / float(prev_oi) * 100.0
                            await db.execute(
                                """
                                INSERT INTO oraculo.metrics_series
                                  (instrument_id, event_time, window_s, metric, value, profile, meta)
                                VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
                                ON CONFLICT DO NOTHING
                                """,
                                instrument_id,
                                ts,
                                int(oi_window_s),
                                "oi_delta_pct_doc",
                                float(delta_pct),
                                "default",
                                "{}",
                            )
                    except Exception as e:
                        logger.warning(f"[oi] fallo delta_pct_doc {type(e).__name__}: {e!s}")
                await batcher.flush_if_needed()
                backoff = 0.5
                sleep_s = base_sleep * random.uniform(0.9, 1.1)
                await asyncio.sleep(sleep_s)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[oi] fallo {type(e).__name__}: {e!s}; backoff")
                await asyncio.sleep(backoff * random.uniform(0.9, 1.1))
                backoff = min(backoff * 2, 15.0)


async def _fetch_tt_last(
    session: aiohttp.ClientSession,
    endpoint: str,
    symbol: str,
    period: str,
) -> dict | None:
    op = f"tt_{endpoint}"
    arr = await _get_json(
        session,
        f"/futures/data/{endpoint}",
        {"symbol": symbol, "period": period, "limit": 1},
        op=op,
    )
    return arr[-1] if arr else None


async def run_top_traders_pollers(
    batcher: AsyncBatcher, settings: dict, instrument_id: str = "BINANCE:PERP:BTCUSDT"
) -> None:
    """
    GET /futures/data/topLongShort{Account,Position}Ratio
    - Intervalo base: settings['top_traders_ms'] (default 15000, efectivo >= 5000)
    - Token-bucket: limits.top_traders_rps (default 0.2 => 1/5s)
    - Jitter: ±10%
    """
    symbol = "BTCUSDT"
    period = settings.get("top_traders_period", settings.get("tt_period", "5m"))
    headers = _mk_session_headers(settings)
    connector = _mk_connector(settings)
    limits = (settings.get("limits") or {})
    rps = float(limits.get("top_traders_rps", 0.2))
    bucket = TokenBucket(rate_per_sec=rps)
    async with aiohttp.ClientSession(headers=headers, connector=connector, trust_env=True) as session:
        backoff = 0.5
        base_sleep = max(5.0, float(settings.get("top_traders_ms", settings.get("tt_ms", 15000))) / 1000.0)
        while True:
            try:
                await bucket.acquire(1.0)
                a = await _fetch_tt_last(session, "topLongShortAccountRatio", symbol, period)
                if a:
                    ts = _ms_to_ts(int(a["timestamp"]))
                    lr = float(a.get("longAccount", 0) or 0)
                    sr = float(a.get("shortAccount", 0) or 0)
                    meta = {"longShortRatio": float(a.get("longShortRatio", 0) or 0)}
                    batcher.add("bfut_tt_acc", (instrument_id, ts, lr, sr, str(meta).replace("'", '"')))
                p = await _fetch_tt_last(session, "topLongShortPositionRatio", symbol, period)
                if p:
                    ts = _ms_to_ts(int(p["timestamp"]))
                    lr = float(p.get("longPosition", 0) or 0)
                    sr = float(p.get("shortPosition", 0) or 0)
                    meta = {"longShortRatio": float(p.get("longShortRatio", 0) or 0)}
                    batcher.add("bfut_tt_pos", (instrument_id, ts, lr, sr, str(meta).replace("'", '"')))
                await batcher.flush_if_needed()
                backoff = 0.5
                sleep_s = base_sleep * random.uniform(0.9, 1.1)
                await asyncio.sleep(sleep_s)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[top_traders] fallo {type(e).__name__}: {e!s}; backoff")
                await asyncio.sleep(backoff * random.uniform(0.9, 1.1))
                backoff = min(backoff * 2, 15.0)
