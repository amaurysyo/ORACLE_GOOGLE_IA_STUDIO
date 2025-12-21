# ===========================================
# file: scripts/cli.py
# ===========================================
from __future__ import annotations

import asyncio
import datetime as dt
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp
import click
import websockets
from dotenv import load_dotenv
from loguru import logger

from oraculo.config import load_config
from oraculo.db import DB
from oraculo.ingest.batch_writer import AsyncBatcher
from oraculo.obs.logging import setup_logging_json
from oraculo.obs.metrics import run_exporter

ROOT = Path(__file__).resolve().parents[1]


# ------- helpers -------
def _load_env() -> None:
    load_dotenv(ROOT / ".env")


def _run_async(coro) -> None:
    try:
        asyncio.get_running_loop()
        running = True
    except RuntimeError:
        running = False
    if not running:
        return asyncio.run(coro)
    try:
        import nest_asyncio  # type: ignore
        nest_asyncio.apply()
    except Exception:
        pass
    return asyncio.get_event_loop().run_until_complete(coro)


# ===========================================
# CLI ROOT
# ===========================================
@click.group()
def cli() -> None:
    """Oráculo CLI."""


# ===========================================
# ENV GROUP
# ===========================================
@cli.group("env")
def env_group() -> None:
    """Diagnóstico de entorno."""


@env_group.command("doctor")
@click.option("--full/--no-full", default=False, help="Incluye prueba REST E2E (openInterest / topTraders).")
def env_doctor(full: bool) -> None:
    """Chequea UBWA, proxies/TLS, ping Binance y DB. Con --full dispara prueba REST."""
    _load_env()
    cfg = load_config(str(ROOT / "config" / "config.yaml"))
    setup_logging_json(ROOT, "INFO", "00:00", "7 days", True)

    async def _run() -> None:
        # UBWA
        try:
            import unicorn_binance_websocket_api as ubwa  # type: ignore
            ver = getattr(ubwa, "__version__", None)
            if not ver:
                from importlib.metadata import version as _pkg_version
                ver = _pkg_version("unicorn-binance-websocket-api")
            logger.info(f"UBWA version: {ver}")
        except Exception as e:
            logger.error(f"UBWA import failed: {e!s}")

        # Proxies/TLS
        https_proxy = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")
        http_proxy = os.getenv("HTTP_PROXY") or os.getenv("http_proxy")
        logger.info(f"Proxies: HTTPS_PROXY={bool(https_proxy)} HTTP_PROXY={bool(http_proxy)}")
        insecure = bool((getattr(cfg, "rest_pollers", {}) or {}).get("insecure_ssl", False))
        logger.info(f"insecure_ssl={insecure}")

        # Ping Binance
        try:
            conn = aiohttp.TCPConnector(ssl=not insecure)
            async with aiohttp.ClientSession(trust_env=True, connector=conn) as s:
                async with s.get("https://fapi.binance.com/fapi/v1/ping", timeout=aiohttp.ClientTimeout(total=7)) as r:
                    logger.info(f"Binance FAPI ping: HTTP {r.status}")
        except Exception as e:
            logger.error(f"Binance FAPI ping failed: {e!s}")

        # DB
        db = DB(cfg.app.storage.dsn)
        try:
            await db.connect()
            v = await db.fetchval("SHOW server_version;")
            u = await db.fetchval("SELECT current_user;")
            logger.info(f"DB connected. version={v} user={u}")
            await db.execute("CREATE TEMP TABLE IF NOT EXISTS __oraculo_perm_test(id int);")
            await db.execute("INSERT INTO __oraculo_perm_test(id) VALUES (1);")
            logger.info("DB insert (TEMP) OK")
        finally:
            await db.close()

        if full:
            logger.info("E2E Binance listo (usa pollers del runtime).")

    _run_async(_run())


@env_group.command("deribit")
@click.option("--timeout", default=30, show_default=True, help="Segundos máximos esperando WS.")
def env_deribit(timeout: int) -> None:
    """
    Diagnóstico Deribit (REST + WS):
    - REST: index_price, get_instruments (expired=false), get_order_book (1 instrumento).
    - WS: trades.option.BTC y/o trades.<instrumento>.raw (si autenticado), ticker/book del instrumento y markprice.options.<index>.
    """
    _load_env()
    cfg = load_config(str(ROOT / "config" / "config.yaml"))
    setup_logging_json(ROOT, "INFO", "00:00", "7 days", True)

    async def _run() -> None:
        dcfg = (getattr(cfg, "streams", {}) or {}).get("deribit", {}) or {}
        if not dcfg.get("enabled", False):
            logger.warning("Deribit disabled en config. Activa streams.deribit.enabled: true")
        ws_url = dcfg.get("ws_url", "wss://www.deribit.com/ws/api/v2")
        idx_name = dcfg.get("index_name_mark", "btc_usd")
        it_trades = dcfg.get("interval_trades", "100ms")
        it_ticker = dcfg.get("interval_ticker", "100ms")
        it_book = dcfg.get("interval_book", "100ms")
        expiries_front = int((dcfg.get("filters", {}) or {}).get("expiries_front", 3))
        strikes_around_atm = int((dcfg.get("filters", {}) or {}).get("strikes_around_atm", 5))

        # Credenciales opcionales para raw
        auth_cfg = (dcfg.get("auth") or {})
        client_id = auth_cfg.get("client_id") or os.getenv("DERIBIT_CLIENT_ID")
        client_secret = auth_cfg.get("client_secret") or os.getenv("DERIBIT_CLIENT_SECRET")

        def sanitize(p: Dict[str, Any]) -> Dict[str, Any]:
            # Deribit no acepta bool Python en query-string
            return {k: ("true" if v is True else "false" if v is False else v) for k, v in p.items()}

        # --- REST ---
        async with aiohttp.ClientSession(trust_env=True) as http:
            r = await http.get(
                "https://www.deribit.com/api/v2/public/get_index_price",
                params={"index_name": idx_name},
                timeout=aiohttp.ClientTimeout(total=7),
            )
            r.raise_for_status()
            idx = (await r.json())["result"]["index_price"]
            logger.info(f"[REST] index {idx_name} = {idx}")

            p_ins = sanitize({"currency": "BTC", "kind": "option", "expired": False})
            r = await http.get(
                "https://www.deribit.com/api/v2/public/get_instruments",
                params=p_ins,
                timeout=aiohttp.ClientTimeout(total=10),
            )
            r.raise_for_status()
            ins_all = (await r.json())["result"]
            live = [x for x in ins_all if x.get("is_active")]
            live.sort(key=lambda x: x["expiration_timestamp"])
            expiries = sorted({x["expiration_timestamp"] for x in live})[:expiries_front]
            cand = [x for x in live if x["expiration_timestamp"] in expiries]
            for x in cand:
                x["_dist"] = abs(float(x["strike"]) - float(idx))
            cand.sort(key=lambda x: (x["expiration_timestamp"], x["_dist"]))
            pick = cand[0]["instrument_name"] if cand else None
            logger.info(f"[REST] instruments live={len(live)} expiries_front={expiries_front} pick={pick}")

            if pick:
                r = await http.get(
                    "https://www.deribit.com/api/v2/public/get_order_book",
                    params={"instrument_name": pick, "depth": 20},
                    timeout=aiohttp.ClientTimeout(total=10),
                )
                r.raise_for_status()
                ob = (await r.json())["result"]
                logger.info(f"[REST] order_book {pick}: bids={len(ob.get('bids', []))} asks={len(ob.get('asks', []))}")

        # --- WS ---
        if not pick:
            logger.warning("Sin instrumento seleccionable; WS probará solo trades.option y markprice.")
        seen: Dict[str, Optional[float]] = {"trades": None, "ticker": None, "book": None, "markprice": None}
        t0 = time.perf_counter()

        async with websockets.connect(ws_url, ping_interval=None, max_queue=None) as ws:
            # (1) Autenticación opcional para raw
            access_token: Optional[str] = None
            if client_id and client_secret:
                await ws.send(json.dumps({
                    "jsonrpc": "2.0",
                    "id": 0,
                    "method": "public/auth",
                    "params": {"grant_type": "client_credentials", "client_id": client_id, "client_secret": client_secret}
                }))
                auth_resp = json.loads(await ws.recv())
                if "result" in auth_resp and auth_resp["result"].get("access_token"):
                    access_token = auth_resp["result"]["access_token"]
                    logger.info("[WS] auth OK (client_credentials)")
                else:
                    logger.error(f"[WS] auth FAIL: {auth_resp}")

            channels: List[str] = [f"trades.option.BTC.{it_trades}", f"markprice.options.{idx_name}"]
            if pick:
                # solo pedimos raw si autenticado; si no, evitamos error 13778
                if access_token:
                    channels += [f"ticker.{pick}.{it_ticker}", f"book.{pick}.{it_book}", f"trades.{pick}.raw"]
                else:
                    channels += [f"ticker.{pick}.{it_ticker}", f"book.{pick}.{it_book}"]

            sub_params: Dict[str, Any] = {"channels": channels}
            if access_token:
                sub_params["access_token"] = access_token

            await ws.send(json.dumps({"jsonrpc": "2.0", "method": "public/subscribe", "id": 1, "params": sub_params}))
            logger.info(f"[WS] subscribed {len(channels)} canales: {channels}")

            # Habilita heartbeats, Deribit exige responder "test_request" con public/test
            try:
                await ws.send(
                    json.dumps({"jsonrpc": "2.0", "method": "public/set_heartbeat", "id": 2, "params": {"interval": 10}})
                )
            except Exception:
                pass

            # ---- Espera ACK de suscripción (hasta 5s) ----
            ack_ok: Optional[Any] = None
            ack_err: Optional[Any] = None
            ack_deadline = time.perf_counter() + 5
            while time.perf_counter() < ack_deadline and ack_ok is None and ack_err is None:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=max(0.1, ack_deadline - time.perf_counter()))
                except asyncio.TimeoutError:
                    break
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue

                # responder heartbeat
                if msg.get("method") == "heartbeat":
                    p = msg.get("params") or {}
                    if p.get("type") == "test_request":
                        await ws.send(json.dumps({"jsonrpc": "2.0", "id": 999, "method": "public/test", "params": {}}))
                    continue

                if msg.get("id") == 1 and "result" in msg:
                    ack_ok = msg["result"]
                    break
                if msg.get("id") == 1 and "error" in msg:
                    ack_err = msg["error"]
                    break

                # Si llegan eventos antes del ACK, marcamos vistos
                if msg.get("method") == "subscription":
                    ch = msg["params"]["channel"]
                    if ch.startswith("trades.") and seen["trades"] is None:
                        seen["trades"] = time.perf_counter() - t0
                    elif ch.startswith("ticker.") and seen["ticker"] is None:
                        seen["ticker"] = time.perf_counter() - t0
                    elif ch.startswith("book.") and seen["book"] is None:
                        seen["book"] = time.perf_counter() - t0
                    elif ch.startswith("markprice.options.") and seen["markprice"] is None:
                        seen["markprice"] = time.perf_counter() - t0

            if ack_ok is not None:
                subs: List[str] = []
                if isinstance(ack_ok, dict):
                    if "channels" in ack_ok and isinstance(ack_ok["channels"], list):
                        subs = ack_ok["channels"]
                    elif "subscriptions" in ack_ok and isinstance(ack_ok["subscriptions"], list):
                        subs = ack_ok["subscriptions"]
                    else:
                        subs = [k for k, v in ack_ok.items() if v is True]
                elif isinstance(ack_ok, list):
                    subs = ack_ok
                logger.info(f"[WS] subscribe ACK OK: {subs if subs else ack_ok}")
            elif ack_err is not None:
                logger.error(f"[WS] subscribe ERROR: {ack_err}")
                if ack_err.get("code") == 13778:  # raw_subscriptions_not_available_for_unauthorized
                    logger.error("Activa credenciales DERIBIT_CLIENT_ID/DERIBIT_CLIENT_SECRET o config.streams.deribit.auth para raw.")
                # no abortamos; seguimos leyendo por si otros canales funcionan

            # ---- Lector de eventos hasta timeout global ----
            async def mark(kind: str) -> None:
                if seen.get(kind) is None:
                    seen[kind] = time.perf_counter() - t0

            async def reader():
                end = time.perf_counter() + timeout
                while time.perf_counter() < end and (None in seen.values()):
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=max(0.1, end - time.perf_counter()))
                    except asyncio.TimeoutError:
                        break
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    if msg.get("method") == "heartbeat":
                        p = msg.get("params") or {}
                        if p.get("type") == "test_request":
                            await ws.send(json.dumps({"jsonrpc": "2.0", "id": 1000, "method": "public/test", "params": {}}))
                        continue

                    if msg.get("method") != "subscription":
                        continue

                    ch = msg["params"]["channel"]
                    if ch.startswith("trades."):
                        await mark("trades")
                    elif ch.startswith("ticker."):
                        await mark("ticker")
                    elif ch.startswith("book."):
                        await mark("book")
                    elif ch.startswith("markprice.options."):
                        await mark("markprice")

            await reader()

        def _ok(v: Optional[float]) -> str:
            return f"OK {v*1000:.0f}ms" if v is not None else "FAIL timeout"

        logger.info(
            f"[WS Summary] trades={_ok(seen['trades'])} "
            f"ticker={_ok(seen['ticker'])} book={_ok(seen['book'])} markprice={_ok(seen['markprice'])}"
        )

    _run_async(_run())


# ===========================================
# INGEST GROUP
# ===========================================
@cli.group("ingest")
def ingest_group() -> None:
    """Ingesta (Binance + Deribit)."""


@ingest_group.command("run")
def ingest_run() -> None:
    """WS FUTURES (trade/depth/mark/forceOrder) + REST (OI/top_traders) + WS SPOT + WS DERIBIT (si enabled)."""
    _load_env()
    cfg = load_config(str(ROOT / "config" / "config.yaml"))
    logcfg = (cfg.observability or {}).get("logging", {}) if cfg.observability else {}
    setup_logging_json(
        ROOT,
        logcfg.get("level", "INFO"),
        logcfg.get("rotation", "00:00"),
        logcfg.get("retention", "7 days"),
        logcfg.get("json", True),
    )

    async def _run() -> None:
        from oraculo.ingest.binance_ws import run_binance_ingest
        from oraculo.ingest.binance_ws_spot import run_binance_spot_ingest
        from oraculo.ingest.binance_rest import run_open_interest_poller, run_top_traders_pollers
        from oraculo.ingest.deribit_ws import DeribitRunner

        db = DB(cfg.app.storage.dsn)
        await db.connect()
        batcher = AsyncBatcher(db, flush_ms=cfg.app.storage.flush_ms, max_rows=cfg.app.storage.batch_max_rows)

        # --- Binance FUTURES WS ---
        batcher.register_stmt(
            "bfut_trades",
            "INSERT INTO binance_futures.trades(instrument_id,event_time,trade_id_ext,price,qty,side,meta)"
            " VALUES ($1,$2,$3,$4,$5,$6::side_t,$7::jsonb) ON CONFLICT DO NOTHING",
        )
        batcher.register_stmt(
            "bfut_depth",
            "INSERT INTO binance_futures.depth(instrument_id,event_time,seq,side,action,price,qty,meta)"
            " VALUES ($1,$2,$3,$4::side_t,$5::action_t,$6,$7,$8::jsonb) ON CONFLICT DO NOTHING",
        )
        batcher.register_stmt(
            "bfut_mark",
            "INSERT INTO binance_futures.mark_funding(instrument_id,event_time,mark_price,index_price,funding_rate,next_funding_time,basis_bps,meta)"
            " VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb) ON CONFLICT DO NOTHING",
        )
        batcher.register_stmt(
            "bfut_liq",
            "INSERT INTO binance_futures.liquidations(instrument_id,event_time,side,price,qty,quote_qty_usd,external_id,meta)"
            " VALUES ($1,$2,$3::side_t,$4,$5,$6,$7,$8::jsonb) ON CONFLICT DO NOTHING",
        )

        # --- Binance FUTURES REST ---
        batcher.register_stmt(
            "bfut_oi",
            "INSERT INTO binance_futures.open_interest(instrument_id,event_time,open_interest,meta)"
            " VALUES ($1,$2,$3,$4::jsonb) ON CONFLICT DO NOTHING",
        )
        batcher.register_stmt(
            "bfut_tt_acc",
            "INSERT INTO binance_futures.top_trader_account_ratio(instrument_id,event_time,long_ratio,short_ratio,meta)"
            " VALUES ($1,$2,$3,$4,$5::jsonb) ON CONFLICT DO NOTHING",
        )
        batcher.register_stmt(
            "bfut_tt_pos",
            "INSERT INTO binance_futures.top_trader_position_ratio(instrument_id,event_time,long_ratio,short_ratio,meta)"
            " VALUES ($1,$2,$3,$4,$5::jsonb) ON CONFLICT DO NOTHING",
        )

        # --- Binance SPOT WS (10 placeholders: buyer/seller) ---
        batcher.register_stmt(
            "bspot_trades",
            "INSERT INTO binance_spot.trades("
            "instrument_id,event_time,trade_id_ext,price,qty,side,is_best_match,buyer_order_id,seller_order_id,meta)"
            " VALUES ($1,$2,$3,$4,$5,$6::side_t,$7,$8,$9,$10::jsonb)"
            " ON CONFLICT DO NOTHING",
        )
        batcher.register_stmt(
            "bspot_depth",
            "INSERT INTO binance_spot.depth(instrument_id,event_time,seq,side,action,price,qty,meta)"
            " VALUES ($1,$2,$3,$4::side_t,$5::action_t,$6,$7,$8::jsonb) ON CONFLICT DO NOTHING",
        )

        # --- DERIBIT (Opciones) ---
        batcher.register_stmt(
            "deriv_trades",
            """
            INSERT INTO deribit.options_trades
              (instrument_id, event_time, trade_id_ext, price, qty, side, underlying_price, meta)
            VALUES
              ($1, to_timestamp($2/1000.0), $3, $4, $5, $6::side_t, $7, $8::jsonb)
            ON CONFLICT (instrument_id, event_time, trade_id_ext) DO NOTHING
            """
        )
        batcher.register_stmt(
            "deriv_book",
            "INSERT INTO deribit.options_book_changes(instrument_id,event_time,seq,side,action,price,qty,meta)"
            " VALUES ($1,to_timestamp($2/1000.0),$3,$4::side_t,$5::action_t,$6,$7,$8::jsonb) ON CONFLICT DO NOTHING",
        )
        batcher.register_stmt(
            "deriv_ticker",
            "INSERT INTO deribit.options_ticker(instrument_id,event_time,mark_iv,delta,gamma,vega,theta,bid,ask,underlying_price,meta)"
            " VALUES ($1,to_timestamp($2/1000.0),$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb) ON CONFLICT DO NOTHING",
        )
        batcher.register_stmt(
            "deriv_mark",
            "INSERT INTO deribit.options_mark_price(instrument_id,event_time,mark_price,underlying_price,iv_mark,meta)"
            " VALUES ($1,to_timestamp($2/1000.0),$3,$4,$5,$6::jsonb) ON CONFLICT DO NOTHING",
        )

        tasks: list[asyncio.Task] = []

        # Exporter
        if (cfg.observability or {}).get("prometheus_exporter", False):
            run_exporter(9000)
            logger.info("Prometheus exporter en :9000")

        # Heartbeats (por referencia)
        hb = getattr(cfg, "heartbeat", {}) or {}

        # Binance FUTURES WS
        tasks.append(
            asyncio.create_task(
                run_binance_ingest(
                    db,
                    batcher,
                    cfg.streams["depth_levels"],
                    cfg.streams["depth_interval_ms"],
                    hb_ref=hb.get("futures", {}),
                ),
                name="ingest-fut-ws",
            )
        )

        # Binance REST
        rest_settings = getattr(cfg, "rest_pollers", {}) or {}
        tasks += [
            asyncio.create_task(run_open_interest_poller(batcher, rest_settings, db=db), name="poll-oi"),
            asyncio.create_task(run_top_traders_pollers(batcher, rest_settings), name="poll-toptraders"),
        ]

        # Binance SPOT WS (si enabled)
        sspot0 = getattr(cfg, "streams_spot", {}) or {}
        if sspot0.get("enabled", False):
            tasks.append(
                asyncio.create_task(
                    run_binance_spot_ingest(
                        db,
                        batcher,
                        symbol=(sspot0.get("symbol") or "BTCUSDT").lower(),
                        depth_levels=int(sspot0.get("depth_levels", 20)),
                        depth_ms=int(sspot0.get("depth_interval_ms", 100)),
                        use_agg_trade=bool(sspot0.get("use_agg_trade", True)),
                        changes_queue=None,
                        hb_ref=hb.get("spot", {}),
                    ),
                    name="ingest-spot-ws",
                )
            )
            logger.info("SPOT WS habilitado.")

        # DERIBIT WS (si enabled)
        d = (getattr(cfg, "streams", {}) or {}).get("deribit", {}) or {}
        if d.get("enabled", False):
            from oraculo.ingest.deribit_ws import DeribitRunner
            runner = DeribitRunner(
                db=db,
                batcher=batcher,
                ws_url=d.get("ws_url", "wss://www.deribit.com/ws/api/v2"),
                index_name_mark=d.get("index_name_mark", "btc_usd"),
                interval_trades=d.get("interval_trades", "100ms"),
                interval_ticker=d.get("interval_ticker", "100ms"),
                interval_book=d.get("interval_book", "100ms"),
                expiries_front=int(d.get("filters", {}).get("expiries_front", 3)),
                strikes_around_atm=int(d.get("filters", {}).get("strikes_around_atm", 5)),
                hb_timeout_s=int(d.get("hb_timeout_s", 30)),
                use_trades_by_instrument=bool(d.get("use_trades_by_instrument", False)),
                auth_client_id=(d.get("auth", {}) or {}).get("client_id") or os.getenv("DERIBIT_CLIENT_ID"),
                auth_client_secret=(d.get("auth", {}) or {}).get("client_secret") or os.getenv("DERIBIT_CLIENT_SECRET"),
            )
            tasks.append(asyncio.create_task(runner.run(), name="ingest-deribit"))
            logger.info("DERIBIT WS habilitado.")

        await asyncio.gather(*tasks)

    _run_async(_run())


# ===========================================
# MAIN
# ===========================================
if __name__ == "__main__":
    cli()
