# ===============================================
# oraculo/alerts/runner.py
# ===============================================
from __future__ import annotations

import asyncio
import datetime as dt
import faulthandler
import io
import json
import math
import os
import time
import threading
from distutils.util import strtobool
from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict, Optional, Tuple, List, Sequence, Callable, Awaitable

import aiohttp
from loguru import logger

from oraculo.db import DB
from oraculo.alerts.cpu_worker import (
    CPUWorkerClient,
    DepthProcessResult,
    SnapshotProcessResult,
    TradeProcessResult,
    WorkerResult,
)
from oraculo.detect.detectors import (
    IVSpikeCfg,
    IVSpikeDetector,
    OISkewCfg,
    OISkewDetector,
    Event,
)
from oraculo.obs import metrics as obs_metrics
from oraculo.rules.engine import eval_rules, RuleContext
from oraculo.rules.router import TelegramRouter


BINANCE_FUT_INST = "BINANCE:PERP:BTCUSDT"


@dataclass
class TradeEvent:
    ts: float
    side: str
    price: float
    qty: float


@dataclass
class DepthEvent:
    ts: float
    side: str
    action: str
    price: float
    qty: float


@dataclass
class MarkEvent:
    ts: float
    mark_price: Optional[float]
    index_price: Optional[float]


@dataclass
class QueuedEvent:
    stream: str
    event: Any
    enqueued_at: float


@dataclass
class DBRequest:
    kind: str
    payload: Any
    future: Optional[asyncio.Future]
    enqueued_at: float

class EventSource:
    async def start(self) -> None:  # pragma: no cover - interfaz
        raise NotImplementedError

    async def stop(self) -> None:  # pragma: no cover - interfaz
        raise NotImplementedError

    def __aiter__(self) -> AsyncIterator[Any]:  # pragma: no cover - interfaz
        raise NotImplementedError

# --- Normalización de severidad hacia el enum de BD (ALTA/MEDIA/BAJA)
_SEV_MAP = {
    "HIGH": "ALTA",
    "MEDIUM": "MEDIA",
    "LOW": "BAJA",
    "ALTA": "ALTA",
    "MEDIA": "MEDIA",
    "BAJA": "BAJA",
}
def _sev_norm(x: str) -> str:
    return _SEV_MAP.get(str(x).strip().upper(), "MEDIA")


def _parse_deribit_option(instrument_id: str) -> tuple[Optional[str], Optional[str]]:
    """
    Extrae (underlying, tipo) de un instrument_id canónico de Deribit OPTIONS.
    Ejemplo esperado:
      DERIBIT:OPTIONS:BTC-28NOV25-50000-C -> ("BTC", "C")
    """
    s = str(instrument_id)
    if s.endswith("-C"):
        opt_type = "C"
    elif s.endswith("-P"):
        opt_type = "P"
    else:
        return None, None

    tail = s.split(":")[-1]  # BTC-28NOV25-50000-C
    parts = tail.split("-")
    if not parts:
        return None, None
    underlying = parts[0]
    return underlying, opt_type


# ----------------- Auxiliares -----------------

class DBTail:
    """
    Clase para seguir la cola de una tabla (tailing) usando una columna cursor (ID o Tiempo).
    Evita perder filas con el mismo timestamp usando IDs únicos cuando es posible.
    """
    def __init__(self, db: DB, table: str, id_col: str, default_val: Any) -> None:
        self.db = db
        self.table = table
        self.id_col = id_col
        self.last_val: Any = default_val  # Valor del cursor (int o datetime)

    async def init_live(self, instrument_id: str) -> None:
        """Inicializa el cursor al valor MÁS ALTO actual para empezar en modo LIVE."""
        sql = f"SELECT MAX({self.id_col}) FROM {self.table} WHERE instrument_id=$1"
        val = await self.db.fetchval(sql, instrument_id)
        if val is not None:
            self.last_val = val
            logger.info(f"[{self.table}] Tail initialized LIVE at {self.id_col}={self.last_val}")
        else:
            logger.info(f"[{self.table}] Table empty or no data for {instrument_id}, starting from default.")

    async def fetch_new(self, instrument_id: str, limit: int = 2000) -> list[dict]:
        """Recupera filas nuevas donde id_col > last_val."""
        sql = f"""SELECT * FROM {self.table}
                  WHERE instrument_id=$1 AND {self.id_col} > $2
                  ORDER BY {self.id_col} ASC
                  LIMIT {limit}"""
        
        rows = await self.db.fetch(sql, instrument_id, self.last_val)
        
        if rows:
            # Actualizamos el cursor al último procesado
            self.last_val = rows[-1][self.id_col]
            
        return [dict(r) for r in rows]


class WsReader(EventSource):
    """Lectura WS con colas por stream y backpressure temprano."""

    def __init__(
        self,
        depth_levels: int = 20,
        depth_ms: int = 100,
        symbol: str = "btcusdt",
        *,
        stream_cfg: Optional[Dict[str, Dict[str, float]]] = None,
    ) -> None:
        self._depth_levels = depth_levels
        self._depth_ms = depth_ms
        self._symbol = symbol.lower()
        self._stream_cfg = stream_cfg or {
            "trade": {"maxsize": 5_000, "backpressure": 0.45, "stale_after": 2.0},
            "depth": {"maxsize": 10_000, "backpressure": 0.5, "stale_after": 2.0},
            "mark": {"maxsize": 1_000, "backpressure": 0.4, "stale_after": 1.0},
        }
        self._queues: Dict[str, asyncio.Queue[QueuedEvent]] = {
            name: asyncio.Queue(maxsize=int(cfg.get("maxsize", 1_000)))
            for name, cfg in self._stream_cfg.items()
        }
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._mgr: Optional[Any] = None
        self._last_msg_ts: Dict[str, float] = {"trade": 0.0, "depth": 0.0, "mark": 0.0}
        self._drop_log_window_s = 5.0
        self._last_drop_log_ts: Dict[str, float] = {"trade": 0.0, "depth": 0.0, "mark": 0.0}

    @property
    def queues(self) -> Dict[str, asyncio.Queue[QueuedEvent]]:
        return self._queues

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run(), name="alerts-ws-reader")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except Exception:
                pass
        if self._mgr:
            try:
                self._mgr.stop_manager()
            except Exception:
                pass

    async def get_batch(
        self, stream: str, *, max_items: int = 300, max_wait_s: float = 0.02
    ) -> list[QueuedEvent]:
        queue = self._queues.get(stream)
        if queue is None:
            return []

        try:
            first = await asyncio.wait_for(queue.get(), timeout=max_wait_s)
        except asyncio.TimeoutError:
            return []

        batch = [first]
        deadline = asyncio.get_event_loop().time() + max_wait_s
        while len(batch) < max_items:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                break
            try:
                ev = await asyncio.wait_for(queue.get(), timeout=remaining)
            except asyncio.TimeoutError:
                break
            batch.append(ev)

        self._set_queue_depth_metrics()
        return batch

    def _set_queue_depth_metrics(self) -> None:
        total = 0
        for name, queue in self._queues.items():
            size = queue.qsize()
            total += size
            obs_metrics.alerts_queue_depth.labels(stream=name).set(size)
        obs_metrics.alerts_queue_depth.labels(stream="all").set(total)

    def _observe_last_msg_age(self) -> None:
        now = asyncio.get_event_loop().time()
        for k, ts in self._last_msg_ts.items():
            if ts <= 0:
                continue
            obs_metrics.ws_last_msg_age_s.labels(venue="futures", stream=k).set(
                max(0.0, now - ts)
            )

    def _apply_backpressure(self, stream: str) -> None:
        queue = self._queues.get(stream)
        cfg = self._stream_cfg.get(stream, {})
        if not queue or queue.maxsize <= 0:
            return

        ratio = queue.qsize() / float(queue.maxsize)
        threshold = float(cfg.get("backpressure", 0.5))
        if ratio < threshold:
            return

        target = max(0, int(queue.maxsize * threshold * 0.9))
        dropped = 0
        while queue.qsize() > target:
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            else:
                dropped += 1
        if dropped:
            obs_metrics.alerts_queue_discarded_total.labels(
                stream=stream, cause="backlog_drop"
            ).inc(dropped)
            obs_metrics.alerts_queue_backpressure_skipped_total.labels(kind=stream).inc(
                dropped
            )
            obs_metrics.alerts_queue_dropped_total.inc(dropped)
            self._log_drops_if_needed(stream, dropped, "backlog")

    def _log_drops_if_needed(self, stream: str, dropped: int, cause: str) -> None:
        now = asyncio.get_event_loop().time()
        last = self._last_drop_log_ts.get(stream, 0.0)
        if (now - last) < self._drop_log_window_s:
            return
        logger.warning(
            "[alerts-ws] queue '{}' dropping {} events due to {} (depth={}/{})",
            stream,
            dropped,
            cause,
            self._queues.get(stream).qsize() if self._queues.get(stream) else 0,
            self._queues.get(stream).maxsize if self._queues.get(stream) else 0,
        )
        self._last_drop_log_ts[stream] = now

    def _publish(self, stream: str, ev: Any) -> None:
        queue = self._queues.get(stream)
        if queue is None:
            return

        self._apply_backpressure(stream)
        try:
            queue.put_nowait(QueuedEvent(stream=stream, event=ev, enqueued_at=time.perf_counter()))
        except asyncio.QueueFull:
            obs_metrics.alerts_queue_discarded_total.labels(
                stream=stream, cause="full_drop"
            ).inc()
            obs_metrics.alerts_queue_dropped_total.inc()
            self._log_drops_if_needed(stream, 1, "full")
        self._set_queue_depth_metrics()

    async def _run(self) -> None:
        try:
            try:
                from unicorn_binance_websocket_api import BinanceWebSocketApiManager  # type: ignore
            except Exception:
                from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import (  # type: ignore
                    BinanceWebSocketApiManager,
                )

            self._mgr = BinanceWebSocketApiManager(exchange="binance.com-futures")
            depth_channel = f"depth{self._depth_levels}@{self._depth_ms}ms"
            self._mgr.create_stream(["trade"], [self._symbol])
            self._mgr.create_stream([depth_channel], [self._symbol])
            self._mgr.create_stream(["markPrice@1s"], [self._symbol])
            obs_metrics.ws_reconnects_total.labels("futures", "trade").inc()
            obs_metrics.ws_reconnects_total.labels("futures", "depth").inc()
            obs_metrics.ws_reconnects_total.labels("futures", "mark").inc()
            logger.info(
                f"[alerts-ws] Streams up: trade, {depth_channel}, markPrice@1s"
            )
        except Exception as e:
            logger.error(f"[alerts-ws] failed to start WS manager: {e!s}")
            self._running = False
            return

        while self._running:
            raw = self._mgr.pop_stream_data_from_stream_buffer() if self._mgr else None
            if raw is None:
                await asyncio.sleep(0.01)
                self._observe_last_msg_age()
                continue
            try:
                msg = json.loads(raw) if isinstance(raw, str) else raw
            except Exception:
                continue
            data = msg.get("data", msg)
            etype = data.get("e")
            now = asyncio.get_event_loop().time()
            if etype == "trade":
                obs_metrics.ws_msgs_total.labels("trade").inc()
                self._last_msg_ts["trade"] = now
                ev = TradeEvent(
                    ts=float(data.get("T", 0)) / 1000.0,
                    side="buy" if str(data.get("m")).lower() == "false" else "sell",
                    price=float(data.get("p", 0)),
                    qty=float(data.get("q", 0)),
                )
                self._publish("trade", ev)
            elif etype == "depthUpdate":
                obs_metrics.ws_msgs_total.labels("depth").inc()
                self._last_msg_ts["depth"] = now
                ts = float(data.get("E", 0)) / 1000.0
                for p, q in data.get("b", []):
                    ev = DepthEvent(ts=ts, side="buy", action="insert" if float(q) > 0 else "delete", price=float(p), qty=float(q))
                    self._publish("depth", ev)
                for p, q in data.get("a", []):
                    ev = DepthEvent(ts=ts, side="sell", action="insert" if float(q) > 0 else "delete", price=float(p), qty=float(q))
                    self._publish("depth", ev)
            elif etype in ("markPriceUpdate", "24hrMiniTicker", "24hrTicker"):
                obs_metrics.ws_msgs_total.labels("mark").inc()
                self._last_msg_ts["mark"] = now
                ev = MarkEvent(
                    ts=float(data.get("E", 0)) / 1000.0,
                    mark_price=float(data.get("p") or data.get("c") or 0),
                    index_price=float(data.get("i") or data.get("P") or data.get("i")),
                )
                self._publish("mark", ev)
            self._observe_last_msg_age()

# ----------------- Persistencia asíncrona -----------------
class DBWriter:
    """Canaliza escrituras hacia la base de datos en un solo punto."""

    def __init__(
        self,
        db: DB,
        telemetry: Telemetry,
        *,
        instrument_id: str,
        profile: str,
        flush_interval: float = 0.05,
        max_batch: int = 200,
        max_queue: int = 10_000,
        service_name: str = "alerts",
        exported_service: str | None = None,
    ) -> None:
        self.db = db
        self.telemetry = telemetry
        self.instrument_id = instrument_id
        self.profile = profile
        self.flush_interval = flush_interval
        self.max_batch = max_batch
        self.queue: asyncio.Queue[DBRequest] = asyncio.Queue(maxsize=max_queue)
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self.service_name = service_name
        self.exported_service = exported_service or service_name
        self._metric_labels: dict[str, str] = {
            "service": self.service_name,
            "exported_service": self.exported_service,
        }

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run(), name="alerts-db-writer")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except Exception:
                pass

    async def submit(self, kind: str, payload: Any, *, expect_result: bool = False) -> Any:
        loop = asyncio.get_event_loop()
        future: Optional[asyncio.Future] = loop.create_future() if expect_result else None
        req = DBRequest(kind=kind, payload=payload, future=future, enqueued_at=time.perf_counter())
        try:
            self.queue.put_nowait(req)
        except asyncio.QueueFull:
            obs_metrics.alerts_queue_discarded_total.labels(stream="db_writer", cause="full_drop").inc()
            obs_metrics.alerts_queue_dropped_total.inc()
            if future is not None:
                future.set_exception(asyncio.QueueFull("db_writer queue full"))
                return await future
            return None
        return await future if future is not None else None

    async def _run(self) -> None:
        try:
            while self._running:
                batch = await self._drain_batch()
                if not batch:
                    continue
                await self._process_batch(batch)
        except asyncio.CancelledError:
            pass
        except Exception:
            obs_metrics.alerts_uncaught_errors_total.labels(
                **self._error_labels("db_writer_loop")
            ).inc()
            logger.exception("[db-writer] loop crashed")

    async def _drain_batch(self) -> list[DBRequest]:
        batch: list[DBRequest] = []
        try:
            first = await asyncio.wait_for(self.queue.get(), timeout=self.flush_interval)
        except asyncio.TimeoutError:
            return batch
        batch.append(first)

        deadline = time.perf_counter() + self.flush_interval
        while len(batch) < self.max_batch:
            remaining = deadline - time.perf_counter()
            if remaining <= 0:
                break
            try:
                req = await asyncio.wait_for(self.queue.get(), timeout=remaining)
            except asyncio.TimeoutError:
                break
            batch.append(req)
        return batch

    async def _process_batch(self, batch: list[DBRequest]) -> None:
        start_batch_ts = time.perf_counter()
        if batch:
            obs_metrics.alerts_db_queue_time_seconds.observe(start_batch_ts - batch[0].enqueued_at)
            obs_metrics.alerts_db_queue_depth.set(self.queue.qsize())
        obs_metrics.alerts_db_inflight_batches.inc()
        slice_events: list[Event] = []
        metric_rows: List[Tuple[str, dt.datetime, int, str, float, Optional[str], str]] = []
        telemetry_flushes = 0
        upserts: list[DBRequest] = []

        for req in batch:
            if req.kind == "insert_slice":
                slice_events.append(req.payload)
            elif req.kind == "insert_metrics":
                metric_rows.extend(req.payload or [])
            elif req.kind == "telemetry_flush":
                telemetry_flushes += 1
            elif req.kind == "upsert_rule":
                upserts.append(req)
            else:
                if req.future and not req.future.done():
                    req.future.set_result(None)

        try:
            if slice_events:
                t0 = time.perf_counter()
                await self._insert_slice_bulk(slice_events)
                obs_metrics.alerts_db_batch_duration_seconds.labels(kind="slice").observe(
                    time.perf_counter() - t0
                )

            if metric_rows:
                t0 = time.perf_counter()
                await self._insert_metrics(metric_rows)
                obs_metrics.alerts_db_batch_duration_seconds.labels(kind="metrics").observe(
                    time.perf_counter() - t0
                )

            for _ in range(telemetry_flushes):
                t0 = time.perf_counter()
                await self.telemetry.flush_if_needed()
                obs_metrics.alerts_db_batch_duration_seconds.labels(kind="telemetry").observe(
                    time.perf_counter() - t0
                )

            for req in upserts:
                try:
                    t0 = time.perf_counter()
                    res = await self._upsert_rule(req.payload)
                    obs_metrics.alerts_db_batch_duration_seconds.labels(kind="upsert_rule").observe(
                        time.perf_counter() - t0
                    )
                    if req.future and not req.future.done():
                        req.future.set_result(res)
                except Exception as e:
                    logger.error(f"[db-writer] upsert_rule failed: {e!s}")
                    if req.future and not req.future.done():
                        req.future.set_exception(e)
        except Exception:
            obs_metrics.alerts_uncaught_errors_total.labels(
                **self._error_labels("db_flush")
            ).inc()
            logger.exception("[db-writer] batch processing failed")
        finally:
            obs_metrics.alerts_db_inflight_batches.dec()

    async def _insert_slice_bulk(self, events: list[Event]) -> None:
        sql = (
            "INSERT INTO oraculo.slice_events("
            " instrument_id,event_time,event_type,side,intensity,price,duration_ms,fields,latency_ms,profile)"
            " VALUES ($1,$2,$3,$4::side_t,$5,$6,$7,$8::jsonb,$9,$10)"
            " ON CONFLICT DO NOTHING"
        )
        rows = [
            (
                self.instrument_id,
                dt.datetime.fromtimestamp(ev.ts, tz=dt.timezone.utc),
                ev.kind,
                ev.side,
                ev.intensity,
                ev.price,
                int((getattr(ev, "fields", {}) or {}).get("dur_s", 0) * 1000),
                json.dumps(getattr(ev, "fields", {}) or {}),
                None,
                self.profile,
            )
            for ev in events
        ]
        try:
            await self.db.execute_many(sql, rows)
        except Exception as e:
            logger.warning(f"[db-writer] bulk insert_slice failed ({e!s}); fallback row-by-row")
            for row in rows:
                try:
                    await self.db.execute(sql, *row)
                except Exception as e2:
                    logger.error(f"[db-writer] drop slice row {row}: {e2!s}")

    async def _insert_metrics(
        self, rows: Sequence[Tuple[str, dt.datetime, int, str, float, Optional[str], str]]
    ) -> None:
        if not rows:
            return
        sql = (
            "INSERT INTO oraculo.metrics_series("
            " instrument_id,event_time,window_s,metric,value,profile,meta"
            ") VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb)"
            " ON CONFLICT DO NOTHING"
        )
        try:
            await self.db.execute_many(sql, rows)
        except Exception as e:
            logger.warning(f"[metrics] bulk insert failed ({e!s}); fallback row-by-row")
            for r in rows:
                try:
                    await self.db.execute(sql, *r)
                except Exception as e2:
                    logger.error(f"[metrics] drop row {r}: {e2!s}")

    async def _upsert_rule(self, payload: Tuple[dict, float]):
        rule, event_ts = payload
        _observe_rule_event(rule, event_ts)
        t0 = time.perf_counter()
        try:
            sev_db = _sev_norm(rule["severity"])
            alert_id = await self.db.fetchval(
                """
                SELECT oraculo.upsert_rule_alert(
                  $1, $2, $3, $4::severity_t, $5, $6::jsonb, $7, $8
                ) AS id
            """,
                self.instrument_id,
                dt.datetime.fromtimestamp(event_ts, tz=dt.timezone.utc),
                rule["rule"],
                sev_db,
                rule["dedup_key"],
                json.dumps(rule["context"] or {}),
                rule.get("suppress_window_s"),
                self.profile,
            )
            if alert_id is None:
                obs_metrics.rule_alerts_no_id_total.labels(
                    rule=rule.get("rule", "unknown"), reason="null_from_db"
                ).inc()
                logger.warning(
                    "[db-writer] upsert_rule_alert returned NULL (rule=%s side=%s dedup_key=%s severity=%s profile=%s)",
                    rule.get("rule"),
                    rule.get("side"),
                    rule.get("dedup_key"),
                    sev_db,
                    self.profile,
                )
                return None, None

            ts_first = await self.db.fetchval(
                "SELECT ts_first FROM oraculo.rule_alerts WHERE id=$1", alert_id
            )
            if ts_first is None:
                obs_metrics.rule_alerts_unreadable_total.labels(
                    rule=rule.get("rule", "unknown"), reason="ts_first_missing"
                ).inc()
                logger.warning(
                    "[db-writer] ts_first not readable for alert_id=%s (rule=%s dedup_key=%s profile=%s)",
                    alert_id,
                    rule.get("rule"),
                    rule.get("dedup_key"),
                    self.profile,
                )
            obs_metrics.rule_alerts_upsert_total.labels(
                rule=rule["rule"], severity=sev_db
            ).inc()
            return int(alert_id), ts_first
        except Exception as e:
            logger.error(f"upsert_rule_alert failed: {e!s}")
            return None, None
        finally:
            obs_metrics.db_upsert_rule_alert_ms.observe((time.perf_counter() - t0) * 1000)

    def _error_labels(self, where: str) -> dict[str, str]:
        return {**self._metric_labels, "where": where}


async def fetch_orderbook_snapshot(
    session: aiohttp.ClientSession, symbol: str = "BTCUSDT", depth: int = 1000
) -> tuple[list[tuple[float, float]], list[tuple[float, float]]]:
    url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit={depth}"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
        r.raise_for_status()
        d = await r.json()
        bids = [(float(p), float(q)) for p, q in d.get("bids", [])]
        asks = [(float(p), float(q)) for p, q in d.get("asks", [])]
        return bids, asks


def _evdict(ev: Event) -> Dict[str, Any]:
    return {
        "type": getattr(ev, "kind", None),
        "side": getattr(ev, "side", None),
        "price": getattr(ev, "price", None),
        "intensity": getattr(ev, "intensity", None),
        "fields": (getattr(ev, "fields", {}) or {}),
        "ts": getattr(ev, "ts", None),
    }


def _routing_to_dict(routing_cfg: Any) -> Dict[str, Any]:
    """Acepta pydantic u objeto similar y devuelve un dict plano."""
    if isinstance(routing_cfg, dict):
        return routing_cfg
    if hasattr(routing_cfg, "model_dump"):
        return routing_cfg.model_dump()
    if hasattr(routing_cfg, "dict"):
        return routing_cfg.dict()
    # fallback conservador
    return {"telegram": {}}


def _observe_rule_event(rule: dict, event_ts: float) -> None:
    try:
        rule_code = rule.get("rule") or "unknown"
        event_ts_epoch = float(event_ts)
        now = time.time()
        lag = max(0.0, now - event_ts_epoch)
        obs_metrics.rule_alert_lag_seconds.labels(rule=rule_code).observe(lag)
        obs_metrics.rule_event_age_seconds.labels(rule=rule_code).set(lag)
        obs_metrics.rule_watermark_event_time.labels(rule=rule_code).set(event_ts_epoch)
    except Exception:
        logger.debug("[metrics] failed to observe rule event", exc_info=True)


async def dispatch_macro_event(
    ev: Event,
    ctx: RuleContext,
    enqueue_rule: Callable[[dict, float], Awaitable[tuple[Optional[int], Optional[dt.datetime]]]],
    telemetry: Any,
    router: Any,
) -> None:
    for rule in eval_rules(_evdict(ev), ctx):
        aid, t0_dt = await enqueue_rule(rule, ev.ts)
        if aid is None:
            continue
        telemetry.bump(ev.ts, rule["rule"], rule.get("side", "na"), emitted=1)
        fields = ev.fields or {}
        text = f"#{rule['rule']} {ev.kind} {rule.get('side', 'na').upper()} | i={ev.intensity:.2f}"
        if ev.kind == "oi_spike":
            oi_val = fields.get("oi_delta_pct")
            mom_val = fields.get("momentum_usd")
            try:
                oi_str = f"{float(oi_val):.2f}%"
            except Exception:
                oi_str = "na"
            try:
                mom_str = f"{float(mom_val):.2f} USD"
            except Exception:
                mom_str = "na"
            try:
                obs_metrics.oi_spike_events_total.labels(
                    side=rule.get("side", "na"), source_oi=str(fields.get("metric_used_oi") or "na")
                ).inc()
                if oi_val is not None:
                    obs_metrics.oi_spike_last_oi_delta_pct.labels(instrument_id=ctx.instrument_id).set(float(oi_val))
                if mom_val is not None:
                    obs_metrics.oi_spike_last_momentum_usd.labels(instrument_id=ctx.instrument_id).set(float(mom_val))
            except Exception:
                logger.debug("[metrics] failed to observe oi_spike metrics", exc_info=True)
            text = (
                f"#{rule['rule']} OI spike {rule.get('side', 'na').upper()} | "
                f"oi={oi_str} "
                f"mom={mom_str} "
                f"i={ev.intensity:.2f}"
            )
        elif ev.kind == "liq_cluster":
            liq_sum_usd = fields.get("liq_sum_usd")
            liq_orders = fields.get("liq_orders")
            price_max = fields.get("price_max")
            price_min = fields.get("price_min")
            try:
                liq_sum = f"{float(liq_sum_usd):.0f} USD"
            except Exception:
                liq_sum = "na"
            try:
                liq_ord_str = f"k={int(liq_orders)}"
            except Exception:
                liq_ord_str = "k=na"
            try:
                price_span = f"[{float(price_min):.1f}, {float(price_max):.1f}]"
            except Exception:
                price_span = "[na, na]"
            text = (
                f"#{rule['rule']} liq_cluster {rule.get('side', 'na').upper()} | "
                f"{liq_sum} "
                f"{liq_ord_str} "
                f"{price_span} "
                f"i={ev.intensity:.2f}"
            )
        elif ev.kind == "top_traders":
            acc_lr = fields.get("acc_long_ratio")
            acc_sr = fields.get("acc_short_ratio")
            pos_lr = fields.get("pos_long_ratio")
            pos_sr = fields.get("pos_short_ratio")
            text = (
                f"#{rule['rule']} top_traders {rule.get('side', 'na').upper()} | "
                f"acc=({acc_lr},{acc_sr}) "
                f"pos=({pos_lr},{pos_sr}) "
                f"i={ev.intensity:.2f}"
            )
        elif ev.kind == "basis_dislocation":
            basis = fields.get("basis_bps")
            vel = fields.get("basis_vel_bps_s")
            ftrend = fields.get("funding_trend")
            metric_used_basis = fields.get("metric_used_basis")
            metric_used_vel = fields.get("metric_used_vel")
            text = (
                f"#{rule['rule']} basis_dislocation | "
                f"basis={basis} vel={vel} "
                f"f_trend={ftrend} "
                f"mb={metric_used_basis} mv={metric_used_vel} "
                f"i={ev.intensity:.2f}"
            )
        elif ev.kind == "skew_shock":
            delta = fields.get("rr_delta")
            vel = fields.get("rr_vel_per_s")
            side_macro = ev.side or rule.get("side", "na")
            try:
                delta_str = f"{float(delta):.4f}"
            except Exception:
                delta_str = "na"
            try:
                vel_str = f"{float(vel):.6f}/s"
            except Exception:
                vel_str = "na/s"
            text = (
                f"[{rule['rule']}] Skew shock (25Δ) {side_macro} "
                f"ΔRR={delta_str}, vel={vel_str}, i={ev.intensity:.2f}"
            )
        await router.send("rules", text, alert_id=aid, ts_first=t0_dt)

# ---- Telemetría (agregada y volcada a tabla oraculo.rule_telemetry) ----
class Telemetry:
    def __init__(self, db: DB, instrument_id: str, profile: str):
        self.db = db
        self.instrument_id = instrument_id
        self.profile = profile
        self._agg: Dict[Tuple[dt.datetime, str, str], Dict[str, int]] = {}
        self._last_flush = 0.0

    @staticmethod
    def _bucket(ts: float) -> dt.datetime:
        dt_ = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)
        return dt_.replace(second=0, microsecond=0)

    def bump(
        self,
        ts: float,
        rule: str,
        side: str,
        *,
        emitted: int = 0,
        disc_dom_spread: int = 0,
        disc_metrics_none: int = 0,
        disc_iv_missing: int = 0,
        disc_oi_missing: int = 0,
        disc_oi_low: int = 0,
        disc_basis_vel_low: int = 0,
        disc_dep_low: int = 0,
        disc_refill_high: int = 0,
        disc_top_levels_gate: int = 0,
    ) -> None:
        key = (self._bucket(ts), rule, side)
        d = self._agg.setdefault(
            key,
            {
                "emitted": 0,
                "disc_dom_spread": 0,
                "disc_metrics_none": 0,
                "disc_iv_missing": 0,
                "disc_oi_missing": 0,
                "disc_oi_low": 0,
                "disc_basis_vel_low": 0,
                "disc_dep_low": 0,
                "disc_refill_high": 0,
                "disc_top_levels_gate": 0,
            },
        )
        d["emitted"] += emitted
        d["disc_dom_spread"] += disc_dom_spread
        d["disc_metrics_none"] += disc_metrics_none
        d["disc_iv_missing"] += disc_iv_missing
        d["disc_oi_missing"] += disc_oi_missing
        d["disc_oi_low"] += disc_oi_low
        d["disc_basis_vel_low"] += disc_basis_vel_low
        d["disc_dep_low"] += disc_dep_low
        d["disc_refill_high"] += disc_refill_high
        d["disc_top_levels_gate"] += disc_top_levels_gate

    async def flush_if_needed(self) -> None:
        now = dt.datetime.now(dt.timezone.utc).timestamp()
        if (now - self._last_flush) < 15.0:
            return
        self._last_flush = now
        if not self._agg:
            return

        rows: List[
            Tuple[
                dt.datetime,
                str,
                str,
                str,
                str,
                int,
                int,
                int,
                int,
                int,
                int,
                int,
                int,
                int,
                int,
            ]
        ] = []
        for (ts_bucket, rule, side), c in list(self._agg.items()):
            rows.append(
                (
                    ts_bucket,
                    self.instrument_id,
                    self.profile,
                    rule,
                    side,
                    c["emitted"],
                    c["disc_dom_spread"],
                    c["disc_metrics_none"],
                    c["disc_iv_missing"],
                    c["disc_oi_missing"],
                    c["disc_oi_low"],
                    c["disc_basis_vel_low"],
                    c["disc_dep_low"],
                    c["disc_refill_high"],
                    c["disc_top_levels_gate"],
                )
            )
        sql = """
            INSERT INTO oraculo.rule_telemetry(
              ts_bucket, instrument_id, profile, rule, side,
              emitted, disc_dom_spread, disc_metrics_none,
              disc_iv_missing, disc_oi_missing, disc_oi_low,
              disc_basis_vel_low, disc_dep_low, disc_refill_high,
              disc_top_levels_gate
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
            ON CONFLICT (ts_bucket, instrument_id, profile, rule, side)
            DO UPDATE SET
              emitted = oraculo.rule_telemetry.emitted + EXCLUDED.emitted,
              disc_dom_spread = oraculo.rule_telemetry.disc_dom_spread + EXCLUDED.disc_dom_spread,
              disc_metrics_none = oraculo.rule_telemetry.disc_metrics_none + EXCLUDED.disc_metrics_none,
              disc_iv_missing = oraculo.rule_telemetry.disc_iv_missing + EXCLUDED.disc_iv_missing,
              disc_oi_missing = oraculo.rule_telemetry.disc_oi_missing + EXCLUDED.disc_oi_missing,
              disc_oi_low = oraculo.rule_telemetry.disc_oi_low + EXCLUDED.disc_oi_low,
              disc_basis_vel_low = oraculo.rule_telemetry.disc_basis_vel_low + EXCLUDED.disc_basis_vel_low,
              disc_dep_low = oraculo.rule_telemetry.disc_dep_low + EXCLUDED.disc_dep_low,
              disc_refill_high = oraculo.rule_telemetry.disc_refill_high + EXCLUDED.disc_refill_high,
              disc_top_levels_gate = oraculo.rule_telemetry.disc_top_levels_gate + EXCLUDED.disc_top_levels_gate
        """
        try:
            await self.db.execute_many(sql, rows)
            self._agg.clear()
        except Exception as e:
            logger.warning(f"[telemetry] flush failed: {e!s}")


# ----------------- Runner principal -----------------
async def run_pipeline(
    db: DB,
    routing_cfg: Dict[str, Any],
    rules_profile: str = "EU",
    cfg_mgr: Any | None = None,
) -> None:
    alerts_source = "ws"
    depth_levels = 20
    depth_ms = 100
    symbol = "BTCUSDT"
    service_name = "alerts"
    exported_service = os.getenv("ORACULO_EXPORTED_SERVICE", service_name)
    background_tasks: list[asyncio.Task] = []
    loop = asyncio.get_running_loop()
    cpu_sampler_task: asyncio.Task | None = None
    if cfg_mgr is not None and getattr(cfg_mgr, "cfg", None) is not None:
        try:
            cfg_obj = cfg_mgr.cfg
            alerts_cfg = getattr(cfg_obj, "alerts", None) or getattr(cfg_obj, "model_extra", {}).get("alerts", {}) or {}
            alerts_source = str(alerts_cfg.get("source", alerts_source)).lower()
            binance_cfg = getattr(cfg_obj, "binance", {}) or {}
            depth_levels = int(alerts_cfg.get("depth_levels", binance_cfg.get("futures_depth_levels", depth_levels)))
            depth_ms = int(alerts_cfg.get("depth_ms", binance_cfg.get("futures_depth_ms", depth_ms)))
            symbol = getattr(cfg_obj.app, "symbol", symbol)
        except Exception:
            pass

    cpu_sampler_task = asyncio.create_task(
        obs_metrics.cpu_sampler_loop(
            service=service_name, exported_service=exported_service, interval_s=1.0
        ),
        name="alerts-cpu-monitor",
    )
    background_tasks.append(cpu_sampler_task)

    background_tasks.append(
        asyncio.create_task(
            obs_metrics.thread_metrics_sampler(
                service=service_name, exported_service=exported_service, interval_s=2.5
            ),
            name="alerts-thread-metrics",
        )
    )
    background_tasks.append(
        asyncio.create_task(
            obs_metrics.executor_sampler_loop(
                service=service_name, exported_service=exported_service, interval_s=1.0
            ),
            name="alerts-executor-metrics",
        )
    )

    def _default_executor_queue_depth() -> int:
        executor = getattr(loop, "_default_executor", None)
        if executor is None:
            return 0
        queue = getattr(executor, "_work_queue", None)
        if queue is None:
            return 0
        try:
            return int(queue.qsize())
        except Exception:
            return 0

    def _env_bool(name: str, default: bool) -> bool:
        val = os.getenv(name)
        if val is None:
            return default
        try:
            return bool(strtobool(val))
        except Exception:
            return default

    lag_spike_thresholds = (0.5, 2.0, 10.0)
    lag_dump_threshold = float(os.getenv("ORACULO_WATCHDOG_LAG_THRESHOLD", "2.0"))
    lag_immediate_threshold = float(os.getenv("ORACULO_WATCHDOG_LAG_IMMEDIATE_THRESHOLD", "10.0"))
    lag_consecutive_required = int(os.getenv("ORACULO_WATCHDOG_CONSECUTIVE", "3"))
    executor_queue_dump_threshold = int(os.getenv("ORACULO_WATCHDOG_QUEUE_THRESHOLD", "50"))
    dump_cooldown_s = float(os.getenv("ORACULO_WATCHDOG_COOLDOWN_S", "30.0"))
    watchdog_enabled = _env_bool("ORACULO_WATCHDOG_ENABLED", True)
    consecutive_high_lag = 0
    last_dump_ts = 0.0
    last_wall = time.perf_counter()
    prev_proc_cpu = time.process_time()
    try:
        prev_thread_cpu = time.thread_time()
    except Exception:
        prev_thread_cpu = prev_proc_cpu

    logger.warning(
        "[alerts][watchdog] enabled=%s lag>=%.3fs lag_immediate>=%.3fs consecutive=%s queue>=%s cooldown=%.1fs",
        watchdog_enabled,
        lag_dump_threshold,
        lag_immediate_threshold,
        lag_consecutive_required,
        executor_queue_dump_threshold,
        dump_cooldown_s,
    )

    def _emit_watchdog_dump(reason: str, lag: float, queue_depth: int, proc_cores: float, main_share: float, threads_total: int) -> None:
        logger.warning(
            "[alerts][watchdog] DUMP_TRIGGERED reason=%s lag=%.3f queue_depth=%s cooldown=%.1fs proc_cores=%.3f main_thread_share=%.3f threads_total=%s",
            reason,
            lag,
            queue_depth,
            dump_cooldown_s,
            proc_cores,
            main_share,
            threads_total,
        )
        buf = io.StringIO()
        try:
            faulthandler.dump_traceback(file=buf, all_threads=True)
            dump_txt = buf.getvalue()
        except Exception as exc:  # pragma: no cover - best effort
            dump_txt = f"failed to capture dump: {exc!s}"
        finally:
            try:
                buf.close()
            except Exception:
                pass

        obs_metrics.dumps_total.labels(service=service_name, reason=reason).inc()
        obs_metrics.last_dump_timestamp_seconds.labels(service=service_name).set(time.time())
        obs_metrics.last_dump_lag_seconds.labels(service=service_name, reason=reason).set(lag)
        logger.warning(
            "[alerts][watchdog] dump reason=%s lag=%.3f queue_depth=%s proc_cores=%.3f main_thread_share=%.3f threads_total=%s\n%s",
            reason,
            lag,
            queue_depth,
            proc_cores,
            main_share,
            threads_total,
            dump_txt,
        )

    def _on_lag_sample(lag: float) -> None:
        nonlocal consecutive_high_lag, last_dump_ts, last_wall, prev_proc_cpu, prev_thread_cpu

        if not watchdog_enabled:
            return

        wall_now = time.perf_counter()
        proc_now = time.process_time()
        try:
            thread_now = time.thread_time()
        except Exception:
            thread_now = proc_now

        dt_wall = wall_now - last_wall
        proc_delta = proc_now - prev_proc_cpu
        thread_delta = thread_now - prev_thread_cpu

        last_wall = wall_now
        prev_proc_cpu = proc_now
        prev_thread_cpu = thread_now

        proc_cores = proc_delta / dt_wall if dt_wall > 0 else 0.0
        main_share = (thread_delta / proc_delta) if proc_delta > 0 else 0.0
        queue_depth = _default_executor_queue_depth()
        threads_total = threading.active_count()

        reason: str | None = None
        if lag >= lag_immediate_threshold:
            reason = "event_loop_lag_immediate"
        elif lag >= lag_dump_threshold:
            consecutive_high_lag += 1
            if consecutive_high_lag >= lag_consecutive_required:
                reason = "event_loop_lag"
        else:
            consecutive_high_lag = 0

        if queue_depth >= executor_queue_dump_threshold:
            reason = reason or "executor_queue"

        if reason is None:
            return

        now_ts = time.time()
        if now_ts - last_dump_ts < dump_cooldown_s:
            return

        last_dump_ts = now_ts
        _emit_watchdog_dump(
            reason=reason,
            lag=lag,
            queue_depth=queue_depth,
            proc_cores=proc_cores,
            main_share=main_share,
            threads_total=threads_total,
        )

    lag_monitor_task = obs_metrics.start_event_loop_lag_monitor(
        service=service_name, period=1.0, thresholds=lag_spike_thresholds, on_lag=_on_lag_sample
    )
    if lag_monitor_task is not None:
        background_tasks.append(lag_monitor_task)

    def _attach_task_monitor(task: asyncio.Task, label: str) -> asyncio.Task:
        def _on_done(t: asyncio.Task) -> None:
            if t.cancelled():
                return
            exc = t.exception()
            if exc is not None:
                obs_metrics.record_task_exception(service_name, exported_service, label, exc)
                logger.exception("[alerts] task %s crashed", label, exc_info=exc)

        task.add_done_callback(_on_done)
        return task

    db_dsn = getattr(db, "_dsn", None)
    worker_rules = cfg_mgr.rules if cfg_mgr is not None else {}
    worker = CPUWorkerClient(
        rules=worker_rules,
        instrument_id=BINANCE_FUT_INST,
        profile=rules_profile,
        service_name=service_name,
        exported_service=exported_service,
        db_dsn=db_dsn,
    )
    await worker.start()
    engine_lock = asyncio.Lock()

    # Detectores Deribit opciones (R19–R22) – permanecen en proceso principal
    iv_det = IVSpikeDetector(IVSpikeCfg())
    oi_skew_det = OISkewDetector(OISkewCfg())

    def _apply_options_rules(rules: Dict[str, Any]) -> None:
        det = (rules or {}).get("detectors", {}) or {}
        opt = det.get("options") or {}
        iv_cfg = opt.get("iv_spike") or {}
        oi_cfg = opt.get("oi_skew") or {}
        if iv_cfg:
            iv_det.cfg.window_s = float(iv_cfg.get("window_s", iv_det.cfg.window_s))
            iv_det.cfg.up_thresh_pct = float(iv_cfg.get("up_pct", iv_det.cfg.up_thresh_pct))
            iv_det.cfg.down_thresh_pct = float(iv_cfg.get("down_pct", iv_det.cfg.down_thresh_pct))
            if "retrigger_s" in iv_cfg:
                iv_det.cfg.retrigger_s = float(iv_cfg["retrigger_s"])
        if oi_cfg:
            min_calls = float(oi_cfg.get("min_calls", 0.0) or 0.0)
            min_puts = float(oi_cfg.get("min_puts", 0.0) or 0.0)
            min_total_candidate = min_calls + min_puts if (min_calls > 0.0 and min_puts > 0.0) else 0.0
            if min_total_candidate > 0.0:
                oi_skew_det.cfg.min_total_oi = min_total_candidate
            oi_skew_det.cfg.bull_ratio_min = float(oi_cfg.get("bull_ratio", oi_skew_det.cfg.bull_ratio_min))
            oi_skew_det.cfg.bear_ratio_min = float(oi_cfg.get("bear_ratio", oi_skew_det.cfg.bear_ratio_min))
            if "retrigger_s" in oi_cfg:
                oi_skew_det.cfg.retrigger_s = float(oi_cfg["retrigger_s"])

    if cfg_mgr is not None:
        _apply_options_rules(cfg_mgr.rules)

        async def _on_rules_change(new_rules: Dict[str, Any]) -> None:
            _apply_options_rules(new_rules)
            await worker.update_rules(new_rules)

        cfg_mgr.subscribe_rules(_on_rules_change)

    # Router (admite pydantic)
    routing_dict = _routing_to_dict(routing_cfg)
    router = TelegramRouter(routing_dict, db=db, rate_limit_per_min=60)
    ctx = RuleContext(instrument_id=BINANCE_FUT_INST, profile=rules_profile, suppress_window_s=90)

    # Telemetría
    telemetry = Telemetry(db, BINANCE_FUT_INST, ctx.profile)
    db_writer = DBWriter(
        db,
        telemetry,
        instrument_id=BINANCE_FUT_INST,
        profile=ctx.profile,
        flush_interval=0.05,
        max_batch=200,
        max_queue=20_000,
        service_name=service_name,
        exported_service=exported_service,
    )

    # --- DB Tails configurados por ID ---
    # Trades: Paginamos por 'trade_id_ext' para no perder ráfagas del mismo milisegundo
    tail_trades = DBTail(db, "binance_futures.trades", id_col="trade_id_ext", default_val=0)
    
    # Depth: Paginamos por 'seq' (UpdateID)
    tail_depth = DBTail(db, "binance_futures.depth", id_col="seq", default_val=0)
    
    # Mark: Seguimos usando 'event_time' (como no tiene ID secuencial claro y es baja frecuencia, es seguro)
    tail_mark = DBTail(db, "binance_futures.mark_funding", id_col="event_time", default_val=dt.datetime.fromtimestamp(0, tz=dt.timezone.utc))

    # Inicialización LIVE (Obtener MAX ID actual)
    await tail_trades.init_live(BINANCE_FUT_INST)
    await tail_depth.init_live(BINANCE_FUT_INST)
    await tail_mark.init_live(BINANCE_FUT_INST)
    
    logger.info("DB tails initialized via ID (Robust High-Frequency Mode).")

    # Warmup OB snapshot
    try:
        timeout = aiohttp.ClientTimeout(total=6)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            # Calentamos el book sólo hasta los mismos niveles que consumimos por WS
            bids, asks = await fetch_orderbook_snapshot(s, "BTCUSDT", depth=depth_levels)
            warm_ts = time.time()
            for p, q in bids:
                await worker.enqueue_depth(warm_ts, "buy", "insert", p, q)
            for p, q in asks:
                await worker.enqueue_depth(warm_ts, "sell", "insert", p, q)
            logger.info(f"Initialized OB snapshot via REST (depth={depth_levels}).")
    except Exception as e:
        logger.warning(f"Failed to init OB snapshot: {e!s}")

    # Estado para Deribit opciones (R19–R22)
    last_deriv_opt_ts: dt.datetime = await db.fetchval(
        "SELECT COALESCE(MAX(event_time), now()) FROM deribit.options_ticker"
    )
    oi_last_by_instr: Dict[str, float] = {}
    oi_calls_by_und: Dict[str, float] = {}
    oi_puts_by_und: Dict[str, float] = {}

    async def poll_options() -> None:
        nonlocal last_deriv_opt_ts
        t0 = time.perf_counter()
        rows_opt = await db.fetch(
            """
            SELECT instrument_id,
                   event_time,
                   mark_iv,
                   open_interest
            FROM deribit.options_ticker
            WHERE event_time > $1
            ORDER BY event_time ASC
            LIMIT 5000
            """,
            last_deriv_opt_ts,
        )
        obs_metrics.alerts_stage_duration_ms.labels(stage="options_fetch").observe((time.perf_counter() - t0) * 1000)
        obs_metrics.alerts_stage_rows_total.labels(stage="options", worker="main").inc(len(rows_opt))
        if not rows_opt:
            return

        last_deriv_opt_ts = rows_opt[-1]["event_time"]
        for idx, r in enumerate(rows_opt):
            await _yield_if_needed("options", idx, every=50)
            ts_opt = r["event_time"].timestamp()
            inst_id = str(r["instrument_id"])
            mark_iv = r.get("mark_iv")
            oi_val = r.get("open_interest")

            if mark_iv is None or float(mark_iv) <= 0.0:
                telemetry.bump(ts_opt, "R19/R20", "na", disc_iv_missing=1)
            else:
                ev_iv = iv_det.on_iv(ts_opt, float(mark_iv))
                if ev_iv is not None:
                    ev_iv.fields.setdefault("instrument_id", inst_id)
                    for rule in eval_rules(_evdict(ev_iv), ctx):
                        aid, t0_dt = await enqueue_rule(rule, ev_iv.ts)
                        if aid is not None:
                            telemetry.bump(ev_iv.ts, rule["rule"], rule.get("side", "na"), emitted=1)
                            text = (
                                f"#{rule['rule']} IV spike {ev_iv.intensity:.2f}% "
                                f"({inst_id})"
                            )
                            await router.send("rules", text, alert_id=aid, ts_first=t0_dt)

            if oi_val is None:
                telemetry.bump(ts_opt, "R21/R22", "na", disc_oi_missing=1)
                continue

            underlying, opt_type = _parse_deribit_option(inst_id)
            if underlying is None or opt_type is None:
                continue

            prev = oi_last_by_instr.get(inst_id)
            if prev is not None:
                if opt_type == "C":
                    oi_calls_by_und[underlying] = oi_calls_by_und.get(underlying, 0.0) - float(prev)
                else:
                    oi_puts_by_und[underlying] = oi_puts_by_und.get(underlying, 0.0) - float(prev)

            oi_last_by_instr[inst_id] = float(oi_val)

            if opt_type == "C":
                oi_calls_by_und[underlying] = oi_calls_by_und.get(underlying, 0.0) + float(oi_val)
            else:
                oi_puts_by_und[underlying] = oi_puts_by_und.get(underlying, 0.0) + float(oi_val)

            oi_c = oi_calls_by_und.get(underlying, 0.0)
            oi_p = oi_puts_by_und.get(underlying, 0.0)

            total_oi = oi_c + oi_p
            min_total_oi = float(getattr(oi_skew_det.cfg, "min_total_oi", 0.0) or 0.0)
            if min_total_oi > 0.0 and total_oi < min_total_oi:
                telemetry.bump(ts_opt, "R21/R22", "na", disc_oi_low=1)

            ev_oi = oi_skew_det.on_oi(ts_opt, oi_c, oi_p)
            if ev_oi is not None:
                ev_oi.fields.setdefault("underlying", underlying)
                for rule in eval_rules(_evdict(ev_oi), ctx):
                    aid, t0_dt = await enqueue_rule(rule, ev_oi.ts)
                    if aid is not None:
                        telemetry.bump(ev_oi.ts, rule["rule"], rule.get("side", "na"), emitted=1)
                        text = f"#{rule['rule']} OI skew {ev_oi.intensity:.2f} ({underlying})"
                        await router.send("rules", text, alert_id=aid, ts_first=t0_dt)

    async def _yield_if_needed(stage: str, idx: int, every: int = 50, max_gap_s: Optional[float] = None) -> None:
        now = time.perf_counter()
        last_attr = f"_last_yield_{stage}"
        last_val = getattr(_yield_if_needed, last_attr, None)
        if last_val is None:
            setattr(_yield_if_needed, last_attr, now)
            last_val = now

        should_yield = idx > 0 and idx % every == 0
        if max_gap_s is not None and now - last_val >= max_gap_s:
            should_yield = True

        if should_yield:
            obs_metrics.alerts_stage_yields_total.labels(stage=stage).inc()
            if last_val is not None:
                obs_metrics.alerts_stage_yield_gap_seconds.labels(stage=stage).observe(now - last_val)
            setattr(_yield_if_needed, last_attr, now)
            await asyncio.sleep(0)

    async def enqueue_rule(rule: dict, event_ts: float):
        alert_id, ts_first_dt = await db_writer.submit("upsert_rule", (rule, event_ts), expect_result=True)
        if alert_id is None:
            ctx_type = (rule.get("context") or {}).get("type")
            obs_metrics.rule_alerts_no_id_total.labels(
                rule=rule.get("rule", "unknown"), reason="enqueue_none"
            ).inc()
            logger.warning(
                "[alerts] enqueue_rule returned None (rule=%s side=%s severity=%s dedup_key=%s type=%s profile=%s)",
                rule.get("rule"),
                rule.get("side"),
                rule.get("severity"),
                rule.get("dedup_key"),
                ctx_type,
                ctx.profile,
            )
        return alert_id, ts_first_dt

    async def enqueue_slice(ev: Event) -> None:
        await db_writer.submit("insert_slice", ev)

    async def enqueue_metrics(rows: Sequence[Tuple[str, dt.datetime, int, str, float, Optional[str], str]]) -> None:
        if rows:
            await db_writer.submit("insert_metrics", rows)

    async def enqueue_telemetry_flush() -> None:
        await db_writer.submit("telemetry_flush", None)

    async def _handle_depth_result(res: DepthProcessResult) -> None:
        t0 = time.perf_counter()
        if res.passive_event:
            await enqueue_slice(res.passive_event)
            for rule in eval_rules(_evdict(res.passive_event), ctx):
                alert_id, ts_first_dt = await enqueue_rule(rule, res.passive_event.ts)
                if alert_id is not None:
                    telemetry.bump(res.passive_event.ts, rule["rule"], rule.get("side", "na"), emitted=1)
                    text = (
                        f"#{rule['rule']} {res.passive_event.side.upper()} slicing_pass @ {res.passive_event.price} | "
                        f"qty={res.passive_event.intensity:.2f} BTC"
                    )
                    await router.send("rules", text, alert_id=alert_id, ts_first=ts_first_dt)

        if res.spoof_event:
            for rule in eval_rules(_evdict(res.spoof_event), ctx):
                aid, t0_dt = await enqueue_rule(rule, res.spoof_event.ts)
                if aid is not None:
                    telemetry.bump(res.spoof_event.ts, rule["rule"], rule.get("side", "na"), emitted=1)
                    text = (
                        f"#{rule['rule']} {res.spoof_event.side.upper()} spoofing {res.spoof_event.intensity:.2f} BTC @ "
                        f"{res.spoof_event.price} | cancel={res.spoof_event.fields.get('cancel_rate'):.2f}"
                    )
                    await router.send("rules", text, alert_id=aid, ts_first=t0_dt)

        obs_metrics.alerts_stage_duration_ms.labels(stage="depth").observe((time.perf_counter() - t0) * 1000)

    async def _handle_trade_result(res: TradeProcessResult) -> None:
        t0 = time.perf_counter()
        for ev in filter(None, [res.slice_equal, res.slice_hit]):  # type: ignore
            await enqueue_slice(ev)
            for rule in eval_rules(_evdict(ev), ctx):
                alert_id, ts_first_dt = await enqueue_rule(rule, ev.ts)
                if alert_id is not None:
                    telemetry.bump(ev.ts, rule["rule"], rule.get("side", "na"), emitted=1)
                    label = "iceberg" if ev.fields.get("mode") == "iceberg" else "hitting"
                    text = (
                        f"#{rule['rule']} {ev.side.upper()} slicing_{label} @ {ev.price} | "
                        f"qty={ev.intensity:.2f} BTC"
                    )
                    await router.send("rules", text, alert_id=alert_id, ts_first=ts_first_dt)

        if res.absorption:
            for rule in eval_rules(_evdict(res.absorption), ctx):
                alert_id, ts_first_dt = await enqueue_rule(rule, res.absorption.ts)
                if alert_id is not None:
                    telemetry.bump(res.absorption.ts, rule["rule"], rule.get("side", "na"), emitted=1)
                    text = (
                        f"#{rule['rule']} {res.absorption.side.upper()} absorption @ {res.absorption.price} | "
                        f"vol={res.absorption.intensity:.2f} BTC"
                    )
                    await router.send("rules", text, alert_id=alert_id, ts_first=ts_first_dt)

        snap = res.snapshot
        if res.slice_equal or res.slice_hit:
            slice_ts = res.slice_equal.ts if res.slice_equal else res.slice_hit.ts  # type: ignore
            slice_side = (res.slice_equal or res.slice_hit).side  # type: ignore
            if getattr(snap, "basis_vel_bps_s", None) is None:
                telemetry.bump(slice_ts, "R1/R2", slice_side, disc_metrics_none=1)
            if res.break_wall_event:
                for rule in eval_rules(_evdict(res.break_wall_event), ctx):
                    alert_id, ts_first_dt = await enqueue_rule(rule, res.break_wall_event.ts)
                    if alert_id is not None:
                        telemetry.bump(res.break_wall_event.ts, rule["rule"], rule.get("side", "na"), emitted=1)
                        text = (
                            f"#{rule['rule']} {res.break_wall_event.side.upper()} break_wall @ {res.break_wall_event.price} | "
                            f"k={res.break_wall_event.fields.get('k')}"
                        )
                        await router.send("rules", text, alert_id=alert_id, ts_first=ts_first_dt)
            elif res.break_wall_gating:
                reason_map = {
                    "basis_vel_low": {"disc_basis_vel_low": 1},
                    "dep_low": {"disc_dep_low": 1},
                    "refill_high": {"disc_refill_high": 1},
                    "top_levels_gate": {"disc_top_levels_gate": 1},
                }
                bump_kwargs = reason_map.get(res.break_wall_gating, {})
                if bump_kwargs:
                    telemetry.bump(slice_ts, "R1/R2", slice_side, **bump_kwargs)

        if res.tape_pressure:
            for rule in eval_rules(_evdict(res.tape_pressure), ctx):
                aid, t0_dt = await enqueue_rule(rule, res.tape_pressure.ts)
                if aid is not None:
                    telemetry.bump(res.tape_pressure.ts, rule["rule"], rule.get("side", "na"), emitted=1)
                    text = (
                        f"#{rule['rule']} {res.tape_pressure.side.upper()} tape_pressure "
                        f"{res.tape_pressure.intensity:.2f} @ {res.tape_pressure.price}"
                    )
                    await router.send("rules", text, alert_id=aid, ts_first=t0_dt)

        obs_metrics.alerts_stage_duration_ms.labels(stage="trades").observe((time.perf_counter() - t0) * 1000)

    async def _handle_snapshot_result(res: SnapshotProcessResult) -> None:
        snap = res.snapshot
        now_ts = res.ts
        if getattr(snap, "spread_usd", None) is None:
            telemetry.bump(now_ts, "R9/R10", "na", disc_metrics_none=1)
        elif res.dominance_event:
            for rule in eval_rules(_evdict(res.dominance_event), ctx):
                aid, t0_dt = await enqueue_rule(rule, res.dominance_event.ts)
                if aid is not None:
                    telemetry.bump(res.dominance_event.ts, rule["rule"], rule.get("side", "na"), emitted=1)
                    text = (
                        f"#{rule['rule']} {res.dominance_event.side.upper()} dominance {res.dominance_event.intensity:.1f}% "
                        f"@ {res.dominance_event.price}"
                    )
                    await router.send("rules", text, alert_id=aid, ts_first=t0_dt)
        else:
            telemetry.bump(now_ts, "R9/R10", "na", disc_dom_spread=1)

        for evd in [res.dep_bid_event, res.dep_ask_event]:
            if evd:
                for rule in eval_rules(_evdict(evd), ctx):
                    aid, t0_dt = await enqueue_rule(rule, evd.ts)
                    if aid is not None:
                        telemetry.bump(evd.ts, rule["rule"], rule.get("side", "na"), emitted=1)
                        text = f"#{rule['rule']} {evd.side.upper()} depletion {evd.intensity:.2f}"
                        await router.send("rules", text, alert_id=aid, ts_first=t0_dt)

        if getattr(snap, "basis_bps", None) is None:
            telemetry.bump(now_ts, "R15/R16", "na", disc_metrics_none=1)
        else:
            for evb in [res.basis_pos_event, res.basis_neg_event]:
                if evb:
                    for rule in eval_rules(_evdict(evb), ctx):
                        aid, t0_dt = await enqueue_rule(rule, evb.ts)
                        if aid is not None:
                            telemetry.bump(evb.ts, rule["rule"], rule.get("side", "na"), emitted=1)
                            text = f"#{rule['rule']} basis_bps={evb.intensity:.1f}"
                            await router.send("rules", text, alert_id=aid, ts_first=t0_dt)

        if getattr(snap, "basis_bps", None) is None or getattr(snap, "basis_vel_bps_s", None) is None:
            telemetry.bump(now_ts, "R17/R18", "na", disc_metrics_none=1)
        elif res.basis_mr_event:
            for rule in eval_rules(_evdict(res.basis_mr_event), ctx):
                aid, t0_dt = await enqueue_rule(rule, res.basis_mr_event.ts)
                if aid is not None:
                    telemetry.bump(res.basis_mr_event.ts, rule["rule"], rule.get("side", "na"), emitted=1)
                    text = (
                        f"#{rule['rule']} basis_mean_revert {res.basis_mr_event.side.upper()} | "
                        f"vel={res.basis_mr_event.intensity:.2f} bps/s"
                    )
                    await router.send("rules", text, alert_id=aid, ts_first=t0_dt)

        await enqueue_metrics(res.metric_rows)
        await enqueue_telemetry_flush()

    async def _handle_macro_event(ev: Event) -> None:
        await dispatch_macro_event(ev, ctx, enqueue_rule, telemetry, router)

    async def _dispatch_worker_result(res: WorkerResult) -> None:
        stage = res.kind or "unknown"
        if stage == "trade":
            stage = "trades"
        wait_t0 = time.perf_counter()
        await engine_lock.acquire()
        wait_duration = time.perf_counter() - wait_t0
        hold_t0 = time.perf_counter()
        try:
            if res.kind == "depth" and isinstance(res.payload, DepthProcessResult):
                await _handle_depth_result(res.payload)
            elif res.kind == "trade" and isinstance(res.payload, TradeProcessResult):
                await _handle_trade_result(res.payload)
            elif res.kind == "snapshot" and isinstance(res.payload, SnapshotProcessResult):
                await _handle_snapshot_result(res.payload)
            elif res.kind == "mark":
                # no-op: mark results carry no payload beyond timing
                pass
            elif res.kind == "macro" and isinstance(res.payload, Event):
                await _handle_macro_event(res.payload)
        finally:
            hold_duration = time.perf_counter() - hold_t0
            engine_lock.release()
            obs_metrics.alerts_engine_lock_wait_seconds.labels(service=service_name, stage=stage).observe(wait_duration)
            obs_metrics.alerts_engine_lock_seconds.labels(service=service_name, stage=stage).observe(hold_duration)

    queue_stale_after = {"trade": 2.0, "depth": 2.0, "mark": 1.0}
    batch_conf = {
        "trade": {"size": 200, "wait": 0.015, "yield": 40, "yield_gap": 0.025},
        "depth": {"size": 250, "wait": 0.015, "yield": 50, "yield_gap": 0.025},
        "mark": {"size": 90, "wait": 0.01, "yield": 40, "yield_gap": 0.025},
    }

    def _queue_age_metrics(stream: str, enqueued_at: float) -> float:
        age = max(0.0, time.perf_counter() - enqueued_at)
        obs_metrics.alerts_queue_time_seconds.labels(stream=stream).observe(age)
        obs_metrics.alerts_queue_time_latest_seconds.labels(stream=stream).set(age)
        return age

    async def _options_poller(stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            await poll_options()
            await asyncio.sleep(0.5)

    async def _snapshot_loop(stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            t0 = time.perf_counter()
            await worker.enqueue_snapshot(time.time())
            obs_metrics.alerts_snapshot_duration_seconds.observe(time.perf_counter() - t0)
            await asyncio.sleep(0.05)

    async def _worker_results_loop(stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            res = await worker.get_result()
            await _dispatch_worker_result(res)

    async def _process_stream_batch(
        reader: WsReader,
        stream: str,
        handler: Callable[[Any], Awaitable[None]],
        stop_event: asyncio.Event,
        *,
        worker_name: str = "main",
    ) -> None:
        while not stop_event.is_set():
            cfg = batch_conf.get(stream, {})
            batch = await reader.get_batch(
                stream,
                max_items=int(cfg.get("size", 250)),
                max_wait_s=float(cfg.get("wait", 0.02)),
            )
            if not batch:
                await asyncio.sleep(0)
                continue

            t_batch = time.perf_counter()
            stale_after = float(queue_stale_after.get(stream, 2.0))
            handler_t0 = time.perf_counter()
            yield_gap_s = float(cfg.get("yield_gap", 0.025))
            for idx, qev in enumerate(batch):
                age = _queue_age_metrics(stream, qev.enqueued_at)
                if age > stale_after:
                    obs_metrics.alerts_queue_discarded_total.labels(
                        stream=stream, cause="stale_drop"
                    ).inc()
                    obs_metrics.alerts_queue_dropped_total.inc()
                    continue
                await handler(qev.event)
                stage_label = stream if stream != "trade" else "trades"
                obs_metrics.alerts_stage_rows_total.labels(
                    stage=stage_label, worker=worker_name
                ).inc()
                await _yield_if_needed(
                    stream if stream != "trade" else "trades",
                    idx,
                    every=int(cfg.get("yield", 50)),
                    max_gap_s=yield_gap_s,
                )
            obs_metrics.alerts_handler_duration_seconds.labels(
                stream=stream, worker=worker_name
            ).observe(
                time.perf_counter() - handler_t0
            )
            obs_metrics.alerts_batch_duration_seconds.labels(
                stream=stream, worker=worker_name
            ).observe(
                time.perf_counter() - t_batch
            )

    async def _run_ws_pipeline(depth_levels: int, depth_ms: int, symbol: str) -> None:
        ws_reader = WsReader(depth_levels=depth_levels, depth_ms=depth_ms, symbol=symbol)
        await ws_reader.start()
        stop_event = asyncio.Event()

        async def trade_handler(ev: TradeEvent) -> None:
            await worker.enqueue_trade(ev.ts, ev.side, ev.price, ev.qty)

        async def depth_handler(ev: DepthEvent) -> None:
            await worker.enqueue_depth(ev.ts, ev.side, ev.action, ev.price, ev.qty)

        async def mark_handler(ev: MarkEvent) -> None:
            await worker.enqueue_mark(ev.ts, ev.mark_price, ev.index_price)

        async def process_trades() -> None:
            await _process_stream_batch(ws_reader, "trade", trade_handler, stop_event, worker_name="trades")

        async def process_depth(worker_name: str) -> None:
            await _process_stream_batch(
                ws_reader, "depth", depth_handler, stop_event, worker_name=worker_name
            )

        async def process_mark() -> None:
            await _process_stream_batch(ws_reader, "mark", mark_handler, stop_event, worker_name="mark")

        tasks = [
            _attach_task_monitor(
                asyncio.create_task(
                    process_trades(),
                    name="alerts-trade-worker",
                ),
                label="ws_trade_worker",
            ),
            _attach_task_monitor(
                asyncio.create_task(
                    process_depth("depth-0"),
                    name="alerts-depth-worker-0",
                ),
                label="ws_depth_worker_0",
            ),
            _attach_task_monitor(
                asyncio.create_task(
                    process_depth("depth-1"),
                    name="alerts-depth-worker-1",
                ),
                label="ws_depth_worker_1",
            ),
            _attach_task_monitor(
                asyncio.create_task(
                    process_mark(),
                    name="alerts-mark-worker",
                ),
                label="ws_mark_worker",
            ),
            _attach_task_monitor(
                asyncio.create_task(_options_poller(stop_event), name="alerts-options-poller"),
                label="options_poller",
            ),
            _attach_task_monitor(
                asyncio.create_task(_snapshot_loop(stop_event), name="alerts-snapshot-loop"),
                label="snapshot_loop",
            ),
            _attach_task_monitor(
                asyncio.create_task(_worker_results_loop(stop_event), name="alerts-worker-results"),
                label="worker_results",
            ),
        ]

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
        except Exception:
            obs_metrics.alerts_uncaught_errors_total.labels(
                service=service_name, exported_service=exported_service, where="ws_pipeline"
            ).inc()
            logger.exception("[alerts] ws pipeline crashed")
            raise
        finally:
            stop_event.set()
            for task in tasks:
                task.cancel()
            await ws_reader.stop()

    await db_writer.start()
    try:
        if alerts_source == "ws":
            await _run_ws_pipeline(depth_levels, depth_ms, symbol)
            return

        # ------------------- bucle principal -------------------
        stop_event = asyncio.Event()
        options_task = _attach_task_monitor(
            asyncio.create_task(_options_poller(stop_event), name="alerts-options-poller"),
            label="options_poller",
        )
        snapshot_task = _attach_task_monitor(
            asyncio.create_task(_snapshot_loop(stop_event), name="alerts-snapshot-loop"),
            label="snapshot_loop",
        )
        worker_results_task = _attach_task_monitor(
            asyncio.create_task(_worker_results_loop(stop_event), name="alerts-worker-results"),
            label="worker_results",
        )
        try:
            while True:
                # 1) depth -> book & métricas + slicing pasivo + spoofing
                # Paginación por ID (seq)
                for idx, r in enumerate(await tail_depth.fetch_new(BINANCE_FUT_INST, limit=5000)):
                    await _yield_if_needed("depth", idx, every=50)
                    await worker.enqueue_depth(
                        r["event_time"].timestamp(),
                        r["side"],
                        r["action"],
                        float(r["price"]),
                        float(r["qty"]),
                    )
                    obs_metrics.alerts_stage_rows_total.labels(stage="depth", worker="main").inc()

                # 2) mark funding -> basis
                # Paginación por Tiempo (legacy para mark)
                for idx, r in enumerate(await tail_mark.fetch_new(BINANCE_FUT_INST, limit=1000)):
                    await _yield_if_needed("mark", idx, every=50)
                    await worker.enqueue_mark(
                        r["event_time"].timestamp(),
                        float(r.get("mark_price") or 0) or None,
                        float(r.get("index_price") or 0) or None,
                    )
                    obs_metrics.alerts_stage_rows_total.labels(stage="mark", worker="main").inc()

                # 3) trades -> slicing (iceberg/hitting) + absorción + break_wall + tape_pressure + spoofing_exec
                # Paginación por ID (trade_id_ext)
                for idx, r in enumerate(await tail_trades.fetch_new(BINANCE_FUT_INST, limit=5000)):
                    await _yield_if_needed("trades", idx, every=50)
                    await worker.enqueue_trade(
                        r["event_time"].timestamp(),
                        r["side"],
                        float(r["price"]),
                        float(r["qty"]),
                    )
                    obs_metrics.alerts_stage_rows_total.labels(stage="trades", worker="main").inc()

                await asyncio.sleep(0.05)  # 50ms cadence
        finally:
            stop_event.set()
            options_task.cancel()
            snapshot_task.cancel()
            worker_results_task.cancel()
            await asyncio.gather(options_task, snapshot_task, worker_results_task, return_exceptions=True)
    except Exception:
        obs_metrics.alerts_uncaught_errors_total.labels(
            service=service_name, exported_service=exported_service, where="main_loop"
        ).inc()
        logger.exception("[alerts] pipeline crashed")
        raise
    finally:
        for task in background_tasks:
            task.cancel()
        if background_tasks:
            await asyncio.gather(*background_tasks, return_exceptions=True)
        await worker.stop()
        await db_writer.stop()
