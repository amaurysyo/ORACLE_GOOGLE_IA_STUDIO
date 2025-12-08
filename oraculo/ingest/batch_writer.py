#=======================================
# file:  oraculo/ingest/batch_writer.py
#=======================================

from __future__ import annotations

import asyncio
import json
import time
from typing import Iterable

from loguru import logger

from ..db import DB
from ..obs.metrics import db_insert_ms, queue_backlog, batch_queue_delay_ms


class AsyncBatcher:
    """Batcher simple por-clave (tabla)."""

    def __init__(self, db: DB, flush_ms: int, max_rows: int):
        self._db = db
        self._flush_ms = flush_ms
        self._max_rows = max_rows
        self._queues: dict[str, list[tuple]] = {}
        self._stmts: dict[str, str] = {}
        self._last_flush = time.monotonic()
        self._lock = asyncio.Lock()
        # Momento en el que se añadió la primera fila a la cola (por key)
        self._queued_first_ts: dict[str, float] = {}

    def register_stmt(self, key: str, sql: str) -> None:
        self._stmts[key] = sql
        self._queues.setdefault(key, [])
        # Inicializamos el timestamp de primera encolación
        self._queued_first_ts.setdefault(key, 0.0)

    def update_policy(self, flush_ms: int | None = None, max_rows: int | None = None) -> None:
        if flush_ms is not None:
            self._flush_ms = flush_ms
        if max_rows is not None:
            self._max_rows = max_rows

    def add(self, key: str, row: tuple) -> None:
        q = self._queues[key]
        # Serializamos diccionarios para campos jsonb; asyncpg espera str y no objetos nativos
        row = tuple(json.dumps(v) if isinstance(v, dict) else v for v in row)
        # Si la cola estaba vacía, marcamos el instante de la primera fila
        if not q:
            self._queued_first_ts[key] = time.monotonic()
        q.append(row)
        queue_backlog.labels(key).set(len(q))

    async def flush_if_needed(self) -> None:
        now = time.monotonic()
        need_time = (now - self._last_flush) * 1000.0 >= self._flush_ms
        need_size = any(len(q) >= self._max_rows for q in self._queues.values())
        if need_time or need_size:
            await self.flush_all()

    async def flush_all(self) -> None:
        async with self._lock:
            # Usamos un "now" común para calcular el delay de todas las colas
            now = time.monotonic()

            for key, rows in self._queues.items():
                if not rows:
                    continue

                sql = self._stmts[key]

                # --- NUEVO: medimos el tiempo que las filas han pasado en la cola ---
                first_ts = self._queued_first_ts.get(key) or self._last_flush
                queue_delay_ms = (now - first_ts) * 1000.0
                batch_queue_delay_ms.labels(key).observe(queue_delay_ms)

                # --- Ya existente: medimos el tiempo de inserción en DB ---
                start = time.perf_counter()
                await self._db.execute_many(sql, rows)
                elapsed_ms = (time.perf_counter() - start) * 1000.0
                db_insert_ms.observe(elapsed_ms)  # why: monitorizar backpressure DB

                logger.info(
                    f"[batch:{key}] inserted={len(rows)} "
                    f"in {elapsed_ms:.1f}ms (queue_delay={queue_delay_ms:.1f}ms)"
                )

                # Limpiamos cola y métricas asociadas
                rows.clear()
                queue_backlog.labels(key).set(0)
                self._queued_first_ts[key] = 0.0

            self._last_flush = now
