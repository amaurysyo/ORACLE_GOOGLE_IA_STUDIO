#=======================================
# file:  oraculo/db.py
#=======================================
"""
Created on Fri Oct 31 20:20:17 2025

@author: AMAURY
"""
# oraculo/db.py  (añade execute_many helper)
from __future__ import annotations
import pathlib
from typing import Any, Iterable
import asyncpg
from loguru import logger

class DB:
    def __init__(self, dsn: str):
        self._dsn = dsn
        self._pool: asyncpg.Pool | None = None

    async def connect(self, min_size: int = 1, max_size: int = 8) -> None:
        self._pool = await asyncpg.create_pool(dsn=self._dsn, min_size=min_size, max_size=max_size)
        logger.info("Pool DB listo")

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()

    async def exec_sql_file(self, path: str) -> None:
        assert self._pool
        sql = pathlib.Path(path).read_text(encoding="utf-8")
        async with self._pool.acquire() as con:
            async with con.transaction():
                await con.execute(sql)

    async def fetch(self, sql: str, *args: Any) -> list[asyncpg.Record]:
        assert self._pool
        async with self._pool.acquire() as con:
            return await con.fetch(sql, *args)

    async def fetchval(self, sql: str, *args: Any) -> Any:
        assert self._pool
        async with self._pool.acquire() as con:
            return await con.fetchval(sql, *args)

    async def execute(self, sql: str, *args: Any) -> str:
        assert self._pool
        async with self._pool.acquire() as con:
            return await con.execute(sql, *args)

    async def execute_many(self, sql: str, rows: Iterable[tuple]) -> None:
        """Executemany con una transacción."""
        assert self._pool
        async with self._pool.acquire() as con:
            async with con.transaction():
                await con.executemany(sql, rows)