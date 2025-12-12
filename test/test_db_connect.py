# -*- coding: utf-8 -*-
"""
Created on Fri Oct 31 19:41:23 2025

@author: AMAURY
"""

import os
import asyncio
import pytest

if os.environ.get("RUN_DB_TESTS") != "1":
    pytest.skip("Prueba de base de datos deshabilitada por defecto", allow_module_level=True)

from oraculo.db import DB
pytest.importorskip("asyncpg")


def _run_async(coro):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()
        asyncio.set_event_loop(None)


def test_db_connect_event_loop():
    async def _run():
        dsn = os.environ.get("PG_DSN", "postgresql://postgres:postgres@localhost:5432/oraculo")

        db = DB(dsn)
        await db.connect()
        val = await db.fetchval("SELECT 1;")
        await db.close()
        assert val == 1

    _run_async(_run())
