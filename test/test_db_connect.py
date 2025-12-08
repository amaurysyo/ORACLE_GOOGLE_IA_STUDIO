# -*- coding: utf-8 -*-
"""
Created on Fri Oct 31 19:41:23 2025

@author: AMAURY
"""

import os
import pytest

if os.environ.get("RUN_DB_TESTS") != "1":
    pytest.skip("Prueba de base de datos deshabilitada por defecto", allow_module_level=True)

import asyncio
from oraculo.db import DB
pytest.importorskip("asyncpg")


def test_db_connect_event_loop():
    dsn = os.environ.get("PG_DSN", "postgresql://postgres:postgres@localhost:5432/oraculo")

    async def _run():
        db = DB(dsn)
        await db.connect()
        val = await db.fetchval("SELECT 1;")
        await db.close()
        assert val == 1

    asyncio.run(_run())
