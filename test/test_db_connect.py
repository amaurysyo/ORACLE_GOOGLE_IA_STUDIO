# -*- coding: utf-8 -*-
"""
Created on Fri Oct 31 19:41:23 2025

@author: AMAURY
"""

# tests/test_db_connect.py
import asyncio
import os
from oraculo.db import DB

def test_db_connect_event_loop():
    dsn = os.environ.get("PG_DSN", "postgresql://postgres:postgres@localhost:5432/oraculo")
    async def _run():
        db = DB(dsn)
        await db.connect()
        val = await db.fetchval("SELECT 1;")
        await db.close()
        assert val == 1
    asyncio.run(_run())