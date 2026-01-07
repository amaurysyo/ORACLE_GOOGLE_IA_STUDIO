import asyncio
from pathlib import Path
import sys
import types

import pytest

_dummy_ws_module = types.ModuleType("unicorn_binance_websocket_api")
_dummy_ws_module.BinanceWebSocketApiManager = object
sys.modules.setdefault("unicorn_binance_websocket_api", _dummy_ws_module)

_dummy_manager_module = types.ModuleType("unicorn_binance_websocket_api.manager")
_dummy_manager_module.BinanceWebSocketApiManager = object
sys.modules.setdefault("unicorn_binance_websocket_api.manager", _dummy_manager_module)

_dummy_legacy_module = types.ModuleType(
    "unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager"
)
_dummy_legacy_module.BinanceWebSocketApiManager = object
sys.modules.setdefault(
    "unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager",
    _dummy_legacy_module,
)

from oraculo.ingest.binance_ws import FuturesWSRunner, _ms_to_ts


class _BatcherStub:
    def __init__(self) -> None:
        self.rows: list[tuple[str, tuple]] = []

    def add(self, key: str, row: tuple) -> None:
        self.rows.append((key, row))


def test_handle_mark_persists_estimated_settle_price() -> None:
    batcher = _BatcherStub()
    runner = FuturesWSRunner(db=object(), batcher=batcher, depth_levels=5, depth_ms=100)
    payload = {
        "E": 1_700_000_000_000,
        "p": "27123.45",
        "i": "27000.00",
        "P": "27110.00",
        "r": "0.0001",
        "T": 1_700_000_600_000,
    }

    asyncio.run(runner._handle_mark(payload))

    assert len(batcher.rows) == 1
    key, row = batcher.rows[0]
    assert key == "bfut_mark"
    assert row[1] == _ms_to_ts(payload["E"])
    assert row[2] == pytest.approx(27123.45)
    assert row[3] == pytest.approx(27000.00)
    assert row[4] == pytest.approx(27110.00)


def test_handle_mark_sets_estimated_settle_price_none_when_missing() -> None:
    batcher = _BatcherStub()
    runner = FuturesWSRunner(db=object(), batcher=batcher, depth_levels=5, depth_ms=100)
    payload = {
        "E": 1_700_000_000_000,
        "p": "27123.45",
        "i": "27000.00",
        "r": "0.0001",
        "T": 1_700_000_600_000,
    }

    asyncio.run(runner._handle_mark(payload))

    _key, row = batcher.rows[0]
    assert row[4] is None


def test_bfut_mark_insert_has_est_settle_price_column() -> None:
    cli_path = Path(__file__).resolve().parents[1] / "scripts" / "cli.py"
    contents = cli_path.read_text(encoding="utf-8")
    assert "bfut_mark" in contents
    assert "est_settle_price" in contents
    assert "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)" in contents
