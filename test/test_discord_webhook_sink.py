import sys
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from basis_delta_nrt.notifications.sinks.discord_webhook import DiscordWebhookSink  # noqa: E402
from basis_delta_nrt.notifications.sinks.router import DiscordWebhookRouter  # noqa: E402
from basis_delta_nrt.notifications.types import NotificationEvent  # noqa: E402
from basis_delta_nrt.config import DiscordSinkCfg  # noqa: E402


def test_discord_webhook_sink_handles_429_then_success():
    calls = {"count": 0}

    async def _run() -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            calls["count"] += 1
            if calls["count"] == 1:
                return httpx.Response(429, headers={"Retry-After": "0"})
            return httpx.Response(204)

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            sink = DiscordWebhookSink(
                "https://discord.test/webhook",
                client,
                max_retries=1,
                backoff_s=0,
                rate_limit_per_min=0,
            )
            await sink.send(NotificationEvent(topic="trading", event_type="ping", payload={"ok": True}))

    import asyncio

    asyncio.run(_run())
    assert calls["count"] == 2


def test_discord_webhook_sink_success_204():
    async def _run() -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(204)

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            sink = DiscordWebhookSink("https://discord.test/webhook", client, rate_limit_per_min=0)
            await sink.send(NotificationEvent(topic="ops", event_type="ok", payload="ready"))

    import asyncio

    asyncio.run(_run())


def test_discord_router_missing_env_raises():
    async def _run() -> None:
        async with httpx.AsyncClient() as client:
            cfg = DiscordSinkCfg(
                webhook_env_trading="MISSING_TRADING",
                webhook_env_ops="MISSING_OPS",
            )
            router = DiscordWebhookRouter(cfg, client)
            with pytest.raises(RuntimeError, match="missing discord webhook env"):
                await router.send(NotificationEvent(topic="trading", event_type="ping"))

    import asyncio

    asyncio.run(_run())
