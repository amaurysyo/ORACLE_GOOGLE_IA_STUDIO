from __future__ import annotations

import httpx

from basis_delta_nrt.config import AppCfg
from basis_delta_nrt.notifications.engine import NotificationsEngine
from basis_delta_nrt.notifications.sinks.router import DiscordWebhookRouter


class AppState:
    def __init__(self) -> None:
        self.discord_http_client: httpx.AsyncClient | None = None
        self.notifications_engine: NotificationsEngine | None = None


class App:
    def __init__(self, config: AppCfg) -> None:
        self.config = config
        self.state = AppState()

    async def startup(self) -> None:
        discord_cfg = self.config.notifications.sinks.discord
        self.state.discord_http_client = httpx.AsyncClient(timeout=discord_cfg.timeout_s)
        router = DiscordWebhookRouter(discord_cfg, self.state.discord_http_client)
        self.state.notifications_engine = NotificationsEngine(router.send, self.config.notifications)
        self.state.notifications_engine.start()

    async def shutdown(self) -> None:
        if self.state.notifications_engine:
            await self.state.notifications_engine.stop()
        if self.state.discord_http_client:
            await self.state.discord_http_client.aclose()
