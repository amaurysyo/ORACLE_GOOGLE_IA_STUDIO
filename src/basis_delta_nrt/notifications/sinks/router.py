from __future__ import annotations

import os
from typing import Dict

import httpx

from basis_delta_nrt.config import DiscordSinkCfg
from basis_delta_nrt.notifications.sinks.discord_webhook import DiscordWebhookSink
from basis_delta_nrt.notifications.types import NotificationEvent


class DiscordWebhookRouter:
    def __init__(self, cfg: DiscordSinkCfg, client: httpx.AsyncClient) -> None:
        self._cfg = cfg
        self._client = client
        self._sinks: Dict[str, DiscordWebhookSink] = {}

    async def send(self, event: NotificationEvent) -> None:
        sink = self._resolve_sink(event.topic)
        await sink.send(event)

    def _resolve_sink(self, topic: str) -> DiscordWebhookSink:
        key = topic.lower()
        if key in self._sinks:
            return self._sinks[key]
        env_name = self._webhook_env_for_topic(key)
        url = os.getenv(env_name)
        if not url:
            raise RuntimeError(f"missing discord webhook env for topic={topic}")
        sink = DiscordWebhookSink(
            url,
            self._client,
            max_retries=self._cfg.max_retries,
            backoff_s=self._cfg.backoff_s,
            rate_limit_per_min=self._cfg.rate_limit_per_min,
            username=self._cfg.username,
            avatar_url=self._cfg.avatar_url,
        )
        self._sinks[key] = sink
        return sink

    def _webhook_env_for_topic(self, topic: str) -> str:
        if topic == "trading":
            return self._cfg.webhook_env_trading
        if topic == "ops":
            return self._cfg.webhook_env_ops
        raise RuntimeError(f"unsupported discord topic={topic}")
