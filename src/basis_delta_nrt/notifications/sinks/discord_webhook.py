from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import httpx

from basis_delta_nrt.notifications.types import NotificationEvent


class _RateLimiter:
    def __init__(self, rate_limit_per_min: int) -> None:
        self._min_interval = 60.0 / rate_limit_per_min if rate_limit_per_min > 0 else 0.0
        self._last_sent = 0.0

    async def wait(self) -> None:
        if self._min_interval <= 0:
            return
        now = time.monotonic()
        elapsed = now - self._last_sent
        if elapsed < self._min_interval:
            await asyncio.sleep(self._min_interval - elapsed)
        self._last_sent = time.monotonic()


class DiscordWebhookSink:
    def __init__(
        self,
        webhook_url: str,
        client: httpx.AsyncClient,
        *,
        max_retries: int = 2,
        backoff_s: float = 1.0,
        rate_limit_per_min: int = 30,
        username: str | None = None,
        avatar_url: str | None = None,
    ) -> None:
        self._webhook_url = webhook_url
        self._client = client
        self._max_retries = max_retries
        self._backoff_s = backoff_s
        self._limiter = _RateLimiter(rate_limit_per_min)
        self._username = username
        self._avatar_url = avatar_url

    async def send(self, event: NotificationEvent) -> None:
        await self._limiter.wait()
        content = self._format_content(event)
        payload = {"content": content}
        if self._username:
            payload["username"] = self._username
        if self._avatar_url:
            payload["avatar_url"] = self._avatar_url
        attempts = self._max_retries + 1
        last_error: Exception | None = None
        for attempt in range(attempts):
            try:
                response = await self._client.post(self._webhook_url, json=payload)
            except httpx.HTTPError as exc:
                last_error = exc
                await self._sleep_backoff(attempt)
                continue
            if 200 <= response.status_code < 300:
                return
            if response.status_code == 429:
                retry_after = self._retry_after(response)
                await asyncio.sleep(retry_after)
                continue
            if response.status_code in {401, 403, 404}:
                raise RuntimeError("discord webhook auth/invalid") from None
            if 500 <= response.status_code < 600:
                await self._sleep_backoff(attempt)
                last_error = RuntimeError(f"discord webhook server error {response.status_code}")
                continue
            raise RuntimeError(f"discord webhook error {response.status_code}")
        if last_error:
            raise last_error

    def _format_content(self, event: NotificationEvent) -> str:
        payload = self._compact_payload(event.payload)
        content = f"[{event.topic.upper()}] {event.event_type}"
        if payload:
            content = f"{content} {payload}"
        return content[:2000]

    def _compact_payload(self, payload: Any) -> str:
        if payload is None:
            return ""
        if isinstance(payload, str):
            return payload
        try:
            return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        except TypeError:
            return str(payload)

    async def _sleep_backoff(self, attempt: int) -> None:
        await asyncio.sleep(self._backoff_s * (attempt + 1))

    def _retry_after(self, response: httpx.Response) -> float:
        header = response.headers.get("Retry-After")
        if header is None:
            return self._backoff_s
        try:
            return float(header)
        except ValueError:
            return self._backoff_s
