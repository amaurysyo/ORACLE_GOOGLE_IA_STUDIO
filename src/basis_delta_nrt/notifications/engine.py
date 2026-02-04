from __future__ import annotations

import asyncio
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Awaitable, Callable, Optional

from basis_delta_nrt.config import NotificationsCfg
from basis_delta_nrt.notifications.types import NotificationEvent


class NotificationsEngine:
    def __init__(
        self,
        router: Callable[[NotificationEvent], Awaitable[None]],
        config: Optional[NotificationsCfg] = None,
    ) -> None:
        self._router = router
        self._config = config or NotificationsCfg()
        self._queue: asyncio.Queue[NotificationEvent] = asyncio.Queue(maxsize=self._config.queue_max)
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._dedup_map: OrderedDict[str, datetime] = OrderedDict()
        self._last_sent_by_key: OrderedDict[str, datetime] = OrderedDict()
        self._emit_counter = 0

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop_event.clear()
            self._task = asyncio.create_task(self._run(), name="notifications-engine")

    async def stop(self, timeout_s: float | None = None) -> None:
        if self._task is None:
            return
        self._stop_event.set()
        self._task.cancel()
        timeout = timeout_s if timeout_s is not None else self._config.stop_timeout_s
        try:
            await asyncio.wait_for(self._task, timeout=timeout)
        except asyncio.TimeoutError:
            return
        except asyncio.CancelledError:
            return

    async def emit(self, event: NotificationEvent) -> bool:
        event = self._normalize_event(event)
        if not self._passes_cooldown(event):
            return False
        if not self._passes_dedup(event):
            return False
        await self._enqueue(event)
        return True

    async def _run(self) -> None:
        try:
            while not self._stop_event.is_set():
                event = await self._queue.get()
                try:
                    await self._router(event)
                except Exception:
                    continue
                finally:
                    self._queue.task_done()
        except asyncio.CancelledError:
            return

    def _normalize_event(self, event: NotificationEvent) -> NotificationEvent:
        ts = event.ts_utc
        if ts.tzinfo is None:
            event.ts_utc = ts.replace(tzinfo=timezone.utc)
        else:
            event.ts_utc = ts.astimezone(timezone.utc)
        return event

    async def _enqueue(self, event: NotificationEvent) -> None:
        if not self._queue.full():
            await self._queue.put(event)
            return
        if self._config.drop_policy != "drop_oldest":
            return
        try:
            _ = self._queue.get_nowait()
            self._queue.task_done()
        except asyncio.QueueEmpty:
            pass
        await self._queue.put(event)

    def _passes_dedup(self, event: NotificationEvent) -> bool:
        window = self._config.dedup_window_s
        if window <= 0:
            return True
        key = f"{event.topic}:{event.event_type}:{event.payload}"
        now = event.ts_utc
        last = self._dedup_map.get(key)
        if last and (now - last).total_seconds() < window:
            return False
        self._dedup_map[key] = now
        self._dedup_map.move_to_end(key)
        if len(self._dedup_map) > self._config.dedup_max_keys:
            while len(self._dedup_map) > self._config.dedup_max_keys:
                self._dedup_map.popitem(last=False)
        return True

    def _passes_cooldown(self, event: NotificationEvent) -> bool:
        cooldown = self._config.cooldown_s_by_type.get(event.event_type, 0)
        if cooldown <= 0:
            return True
        now = event.ts_utc
        key = event.cooldown_key
        last = self._last_sent_by_key.get(key)
        if last and (now - last).total_seconds() < cooldown:
            return False
        self._last_sent_by_key[key] = now
        self._last_sent_by_key.move_to_end(key)
        self._emit_counter += 1
        if self._emit_counter % self._config.cooldown_prune_every_n == 0:
            self._prune_cooldown_keys()
        if len(self._last_sent_by_key) > self._config.cooldown_max_keys:
            self._prune_cooldown_keys()
        return True

    def _prune_cooldown_keys(self) -> None:
        while len(self._last_sent_by_key) > self._config.cooldown_max_keys:
            self._last_sent_by_key.popitem(last=False)
