from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass(slots=True)
class NotificationEvent:
    topic: str
    event_type: str
    payload: Any = None
    ts_utc: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    key: str | None = None

    def __post_init__(self) -> None:
        ts = self.ts_utc
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        else:
            ts = ts.astimezone(timezone.utc)
        self.ts_utc = ts

    @property
    def cooldown_key(self) -> str:
        return self.key or f"{self.topic}:{self.event_type}"
