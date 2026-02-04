from __future__ import annotations

from dataclasses import dataclass, field
import os
from typing import Any

import yaml


@dataclass(slots=True)
class DiscordSinkCfg:
    enabled: bool = False
    timeout_s: float = 5.0
    max_retries: int = 2
    backoff_s: float = 1.0
    rate_limit_per_min: int = 30
    username: str | None = "Basis-Delta NRT"
    avatar_url: str | None = None
    webhook_env_trading: str = "BDNRT_DISCORD_WEBHOOK_TRADING_URL"
    webhook_env_ops: str = "BDNRT_DISCORD_WEBHOOK_OPS_URL"


@dataclass(slots=True)
class NotificationsSinksCfg:
    discord: DiscordSinkCfg = field(default_factory=DiscordSinkCfg)


@dataclass(slots=True)
class NotificationsCfg:
    enabled: bool = False
    queue_max: int = 200
    drop_policy: str = "drop_oldest"
    dedup_window_s: int = 60
    dedup_max_keys: int = 2000
    cooldown_s_by_type: dict[str, float] = field(default_factory=dict)
    cooldown_max_keys: int = 5000
    cooldown_prune_every_n: int = 200
    stop_timeout_s: float = 2.0
    sinks: NotificationsSinksCfg = field(default_factory=NotificationsSinksCfg)

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "NotificationsCfg":
        payload = data or {}
        sinks_data = dict(payload.get("sinks") or {})
        if "discord" not in sinks_data and "discord" in payload:
            sinks_data["discord"] = payload.get("discord")
        discord_cfg = DiscordSinkCfg(**(sinks_data.get("discord") or {}))
        return cls(
            enabled=payload.get("enabled", cls.enabled),
            queue_max=payload.get("queue_max", cls.queue_max),
            drop_policy=payload.get("drop_policy", cls.drop_policy),
            dedup_window_s=payload.get("dedup_window_s", cls.dedup_window_s),
            dedup_max_keys=payload.get("dedup_max_keys", cls.dedup_max_keys),
            cooldown_s_by_type=payload.get("cooldown_s_by_type", {}) or {},
            cooldown_max_keys=payload.get("cooldown_max_keys", cls.cooldown_max_keys),
            cooldown_prune_every_n=payload.get("cooldown_prune_every_n", cls.cooldown_prune_every_n),
            stop_timeout_s=payload.get("stop_timeout_s", cls.stop_timeout_s),
            sinks=NotificationsSinksCfg(discord=discord_cfg),
        )


@dataclass(slots=True)
class AppCfg:
    notifications: NotificationsCfg = field(default_factory=NotificationsCfg)

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "AppCfg":
        payload = data or {}
        notifications = NotificationsCfg.from_dict(payload.get("notifications") or {})
        return cls(notifications=notifications)


def load_config(path: str) -> AppCfg:
    with open(path, "r", encoding="utf-8") as handle:
        raw = os.path.expandvars(handle.read())
    data = yaml.safe_load(raw)
    return AppCfg.from_dict(data or {})
