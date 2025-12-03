#=======================================
# file:  oraculo/config.py
#=======================================
"""
Created on Fri Oct 31 20:16:03 2025

@author: AMAURY
"""

from __future__ import annotations
import os
import yaml
from pydantic import BaseModel, ConfigDict

class StorageCfg(BaseModel):
    dsn: str
    batch_max_rows: int = 500
    flush_ms: int = 200

class TelegramBotCfg(BaseModel):
    token: str
    chat_id: int

class RoutingCfg(BaseModel):
    bot_events: TelegramBotCfg
    bot_rules: TelegramBotCfg
    bot_errors: TelegramBotCfg

class AppCfg(BaseModel):
    symbol: str = "BTCUSDT"
    network_timeout_ms: int = 1500
    storage: StorageCfg

class Config(BaseModel):
    # Permite leer campos no modelados (p.ej. 'streams', 'observability' extendida)
    model_config = ConfigDict(extra="allow")
    app: AppCfg
    routing: dict[str, RoutingCfg] | None = None
    observability: dict | None = None
    failure_policies: dict | None = None

def load_config(path: str) -> Config:
    with open(path, "r", encoding="utf-8") as f:
        raw = os.path.expandvars(f.read())
    data = yaml.safe_load(raw)
    return Config.model_validate(data)