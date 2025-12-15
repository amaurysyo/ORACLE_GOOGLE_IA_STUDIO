#=======================================
# file:  oraculo/config_hot.py
#=======================================
from __future__ import annotations
import asyncio
import os
from pathlib import Path
from typing import Awaitable, Callable, Any, Dict, List

import yaml
from dotenv import load_dotenv
from watchfiles import awatch

from .config import load_config, Config

OnChange = Callable[[Config], Awaitable[None]]
OnRulesChange = Callable[[Dict[str, Any]], Awaitable[None]]

class ConfigManager:
    def __init__(
        self,
        root: Path,
        config_rel_path: str = "config/config.yaml",
        env_rel_path: str = ".env",
        rules_rel_path: str = "config/rules.yaml",
    ):
        self._root = root
        self._cfg_path = root / config_rel_path
        self._env_path = root / env_rel_path
        self._rules_path = root / rules_rel_path

        load_dotenv(self._env_path)
        self._cfg = load_config(str(self._cfg_path))
        self._rules = self._load_rules(self._rules_path)

        self._listeners: List[OnChange] = []
        self._rules_listeners: List[OnRulesChange] = []
        self._lock = asyncio.Lock()

    @property
    def cfg(self) -> Config:
        return self._cfg

    @property
    def rules(self) -> Dict[str, Any]:
        return self._rules

    def subscribe(self, listener: OnChange) -> None:
        self._listeners.append(listener)

    def subscribe_rules(self, listener: OnRulesChange) -> None:
        self._rules_listeners.append(listener)

    async def _notify(self) -> None:
        for cb in list(self._listeners):
            try:
                await cb(self._cfg)
            except Exception:
                # Los listeners deben manejar sus errores
                pass

    async def _notify_rules(self) -> None:
        for cb in list(self._rules_listeners):
            try:
                await cb(self._rules)
            except Exception:
                # Los listeners deben manejar sus errores
                pass

    def _load_rules(self, path: Path) -> Dict[str, Any]:
        try:
            if not path.exists():
                return {}
            raw = path.read_text(encoding="utf-8")
            raw = os.path.expandvars(raw)
            data = yaml.safe_load(raw)
            return data or {}
        except Exception:
            # Si hay YAML inválido durante la edición, conservamos las reglas previas
            return getattr(self, "_rules", {}) or {}

    async def reload(self) -> None:
        async with self._lock:
            load_dotenv(self._env_path, override=True)
            self._cfg = load_config(str(self._cfg_path))
            self._rules = self._load_rules(self._rules_path)
        await self._notify()
        await self._notify_rules()

    async def watch(self, enabled: bool, debounce_ms: int = 300) -> None:
        if not enabled:
            return
        debounce = debounce_ms / 1000.0
        watch_set = {str(self._cfg_path), str(self._env_path), str(self._rules_path)}
        async for _changes in awatch(*watch_set):
            await asyncio.sleep(0)
            await asyncio.sleep(debounce)
            await self.reload()
