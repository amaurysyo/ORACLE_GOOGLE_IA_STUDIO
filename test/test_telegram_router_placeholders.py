import os

import pytest

from oraculo.rules.router import TelegramRouter


@pytest.fixture(autouse=True)
def clear_env(monkeypatch):
    keys = [
        "TELEGRAM_BOT_RULES_TOKEN",
        "TELEGRAM_CHAT_RULES",
    ]
    for key in keys:
        monkeypatch.delenv(key, raising=False)
    yield


def test_placeholder_falls_back_to_env(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_RULES_TOKEN", "123:ABC")
    monkeypatch.setenv("TELEGRAM_CHAT_RULES", "1381564462")
    cfg = {
        "telegram": {
            "bot_rules": {
                "token": "${TELEGRAM_BOT_RULES_TOKEN}",
                "chat_id": "${TELEGRAM_CHAT_RULES}",
            }
        }
    }

    router = TelegramRouter(cfg)

    assert router._targets["rules"]["token"] == "123:ABC"
    assert router._targets["rules"]["chat_id"] == 1381564462


def test_placeholder_without_env_results_in_none():
    cfg = {
        "telegram": {
            "bot_rules": {
                "token": "${TELEGRAM_BOT_RULES_TOKEN}",
                "chat_id": "${TELEGRAM_CHAT_RULES}",
            }
        }
    }

    router = TelegramRouter(cfg)

    assert router._targets["rules"]["token"] in (None, "")
    assert router._targets["rules"]["chat_id"] is None


def test_real_config_value_has_priority(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_RULES_TOKEN", "env-token")
    monkeypatch.setenv("TELEGRAM_CHAT_RULES", "999999999")
    cfg = {
        "telegram": {
            "bot_rules": {
                "token": "real_token",
                "chat_id": "1381564462",
            }
        }
    }

    router = TelegramRouter(cfg)

    assert router._targets["rules"]["token"] == "real_token"
    assert router._targets["rules"]["chat_id"] == 1381564462
