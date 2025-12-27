"""
Herramienta auxiliar para inspeccionar credenciales de Telegram y ejecutar un
mensaje de prueba.

Ejemplos:
  python scripts/telegram_check.py --show
  python scripts/telegram_check.py --send events --message "Ping desde script"

Uso en Spyder:
  - Define la variable de entorno ORACULO_TG_CHECK_ARGS con los flags deseados
    (ej.: "--show" o "--send events --message hola").
  - Ejecuta este archivo en Spyder; usará esa variable en vez de sys.argv.
"""
from __future__ import annotations

import argparse
import json
import os
import shlex
import sys
from pathlib import Path
from typing import Dict, Mapping, Optional
from urllib import error, request

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_CONFIG_PATH = ROOT / "config" / "config.yaml"

BOT_ENV_KEYS: Mapping[str, Mapping[str, str]] = {
    "events": {"token": "TELEGRAM_BOT_EVENTS_TOKEN", "chat_id": "TELEGRAM_CHAT_EVENTS"},
    "rules": {"token": "TELEGRAM_BOT_RULES_TOKEN", "chat_id": "TELEGRAM_CHAT_RULES"},
    "errors": {"token": "TELEGRAM_BOT_ERRORS_TOKEN", "chat_id": "TELEGRAM_CHAT_ERRORS"},
}

ARG_ENV_VAR = "ORACULO_TG_CHECK_ARGS"


def _mask(value: Optional[str]) -> str:
    if value is None or value == "":
        return "unset"
    text = str(value)
    if len(text) <= 8:
        return text
    head = text[:4]
    tail = text[-4:]
    middle = "*" * (len(text) - 8)
    return f"{head}{middle}{tail}"


def _normalize_chat_id(value: Optional[str]) -> Optional[int | str]:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    if text.startswith("@"):
        return text
    digits = text.lstrip("-")
    if digits.isdigit():
        try:
            return int(text)
        except Exception:
            return None
    return text


def _ensure_root_in_syspath() -> None:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, raw_value = stripped.split("=", 1)
        value = raw_value.strip().strip("'").strip('"')
        os.environ.setdefault(key.strip(), value)


def _load_config_routing(path: Path) -> Mapping[str, Mapping[str, object]]:
    if not path.exists():
        return {}

    bots: Dict[str, Dict[str, str]] = {}
    with path.open("r", encoding="utf-8") as cfg_file:
        lines = cfg_file.readlines()

    in_telegram = False
    telegram_indent: Optional[int] = None
    current_bot: Optional[str] = None
    bot_indent: Optional[int] = None

    for raw_line in lines:
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue

        indent = len(raw_line) - len(raw_line.lstrip(" "))
        stripped = raw_line.strip()

        if stripped.startswith("routing:"):
            in_telegram = False
            current_bot = None
            telegram_indent = None
            bot_indent = None
            continue

        if stripped.startswith("telegram:"):
            in_telegram = True
            telegram_indent = indent
            continue

        if not in_telegram or telegram_indent is None:
            continue

        if indent <= telegram_indent:
            in_telegram = False
            current_bot = None
            bot_indent = None
            continue

        if stripped.endswith(":"):
            current_bot = stripped[:-1]
            bot_indent = indent
            bots.setdefault(current_bot, {})
            continue

        if current_bot and bot_indent is not None and indent > bot_indent and ":" in stripped:
            key, raw_value = stripped.split(":", 1)
            value = raw_value.strip().strip("'").strip('"')
            bots[current_bot][key.strip()] = value

    return bots


def describe_sources(cfg_path: Path) -> Dict[str, Dict[str, object]]:
    _load_env_file(ROOT / ".env")
    config_mapping = _load_config_routing(cfg_path)
    report: Dict[str, Dict[str, object]] = {}

    for name, keys in BOT_ENV_KEYS.items():
        env_token = os.getenv(keys["token"])
        env_chat = os.getenv(keys["chat_id"])

        cfg_bot = config_mapping.get(name) if isinstance(config_mapping, Mapping) else None
        cfg_token = None
        cfg_chat = None
        if isinstance(cfg_bot, Mapping):
            cfg_token = cfg_bot.get("token")
            cfg_chat = cfg_bot.get("chat_id")

        resolved_token = env_token or cfg_token
        resolved_chat = _normalize_chat_id(env_chat or cfg_chat)

        report[name] = {
            "env": {"token": env_token, "chat_id": env_chat},
            "config": {"token": cfg_token, "chat_id": cfg_chat},
            "resolved": {"token": resolved_token, "chat_id": resolved_chat},
        }

    return report


def print_report(cfg_path: Path) -> None:
    report = describe_sources(cfg_path)
    print(f"Config path: {cfg_path}")
    for name, data in report.items():
        env_token = data["env"]["token"]
        env_chat = data["env"]["chat_id"]
        cfg_token = data["config"]["token"]
        cfg_chat = data["config"]["chat_id"]
        resolved_token = data["resolved"]["token"]
        resolved_chat = data["resolved"]["chat_id"]

        print(f"\n[{name}]")
        print(f"  env ({BOT_ENV_KEYS[name]['token']}): {_mask(env_token)}")
        print(f"  env ({BOT_ENV_KEYS[name]['chat_id']}): {env_chat or 'unset'}")
        print(f"  config token: {_mask(cfg_token)}")
        print(f"  config chat_id: {cfg_chat or 'unset'}")
        print(f"  resolved token: {_mask(resolved_token)}")
        print(f"  resolved chat_id: {resolved_chat or 'unset'}")


def send_test_message(report: Mapping[str, Mapping[str, Mapping[str, object]]], target: str, message: str) -> None:
    if target not in report:
        raise ValueError(f"Invalid target '{target}'. Use one of: {', '.join(report.keys())}")

    resolved = report[target].get("resolved", {})
    token = resolved.get("token")
    chat_id = resolved.get("chat_id")

    if not token or chat_id in (None, "", 0):
        raise RuntimeError(f"Missing credentials for target '{target}' (token or chat_id)")

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = json.dumps({"chat_id": chat_id, "text": message, "disable_web_page_preview": True}).encode("utf-8")
    req = request.Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")

    try:
        with request.urlopen(req, timeout=10) as resp:
            body = resp.read().decode("utf-8")
            parsed = json.loads(body)
            message_id = parsed.get("result", {}).get("message_id", "?")
            print(f"Sent message_id={message_id} to chat_id={chat_id}")
            return
    except error.HTTPError as exc:  # pragma: no cover - manual path
        detail = exc.read().decode("utf-8") if exc.fp else str(exc)
        raise RuntimeError(f"Telegram send failed: status={exc.code}, body={detail}") from exc

    raise RuntimeError("Telegram send failed: unknown error")


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspecciona credenciales de Telegram y envía un mensaje opcional")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH, help="Ruta a config.yaml")
    parser.add_argument("--show", action="store_true", help="Imprime el estado de las credenciales")
    parser.add_argument("--send", choices=BOT_ENV_KEYS.keys(), help="Envía un mensaje de prueba al target indicado")
    parser.add_argument("--message", default="Mensaje de prueba desde telegram_check.py", help="Texto del mensaje de prueba")
    return parser.parse_args(argv)


def main() -> None:
    _ensure_root_in_syspath()

    env_args = os.getenv(ARG_ENV_VAR)
    argv = shlex.split(env_args) if env_args else None
    args = parse_args(argv)
    report = describe_sources(args.config)

    if args.show or not args.send:
        print_report(args.config)

    if args.send:
        send_test_message(report, args.send, args.message)


if __name__ == "__main__":
    main()
