# ===========================================
# file: oraculo/logging_colors.py
# ===========================================
from __future__ import annotations

import sys
import re
from pathlib import Path
from typing import Any, Dict

from loguru import logger

# Windows/Spyder: habilitar ANSI siempre
try:
    from colorama import just_fix_windows_console  # type: ignore
    just_fix_windows_console()
except Exception:
    pass


# --- Tag colorizer -----------------------------------------------------------
_TAG = re.compile(r"\[(?P<tag>[A-Za-z0-9:\-_/\.]+)\]")

def _paint_tag(tag: str) -> str:
    # Colores por intercambio/origen (pedido del usuario)
    # - Binance FUTURES WS: azul claro
    # - Binance FUTURES REST (oi/top_traders): amarillo
    # - Binance SPOT WS: cian claro
    # - Deribit (todos): magenta claro
    # - [REST] y [WS]: amarillo / azul
    t = tag

    # REST / WS genéricos
    if t == "REST":
        return "<yellow>[REST]</yellow>"
    if t == "WS":
        return "<blue>[WS]</blue>"

    # batch:* con subtag
    if t.startswith("batch:"):
        subt = t.split(":", 1)[1]
        if subt.startswith(("bfut_oi", "bfut_tt_acc", "bfut_tt_pos")):
            return f"<yellow>[{t}]</yellow>"           # Binance FUTURES REST
        if subt.startswith("bfut_"):
            return f"<light-blue>[{t}]</light-blue>"   # Binance FUTURES WS
        if subt.startswith("bspot_"):
            return f"<light-cyan>[{t}]</light-cyan>"   # Binance SPOT WS
        if subt.startswith("deriv_"):
            return f"<light-magenta>[{t}]</light-magenta>"  # Deribit
        return f"<white>[{t}]</white>"

    # Otros prefijos útiles en tus logs
    if t.startswith(("bfut_", "fut-ws")):
        return f"<light-blue>[{t}]</light-blue>"
    if t.startswith("bspot_"):
        return f"<light-cyan>[{t}]</light-cyan>"
    if t.startswith(("deriv_", "deribit")):
        return f"<light-magenta>[{t}]</light-magenta>"

    return f"<white>[{t}]</white>"


def _colorize_message(msg: str) -> str:
    return _TAG.sub(lambda m: _paint_tag(m.group("tag")), msg)


def _console_format(record: Dict[str, Any]) -> str:
    # Timestamp verde claro, nivel con color propio, todo lo demás blanco salvo tags coloreados.
    ts = record["time"].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    lvl = record["level"].name
    name = record["name"]
    func = record["function"]
    line = record["line"]
    message = _colorize_message(record["message"])
    return (
        f"<light-green>{ts}</light-green> | "
        f"<level>{lvl: <5}</level> | "
        f"{name}:{func}:{line} - "
        f"{message}\n"
    )


def setup_colored_console_from_cfg(cfg: Any) -> None:
    """
    Configura:
      - Consola con colores forzados (fecha verde, INFO azul, WARNING amarillo, ERROR rojo),
        tags coloreados por exchange/origen.
      - (Opcional) sink JSON en logs/app.jsonl según cfg.observability.logging.*
    """
    logcfg = (getattr(cfg, "observability", None) or {}).get("logging", {}) or {}
    level = logcfg.get("level", "INFO")
    json_enabled = bool(logcfg.get("json", False))
    rotation = logcfg.get("rotation", "00:00")
    retention = logcfg.get("retention", "7 days")

    # Niveles con color explícito
    logger.level("DEBUG", color="<white>")
    logger.level("INFO", color="<light-blue>")      # INFO azul claro
    logger.level("WARNING", color="<yellow>")       # WARNING amarillo
    logger.level("ERROR", color="<red>")            # ERROR rojo
    logger.level("SUCCESS", color="<green>")

    # Reset sinks y añadir consola coloreada
    logger.remove()
    logger.add(
        sys.stdout,
        level=level,
        colorize=True,               # fuerza ANSI incluso si no es TTY (Spyder)
        backtrace=False,
        diagnose=False,
        format=_console_format,      # callable -> podemos pintar tags sin ensuciar JSON
        enqueue=False,
    )

    # Sink JSON opcional a archivo
    if json_enabled:
        root = Path(__file__).resolve().parents[2]  # repo root aproximado (../.. desde oraculo/)
        logdir = root / "logs"
        logdir.mkdir(parents=True, exist_ok=True)
        logger.add(
            logdir / "app.jsonl",
            level=level,
            rotation=rotation,
            retention=retention,
            serialize=True,          # JSON line por evento
            backtrace=False,
            diagnose=False,
            enqueue=True,
        )
