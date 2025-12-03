# =====================================
# file: oraculo/logging.py
# =====================================
from __future__ import annotations
import os
import sys
from pathlib import Path
from loguru import logger

def _default_log_dir() -> Path:
    env = os.environ.get("ORACULO_LOG_DIR")
    if env:
        return Path(env)
    if os.name == "nt":
        base = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData/Local"))
    else:
        base = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local/state"))
    return base / "oraculo" / "logs"


def setup_logging(
    level: str = "INFO",
    rotation: str | int | None = "00:00",
    retention: str | int | None = 10,
    service: str | None = None,
) -> None:
    logger.remove()

    log_dir = _default_log_dir()
    log_dir.mkdir(parents=True, exist_ok=True)

    service = service or os.environ.get("ORACULO_SERVICE") or "main"
    logfile = log_dir / f"oraculo.{service}.log"

    logger.add(sys.stderr, level=level, enqueue=True, backtrace=False, diagnose=False)
    logger.add(
        logfile,
        level=level,
        rotation=rotation,
        retention=retention,
        compression="zip",
        enqueue=True,
        delay=True,
        backtrace=False,
        diagnose=False,
    )

    logger.info(f"Log file -> {logfile} (override con ORACULO_LOG_DIR / ORACULO_SERVICE)")
