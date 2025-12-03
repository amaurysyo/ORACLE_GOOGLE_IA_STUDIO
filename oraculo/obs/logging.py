# ===========================================
# file: oraculo/obs/logging.py
# ===========================================
"""
Utilidades de logging para Oráculo.

- Escribe logs en logs/<name>.log usando Loguru.
- Rotación y retención configurables (por fecha o tamaño).
- Opcionalmente JSON (para Promtail/Loki, etc.).
- En entorno de desarrollo (ENV=dev) también saca logs por consola.
"""

from __future__ import annotations

import sys
import os
from pathlib import Path
from typing import Optional

from loguru import logger


def setup_logging_json(
    root: Path,
    level: str = "INFO",
    rotation: str = "10 MB",
    retention: str = "7 days",
    json_enabled: bool = True,
    name: str = "oraculo",
) -> None:
    """
    Configura Loguru con:
    - salida a fichero logs/<name>.log con rotación y retención configurables
    - salida opcional a stdout para desarrollo

    Parámetros
    ----------
    root : Path
        Ruta raíz del repo (ej. ROOT en scripts/cli.py).
    level : str
        Nivel mínimo: "DEBUG", "INFO", ...
    rotation : str
        Regla de rotación de Loguru. Puede ser:
          - Hora: "00:00" (rotación diaria)
          - Tamaño: "100 MB", "50 MB", etc.  <-- configurar en config.yaml
    retention : str
        Tiempo de retención de ficheros rotados, ej. "7 days", "30 days".
    json_enabled : bool
        Si True, serialize=True (JSON por línea).
        Si False, texto plano.
    name : str
        Nombre base del fichero: logs/<name>.log
    """
    try:
        logs_dir = (root / "logs").resolve()
    except Exception:
        logs_dir = Path("logs").resolve()

    logs_dir.mkdir(parents=True, exist_ok=True)

    # Limpiamos sinks previos
    logger.remove()

    logfile = logs_dir / f"{name}.log"

    # Sink principal a fichero
    logger.add(
        logfile,
        level=level.upper(),
        rotation=rotation,
        retention=retention,
        enqueue=True,        # cola interna (multi-thread / multi-task)
        backtrace=False,
        diagnose=False,
        serialize=json_enabled,
    )

    # Consola en desarrollo (más legible, con colores, sin JSON)
    env = os.getenv("ENV", "dev")
    if env == "dev":
        logger.add(
            sys.stderr,
            level=level.upper(),
            serialize=False,
            enqueue=True,
            backtrace=False,
            diagnose=False,
            colorize=True,
            format=(
                "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
                "<level>{level: <8}</level> | "
                "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
                "<level>{message}</level>"
            ),
        )
