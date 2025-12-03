# -*- coding: utf-8 -*-
"""
Created on Fri Oct 31 20:05:54 2025

@author: AMAURY
"""

# dev/run_cli_from_spyder.py
"""
Ejecuta la CLI desde Spyder sin depender del directorio de trabajo.
Edita la lista de llamadas a tu gusto.
"""
from pathlib import Path
from dotenv import load_dotenv

from scripts.cli import cli, ROOT

# Carga .env desde la raíz del proyecto (independiente del CWD de Spyder)
load_dotenv(ROOT / ".env")

# Ejemplos de ejecución (descomenta/edita):
# cli.main(["health", "--exporter"], standalone_mode=False)
# cli.main(["db", "migrate", "core"], standalone_mode=False)
# cli.main(["db", "migrate", "hotfix"], standalone_mode=False)
# cli.main(["db", "migrate", "bt"], standalone_mode=False)
# cli.main(["db", "refresh-caggs", "--from", "6 hours"], standalone_mode=False)
# cli.main(["telegram", "test", "Hola desde Spyder", "--bot", "events"], standalone_mode=False)
cli.main(["ingest", "run"], standalone_mode=False)
# cli.main(["bt", "sma-smoke", "--venue", "FUTURES", "--tf", "60", "--hours", "6"], standalone_mode=False)

print("OK: run_cli_from_spyder.py terminó de ejecutar.")