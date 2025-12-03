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
from scripts.cli import cli
cli.main(["env","doctor","--full"], standalone_mode=False)
print("OK: run_cli_from_spyder.py terminó de ejecutar.")