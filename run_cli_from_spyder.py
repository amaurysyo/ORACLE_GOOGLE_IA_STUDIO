#=======================================
# file:  run_cli_from_spyder.py
#=======================================
"""
Created on Fri Oct 31 20:05:54 2025

@author: AMAURY
"""

# E:\OneDrive\... \oraculo_docs_actualizados\run_cli_from_spyder.py
from __future__ import annotations

import os
import sys
from pathlib import Path
from importlib import import_module, reload, util, machinery

def _find_project_root(start: Path) -> Path:
    """
    Busca hacia arriba o adyacente una carpeta que contenga scripts/cli.py.
    """
    candidates = [
        start,
        start.parent,
        start / "oraculo_docs_actualizados",
        start.parent / "oraculo_docs_actualizados",
    ]
    for root in candidates:
        cli_path = root / "scripts" / "cli.py"
        if cli_path.exists():
            return root
    raise RuntimeError("No encuentro 'scripts/cli.py'. Verifica tu árbol de carpetas.")

def _ensure_on_syspath(root: Path) -> None:
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

def _import_cli_module(root: Path):
    """
    Intenta 'import scripts.cli'. Si falla, carga por ruta absoluta (fallback).
    """
    try:
        mod = import_module("scripts.cli")
        # Si es otra ruta inesperada, forzamos la nuestra:
        if not Path(getattr(mod, "__file__", "")).resolve().samefile(root / "scripts" / "cli.py"):
            raise ImportError("Se importó otro 'scripts.cli' distinto al del proyecto.")
    except Exception:
        spec = util.spec_from_file_location(
            "scripts.cli", str(root / "scripts" / "cli.py"), loader=machinery.SourceFileLoader("scripts.cli", str(root / "scripts" / "cli.py"))
        )
        if spec is None or spec.loader is None:
            raise
        mod = util.module_from_spec(spec)
        sys.modules["scripts.cli"] = mod
        spec.loader.exec_module(mod)
    return mod

def _apply_nest_asyncio_if_needed():
    try:
        import asyncio
        asyncio.get_running_loop()
        import nest_asyncio  # type: ignore
        nest_asyncio.apply()
    except Exception:
        pass

def main():
    here = Path(__file__).resolve().parent
    root = _find_project_root(here)
    _ensure_on_syspath(root)
    mod = _import_cli_module(root)
    # Recarga por si has editado cli.py sin reiniciar la consola
    mod = reload(mod)

    print(f"[oráculo] CLI file => {getattr(mod, '__file__', 'unknown')}")
    _apply_nest_asyncio_if_needed()

    # Lee args desde variable de entorno o usa por defecto "ingest run"
    #args = os.getenv("ORACULO_CLI_ARGS", "ingest run").split()  
    args = os.getenv("ORACULO_CLI_ARGS", "alerts run").split()
    # Ejemplos:
    #   os.environ["ORACULO_CLI_ARGS"] = "env doctor --full"
    #   os.environ["ORACULO_CLI_ARGS"] = "ingest run"
    # "env doctor --config config/config.yaml"
    #   os.environ["ORACULO_CLI_ARGS"] = "env deribit --timeout 90"
    mod.cli.main(args, standalone_mode=False)

if __name__ == "__main__":
    main()
