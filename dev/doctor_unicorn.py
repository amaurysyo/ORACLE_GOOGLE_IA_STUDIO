# -*- coding: utf-8 -*-
"""
Created on Fri Oct 31 22:31:19 2025

@author: AMAURY
"""

# dev/doctor_unicorn.py
"""
Verifica import de unicorn_binance_websocket_api y muestra versión/ruta.
Ejecuta en Spyder antes de iniciar la ingesta.
"""
import sys, importlib, pkgutil

print("Python:", sys.executable)

def try_import():
    try:
        from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import (
            BinanceWebSocketApiManager,
        )
        print("OK: import path A (unicorn_binance_websocket_api_manager)")
        return "A", BinanceWebSocketApiManager
    except Exception as e_a:
        print("Fallo A:", repr(e_a))
        try:
            from unicorn_binance_websocket_api.manager import BinanceWebSocketApiManager
            print("OK: import path B (manager)")
            return "B", BinanceWebSocketApiManager
        except Exception as e_b:
            print("Fallo B:", repr(e_b))
            return None, None

which, cls = try_import()
if which:
    m = importlib.import_module(cls.__module__.split(".")[0])
    dist = pkgutil.get_loader(m.__name__)
    print("Modulo:", m.__name__)
    try:
        import importlib.metadata as im
        print("Version:", im.version("unicorn-binance-websocket-api"))
    except Exception:
        pass
    print("Loader:", dist)
else:
    print("ERROR: No se pudo importar unicorn_binance_websocket_api. Revisa instalación/entorno.")
