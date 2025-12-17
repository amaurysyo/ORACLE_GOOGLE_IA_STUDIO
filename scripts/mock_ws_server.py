"""
Servidor WS de prueba para el loadtest.

- Acepta conexiones en host/puerto configurables (por defecto 127.0.0.1:8765).
- Ruta por defecto "/ws" para que coincida con TARGET_WS del runner de Spyder.
- Solo consume los mensajes y opcionalmente los imprime; no hace parsing de negocio.

Uso:
    python scripts/mock_ws_server.py --host 127.0.0.1 --port 8765 --path /ws --echo

Activa --echo para devolver el mismo mensaje (útil para validar que llegan los payloads).
"""
from __future__ import annotations

import argparse
import asyncio
from functools import partial
from typing import Optional

from loguru import logger
import websockets


async def _handler(websocket: websockets.WebSocketServerProtocol, path: str, *, echo: bool, verbose: bool) -> None:
    client = f"{websocket.remote_address} {path}"
    logger.info(f"[mock-ws] conexión abierta: {client}")
    try:
        async for message in websocket:
            if verbose:
                logger.info(f"[mock-ws] mensaje recibido ({len(message)} bytes)")
            if echo:
                await websocket.send(message)
    except Exception as exc:  # pragma: no cover - depende del runtime
        logger.warning(f"[mock-ws] conexión cerrada con error: {exc!s}")
    finally:
        logger.info(f"[mock-ws] conexión cerrada: {client}")


async def main(host: str, port: int, path: str, echo: bool, verbose: bool) -> None:
    logger.info(f"[mock-ws] escuchando en ws://{host}:{port}{path} (echo={echo})")
    handler = partial(_handler, echo=echo, verbose=verbose)
    async with websockets.serve(handler, host, port, ping_interval=None, process_request=None, subprotocols=None, path=path):
        await asyncio.Future()  # run forever


if __name__ == "__main__":  # pragma: no cover - CLI utility
    parser = argparse.ArgumentParser(description="Servidor WS mínimo para probar loadtest")
    parser.add_argument("--host", default="127.0.0.1", help="Host de escucha")
    parser.add_argument("--port", type=int, default=8765, help="Puerto de escucha")
    parser.add_argument("--path", default="/ws", help="Ruta del endpoint WS")
    parser.add_argument("--echo", action="store_true", help="Devolver el mensaje recibido")
    parser.add_argument("--verbose", action="store_true", help="Loguear cada payload recibido")
    args = parser.parse_args()

    try:
        asyncio.run(main(args.host, args.port, args.path, args.echo, args.verbose))
    except KeyboardInterrupt:
        logger.info("[mock-ws] detenido por usuario")
