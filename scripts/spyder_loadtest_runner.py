"""
Runner pensado para Spyder: ejecuta el loadtest de alerts con los argumentos
preconfigurados sin depender de bash.

Modo de uso desde Spyder:
1. Abre este archivo.
2. Ajusta TARGET_WS, PROM_URL y los parámetros en LOADTEST_ARGS según tu entorno.
3. Ejecuta el script completo (F5). Usa la consola de Spyder para ver logs y
   métricas (se guardan en artifacts/loadtest/.../metrics.json).

El script delega en run_cli_from_spyder.py, que se encarga de insertar el
proyecto en sys.path, aplicar nest_asyncio si ya hay un loop corriendo y llamar
al comando CLI con los argumentos definidos en ORACULO_CLI_ARGS.
"""
from __future__ import annotations

import os

from run_cli_from_spyder import main as run_cli

# --- Configura aquí según tu entorno local ---
# Si no tienes el servicio real arriba, puedes levantar el mock:
#   python scripts/mock_ws_server.py --host 127.0.0.1 --port 8765 --path /ws --echo
TARGET_WS = "ws://localhost:8765/ws"  # Endpoint WS del servicio de alerts/ingest (o del mock)
PROM_URL = "http://localhost:9001/metrics"  # Exporter Prometheus del servicio alerts

LOADTEST_ARGS = (
    "loadtest alerts "
    f"--target {TARGET_WS} "
    "--stream depth "
    "--rps 1500 "
    "--duration 900 "
    "--symbols 50 "
    "--batch-size 100 "
    f"--prom-url {PROM_URL} "
    "--metrics oraculo_alerts_queue_dropped_total,oraculo_alerts_queue_discarded_total,"
    "oraculo_alerts_queue_depth,oraculo_event_loop_lag_seconds"
)


if __name__ == "__main__":
    # Spyder pasa por aquí cuando haces Run (F5). ORACULO_CLI_ARGS es leído por
    # run_cli_from_spyder.main y ejecuta scripts/cli.py con esos argumentos.
    os.environ["ORACULO_CLI_ARGS"] = LOADTEST_ARGS
    run_cli()
