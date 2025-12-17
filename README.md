# -*- coding: utf-8 -*-
"""
Created on Fri Oct 31 19:27:26 2025

@author: AMAURY
"""

# README.md  # why: guía rápida
# 1) conda create -n oraculo python=3.11 -y && conda activate oraculo
# 2) pip install -r requirements.txt
# 3) cp .env.example .env  # y edita PG_DSN + tokens
# 4) python scripts/cli.py health
# 5) python scripts/cli.py db:migrate core|hotfix|bt
# 6) python scripts/cli.py db:refresh-caggs --from "6 hours"
# 7) python scripts/cli.py telegram:test "Hola"
## Cómo correr el loadtest desde Spyder

1. Abre `scripts/spyder_loadtest_runner.py` en Spyder.
2. Ajusta `TARGET_WS` y `PROM_URL` para tu entorno local (por ejemplo `ws://localhost:8765/ws` y `http://localhost:9001/metrics`).
3. Si quieres modificar la carga, edita `LOADTEST_ARGS` (RPS, duración, símbolos, métricas, etc.).
4. Ejecuta el archivo completo con F5. El runner usa `run_cli_from_spyder.py` para invocar `scripts/cli.py` con los argumentos configurados, sin depender de la terminal.
5. Los resultados se guardan en `artifacts/loadtest/<timestamp>/metrics.json` y los logs aparecen en la consola de Spyder.

### ¿Y si el WebSocket no está arriba?

- El load test **no** crea el servidor WS: debes apuntar `TARGET_WS` al endpoint real de tu servicio (p. ej. `alerts` o `ingest`).
- Para pruebas rápidas locales, levanta el servidor WS de prueba incluido:
  ```bash
  python scripts/mock_ws_server.py --host 127.0.0.1 --port 8765 --path /ws --echo
  ```
  Luego ajusta `TARGET_WS = "ws://127.0.0.1:8765/ws"` en `scripts/spyder_loadtest_runner.py` y vuelve a ejecutar (F5).
- Si usas el servicio real, asegúrate de haberlo iniciado antes de correr el load test (por ejemplo `python scripts/cli.py alerts run` o el proceso que exponga tu WS).
