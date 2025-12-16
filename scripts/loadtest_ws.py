"""
Generador de carga para el servicio de alerts.

Envía eventos sintéticos de trade/depth vía WebSocket a un endpoint
configurable y opcionalmente captura métricas de Prometheus antes y después
para calcular deltas de lag/drops.
"""
from __future__ import annotations

import asyncio
import json
import random
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import aiohttp
from loguru import logger
import websockets

ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS_ROOT = ROOT / "artifacts" / "loadtest"
DEFAULT_METRICS = [
    "oraculo_alerts_queue_dropped_total",
    "oraculo_alerts_queue_discarded_total",
    "oraculo_alerts_queue_depth",
    "oraculo_event_loop_lag_seconds",
]


@dataclass
class LoadTestConfig:
    target_ws: str
    stream: str = "depth"
    rps: int = 1000
    duration: int = 60
    symbols: int = 10
    batch_size: int = 100
    prom_url: Optional[str] = None
    metrics: Optional[List[str]] = None
    artifacts_dir: Optional[Path] = None


@dataclass
class LoadTestResult:
    sent: int
    failed: int
    duration_s: float
    metrics_before: Dict[str, float]
    metrics_after: Dict[str, float]
    metrics_delta: Dict[str, float]
    artifacts_path: Path

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)


async def _scrape_prometheus(url: str, metrics: Iterable[str]) -> Dict[str, float]:
    """Descarga métricas en formato Prometheus text y retorna {nombre: suma}.

    Se suma por nombre ignorando labels para tener un valor agregado rápido.
    """
    wanted = set(metrics)
    out: Dict[str, float] = {m: 0.0 for m in wanted}

    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout, trust_env=True) as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            text = await resp.text()

    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        name_val = line.split()
        if len(name_val) < 2:
            continue
        name_part = name_val[0]
        val_part = name_val[1]
        name = name_part.split("{")[0]
        if name not in wanted:
            continue
        try:
            out[name] += float(val_part)
        except ValueError:
            logger.debug(f"Ignorando línea de métrica no numérica: {line}")
    return out


def _build_trade(symbol: str) -> Dict[str, Any]:
    now_ms = int(time.time() * 1000)
    price = round(random.uniform(30_000, 40_000), 2)
    qty = round(random.uniform(0.01, 1.0), 4)
    buyer_is_maker = random.choice([True, False])
    return {
        "e": "trade",
        "E": now_ms,
        "s": symbol.upper(),
        "t": random.randint(1, 10_000_000),
        "p": str(price),
        "q": str(qty),
        "m": buyer_is_maker,
        "T": now_ms,
    }


def _build_depth(symbol: str, levels: int = 5) -> Dict[str, Any]:
    now_ms = int(time.time() * 1000)
    mid = random.uniform(30_000, 40_000)
    bids = []
    asks = []
    for i in range(levels):
        spread = (i + 1) * 0.5
        bids.append([f"{mid - spread:.2f}", f"{random.uniform(0.1, 5):.4f}"])
        asks.append([f"{mid + spread:.2f}", f"{random.uniform(0.1, 5):.4f}"])
    return {
        "e": "depthUpdate",
        "E": now_ms,
        "s": symbol.upper(),
        "U": random.randint(1, 1_000_000),
        "u": random.randint(1, 1_000_000),
        "b": bids,
        "a": asks,
    }


async def _send_batch(ws: websockets.WebSocketClientProtocol, payloads: List[Dict[str, Any]]) -> int:
    sent = 0
    for msg in payloads:
        try:
            await ws.send(json.dumps({"data": msg}))
            sent += 1
        except Exception as exc:  # pragma: no cover - errores de red en runtime
            logger.warning(f"Fallo enviando mensaje: {exc!s}")
            break
    return sent


def _pick_symbol(symbols: int, idx: int) -> str:
    return f"SYM{(idx % symbols) + 1:03d}"


async def run_loadtest(config: LoadTestConfig) -> LoadTestResult:
    metrics = config.metrics or DEFAULT_METRICS
    artifacts_dir = config.artifacts_dir or (ARTIFACTS_ROOT / time.strftime("%Y%m%d_%H%M%S"))
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    metrics_before: Dict[str, float] = {}
    metrics_after: Dict[str, float] = {}
    metrics_delta: Dict[str, float] = {}

    if config.prom_url:
        try:
            metrics_before = await _scrape_prometheus(config.prom_url, metrics)
        except Exception as exc:  # pragma: no cover - depende del entorno
            logger.warning(f"No se pudieron leer métricas iniciales: {exc!s}")

    total_sent = 0
    total_fail = 0
    start = time.perf_counter()
    end_time = start + float(config.duration)

    stream_choices = [s.strip().lower() for s in config.stream.split(",") if s.strip()]

    try:
        async with websockets.connect(config.target_ws, ping_interval=None) as ws:
            logger.info(
                "Iniciando loadtest: stream=%s rps=%s batch=%s duration=%ss symbols=%s",
                stream_choices,
                config.rps,
                config.batch_size,
                config.duration,
                config.symbols,
            )
            interval = config.batch_size / float(config.rps)
            next_tick = time.perf_counter()
            seq = 0
            while time.perf_counter() < end_time:
                batch: List[Dict[str, Any]] = []
                for _ in range(config.batch_size):
                    symbol = _pick_symbol(config.symbols, seq)
                    seq += 1
                    if "trade" in stream_choices:
                        batch.append(_build_trade(symbol))
                    if "depth" in stream_choices:
                        batch.append(_build_depth(symbol))
                sent = await _send_batch(ws, batch)
                total_sent += sent
                if sent < len(batch):
                    total_fail += (len(batch) - sent)
                next_tick += interval
                await asyncio.sleep(max(0.0, next_tick - time.perf_counter()))
    except Exception as exc:  # pragma: no cover - errores de red en runtime
        logger.error(f"Conexión WS falló: {exc!s}")
        total_fail += 1

    duration_s = time.perf_counter() - start

    if config.prom_url:
        try:
            metrics_after = await _scrape_prometheus(config.prom_url, metrics)
            for k, v in metrics_after.items():
                metrics_delta[k] = v - metrics_before.get(k, 0.0)
        except Exception as exc:  # pragma: no cover
            logger.warning(f"No se pudieron leer métricas finales: {exc!s}")

    result = LoadTestResult(
        sent=total_sent,
        failed=total_fail,
        duration_s=duration_s,
        metrics_before=metrics_before,
        metrics_after=metrics_after,
        metrics_delta=metrics_delta,
        artifacts_path=artifacts_dir,
    )

    out_file = artifacts_dir / "metrics.json"
    out_file.write_text(result.to_json(), encoding="utf-8")
    logger.info(f"Resultados guardados en {out_file}")
    return result


async def main() -> None:  # pragma: no cover - CLI
    import argparse

    parser = argparse.ArgumentParser(description="Loadtest WS para alerts")
    parser.add_argument("--target", dest="target", required=True, help="URL WS de destino (p. ej. ws://localhost:8765/ws)")
    parser.add_argument("--stream", default="depth", help="Streams a enviar, separados por coma (depth,trade)")
    parser.add_argument("--rps", type=int, default=1000, help="Mensajes por segundo objetivo")
    parser.add_argument("--duration", type=int, default=60, help="Duración de la prueba en segundos")
    parser.add_argument("--symbols", type=int, default=10, help="Cantidad de símbolos sintéticos")
    parser.add_argument("--batch-size", type=int, default=100, help="Mensajes por batch para temporización")
    parser.add_argument("--prom-url", default=None, help="Endpoint Prometheus para capturar métricas")
    parser.add_argument(
        "--metrics",
        default=",".join(DEFAULT_METRICS),
        help="Lista separada por coma de métricas a capturar",
    )
    args = parser.parse_args()

    cfg = LoadTestConfig(
        target_ws=args.target,
        stream=args.stream,
        rps=args.rps,
        duration=args.duration,
        symbols=args.symbols,
        batch_size=args.batch_size,
        prom_url=args.prom_url,
        metrics=[m.strip() for m in args.metrics.split(",") if m.strip()],
    )
    result = await run_loadtest(cfg)
    logger.info(result.to_json())


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
