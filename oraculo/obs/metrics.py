# ===========================================
# file: oraculo/obs/metrics.py
# ===========================================
"""
Created on Fri Oct 31 19:34:01 2025

@author: AMAURY
"""

# oraculo/obs/metrics.py
from __future__ import annotations

from prometheus_client import Counter, Gauge, Summary, start_http_server,Histogram

# WS mensajes por tipo de evento
ws_msgs_total = Counter("oraculo_ws_msgs_total", "WS messages by event type", ["event"])

# Resyncs efectuados (gap/heartbeat)
ws_resync_total = Counter(
    "oraculo_ws_resync_total", "WS resyncs performed", ["venue", "stream"]
)
ws_heartbeat_miss_total = Counter(
    "oraculo_ws_heartbeat_miss_total", "WS heartbeat misses", ["venue", "stream"]
)

# Tiempos de inserción DB (ms)
db_insert_ms = Summary("oraculo_db_insert_ms", "DB batch insert time (ms)")

# Backlog de colas de batch
queue_backlog = Gauge("oraculo_batch_queue_backlog", "Batch queue backlog", ["queue"])

# Lag entre ts del evento y reloj local (ms)
ws_msg_lag_ms = Summary(
    "oraculo_ws_msg_lag_ms",
    "Lag between WS event time and local time (ms)",
    ["venue", "stream"],
)

# Edad del último mensaje por stream (segundos)
ws_last_msg_age_s = Gauge(
    "oraculo_ws_last_msg_age_seconds",
    "Seconds since last WS message",
    ["venue", "stream"],
)

# Errores de ingesta (REST/WS)
ingest_errors_total = Counter(
    "oraculo_ingest_errors_total",
    "Ingest errors by component and kind",
    ["component", "kind"],
)

# Reconexiones / resubscripciones de WS
ws_reconnects_total = Counter(
    "oraculo_ws_reconnects_total",
    "WS reconnects/resubs by venue and stream",
    ["venue", "stream"],
)

#  Retardo en colas del batcher
batch_queue_delay_ms = Summary(
    "oraculo_batch_queue_delay_ms",
    "Time rows spend in AsyncBatcher queue before flushing (ms)",
    ["queue"],
)

http_fail_total = Counter(
    "oraculo_http_fail_total",
    "HTTP failures by service and reason",
    ["service", "reason"],
)


http_latency_ms = Histogram(
    "oraculo_http_latency_ms",
    "HTTP request latency in milliseconds",
    ["service", "op"],
    buckets=(5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000),
)


_exporter_running = False


def run_exporter(port: int = 9000) -> None:
    """Idempotente: solo arranca una vez."""
    global _exporter_running
    if _exporter_running:
        return
    start_http_server(port)
    _exporter_running = True
