# ===========================================
# file: oraculo/obs/metrics.py
# ===========================================
"""
Created on Fri Oct 31 19:34:01 2025

@author: AMAURY
"""

# oraculo/obs/metrics.py
from __future__ import annotations

import asyncio
import os
import threading
import time
from typing import Dict

from loguru import logger
from prometheus_client import Counter, Gauge, Summary, start_http_server, Histogram

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

# Alertas: tamaños de cola y tiempos por etapa (hot path)
alerts_queue_depth = Gauge(
    "oraculo_alerts_queue_depth", "Queue depth for alerts hot path", ["stream"],
)
alerts_queue_backpressure_skipped_total = Counter(
    "oraculo_alerts_queue_backpressure_skipped_total",
    "Events skipped due to backpressure in alerts hot path",
    ["kind"],
)
alerts_queue_time_seconds = Histogram(
    "oraculo_alerts_queue_time_seconds",
    "Time spent in alerts queues before being processed",
    ["stream"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10),
)
alerts_queue_time_latest_seconds = Gauge(
    "oraculo_alerts_queue_time_latest_seconds",
    "Latest observed time in queue for alerts stream",
    ["stream"],
)
alerts_batch_duration_seconds = Histogram(
    "oraculo_alerts_batch_duration_seconds",
    "Batch processing duration by alerts stream",
    ["stream", "worker"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2),
)
alerts_queue_discarded_total = Counter(
    "oraculo_alerts_queue_discarded_total",
    "Events discarded in alerts pipeline by cause",
    ["stream", "cause"],
)
alerts_stage_duration_ms = Histogram(
    "oraculo_alerts_stage_duration_ms",
    "Stage duration in milliseconds for alerts pipeline",
    ["stage"],
    buckets=(1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000),
)
alerts_stage_rows_total = Counter(
    "oraculo_alerts_stage_rows_total",
    "Rows/events processed per stage in alerts pipeline",
    ["stage", "worker"],
)
alerts_stage_yields_total = Counter(
    "oraculo_alerts_stage_yields_total",
    "Cooperative yields per stage in alerts pipeline",
    ["stage"],
)
alerts_queue_dropped_total = Counter(
    "oraculo_alerts_queue_dropped_total",
    "Dropped events in alerts hot path queue",
)

# Alertas: persistencia y lock/handler profiling
alerts_db_queue_time_seconds = Histogram(
    "oraculo_alerts_db_queue_time_seconds",
    "Time DB requests spend waiting in alerts DB queue before processing",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2),
)
alerts_db_queue_depth = Gauge(
    "oraculo_alerts_db_queue_depth",
    "Queue depth for alerts DB writer",
)
alerts_db_batch_duration_seconds = Histogram(
    "oraculo_alerts_db_batch_duration_seconds",
    "DB batch processing duration by kind in alerts pipeline",
    ["kind"],
    buckets=(0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000),
)
alerts_db_inflight_batches = Gauge(
    "oraculo_alerts_db_inflight_batches",
    "Inflight DB batches being processed concurrently",
)
alerts_engine_lock_seconds = Histogram(
    "oraculo_alerts_engine_lock_seconds",
    "Time spent holding engine_lock in alerts pipeline",
    ["stage"],
    buckets=(0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1),
)
alerts_handler_duration_seconds = Histogram(
    "oraculo_alerts_handler_duration_seconds",
    "Handler execution duration per stream (includes lock time)",
    ["stream", "worker"],
    buckets=(0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1, 2),
)
alerts_stage_yield_gap_seconds = Histogram(
    "oraculo_alerts_stage_yield_gap_seconds",
    "Elapsed time between cooperative yields within a stage",
    ["stage"],
    buckets=(0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25),
)
alerts_snapshot_duration_seconds = Histogram(
    "oraculo_alerts_snapshot_duration_seconds",
    "Duration of snapshot+flush cycle in alerts pipeline",
    buckets=(0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5),
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


cpu_process_seconds_total = Counter(
    "oraculo_cpu_process_seconds_total",
    "Total process CPU time (seconds)",
    ["service", "exported_service"],
)
cpu_main_thread_seconds_total = Counter(
    "oraculo_cpu_main_thread_seconds_total",
    "Total main-thread CPU time (seconds)",
    ["service", "exported_service"],
)
cpu_available_cores = Gauge(
    "oraculo_cpu_available_cores",
    "Available CPU cores reported by the OS",
    ["service", "exported_service"],
)
python_thread_count = Gauge(
    "oraculo_python_thread_count",
    "Active Python thread count",
    ["service", "exported_service"],
)
threads_active = Gauge(
    "oraculo_threads_active",
    "Active thread count",
    ["service", "exported_service"],
)
threads_alive = Gauge(
    "oraculo_threads_alive",
    "Number of alive Python threads (threading.active_count())",
    ["service", "exported_service"],
)
cpu_process_cores = Gauge(
    "oraculo_cpu_process_cores",
    "Effective CPU cores used by the process (delta_cpu/delta_wall)",
    ["service", "exported_service"],
)
cpu_main_thread_cores = Gauge(
    "oraculo_cpu_main_thread_cores",
    "Effective CPU cores used by the main thread (delta_thread_cpu/delta_wall)",
    ["service", "exported_service"],
)
cpu_main_thread_share = Gauge(
    "oraculo_cpu_main_thread_share",
    "Main-thread CPU share over process CPU (delta_thread_cpu/delta_process_cpu)",
    ["service", "exported_service"],
)

to_thread_submitted_total = Counter(
    "oraculo_to_thread_submitted_total",
    "asyncio.to_thread submissions by task",
    ["service", "exported_service", "task"],
)
to_thread_started_total = Counter(
    "oraculo_to_thread_started_total",
    "asyncio.to_thread executions that reached a worker thread",
    ["service", "exported_service", "task"],
)
to_thread_completed_total = Counter(
    "oraculo_to_thread_completed_total",
    "asyncio.to_thread executions completed (success or error)",
    ["service", "exported_service", "task"],
)
to_thread_wall_seconds = Histogram(
    "oraculo_to_thread_wall_seconds",
    "Wall-clock duration of asyncio.to_thread tasks",
    ["service", "exported_service", "task"],
    buckets=(0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5),
)
to_thread_thread_cpu_seconds = Histogram(
    "oraculo_to_thread_thread_cpu_seconds",
    "Thread CPU duration of asyncio.to_thread tasks",
    ["service", "exported_service", "task"],
    buckets=(0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5),
)
to_thread_no_cpu_seconds = Histogram(
    "oraculo_to_thread_no_cpu_seconds",
    "Non-CPU time (wall minus thread CPU) spent in asyncio.to_thread tasks",
    ["service", "exported_service", "task"],
    buckets=(0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5),
)
to_thread_inflight = Gauge(
    "oraculo_to_thread_inflight",
    "Inflight asyncio.to_thread tasks",
    ["service", "exported_service", "task"],
)
to_thread_queue_depth = Gauge(
    "oraculo_to_thread_queue_depth",
    "Queued asyncio.to_thread tasks pending worker execution",
    ["service", "exported_service", "task"],
)
to_thread_pending = Gauge(
    "oraculo_to_thread_pending",
    "asyncio.to_thread tasks submitted but not yet started",
    ["service", "exported_service", "task"],
)
to_thread_exceptions_total = Counter(
    "oraculo_to_thread_exceptions_total",
    "Exceptions raised by asyncio.to_thread tasks",
    ["service", "exported_service", "task"],
)

# ---------- Rules ----------
rule_eval_total = Counter("oraculo_rule_eval_total", "Rule evaluations", ["rule"])
rule_eval_errors_total = Counter(
    "oraculo_rule_eval_errors_total", "Rule evaluation errors", ["rule", "kind"]
)
rule_alerts_upsert_total = Counter(
    "oraculo_rule_alerts_upsert_total",
    "Upserts into rule_alerts by rule and severity",
    ["rule", "severity"],
)
rule_alert_lag_seconds = Histogram(
    "oraculo_rule_alert_lag_seconds",
    "Lag between event_time and alert insertion (seconds)",
    ["rule"],
    buckets=(0.1, 0.5, 1, 2, 5, 10, 20, 30, 60, 120, 300),
)
rule_event_age_seconds = Gauge(
    "oraculo_rule_event_age_seconds",
    "Age of last observed event for rule (seconds)",
    ["rule"],
)
rule_watermark_event_time = Gauge(
    "oraculo_rule_watermark_event_time",
    "Event time watermark for last processed event (epoch seconds)",
    ["rule"],
)

# ---------- Dispatch (Telegram) ----------
dispatch_attempts_total = Counter(
    "oraculo_dispatch_attempts_total", "Dispatch attempts", ["channel"]
)
dispatch_success_total = Counter(
    "oraculo_dispatch_success_total", "Successful dispatches", ["channel"]
)
dispatch_fail_total = Counter(
    "oraculo_dispatch_fail_total", "Failed dispatches", ["channel", "kind"]
)
dispatch_dropped_total = Counter(
    "oraculo_dispatch_dropped_total", "Dropped dispatches", ["channel", "reason"]
)
dispatch_last_attempt_ts = Gauge(
    "oraculo_dispatch_last_attempt_timestamp_seconds",
    "Timestamp of last dispatch attempt",
    ["channel"],
)
dispatch_last_success_ts = Gauge(
    "oraculo_dispatch_last_success_timestamp_seconds",
    "Timestamp of last successful dispatch",
    ["channel"],
)

# ---------- Event loop lag ----------
event_loop_lag_seconds = Gauge(
    "oraculo_event_loop_lag_seconds",
    "Asyncio event-loop lag in seconds (timer drift). Indicates event loop starvation/blocking.",
    ["service"],
)
alerts_uncaught_errors_total = Counter(
    "oraculo_alerts_uncaught_errors_total",
    "Uncaught errors in alerts service components",
    ["service", "exported_service", "where"],
)

# Mantener una tarea por servicio para evitar monitores duplicados
_loop_lag_tasks: Dict[str, asyncio.Task] = {}


async def _monitor_event_loop_lag(service: str, period: float = 1.0) -> None:
    loop = asyncio.get_running_loop()
    expected = loop.time() + period
    while True:
        await asyncio.sleep(period)
        now = loop.time()
        lag = now - expected
        if lag < 0:
            lag = 0.0
        event_loop_lag_seconds.labels(service=service).set(lag)
        expected += period


def start_event_loop_lag_monitor(service: str, period: float = 1.0) -> None:
    """Arranca monitor de lag del event loop. Idempotente por servicio."""

    task = _loop_lag_tasks.get(service)
    if task is not None and not task.done():
        return
    loop = asyncio.get_running_loop()
    _loop_lag_tasks[service] = loop.create_task(
        _monitor_event_loop_lag(service, period),
        name=f"loop-lag:{service}",
    )
    _loop_lag_tasks[service].add_done_callback(
        lambda t: logger.warning(
            "event loop lag monitor for %s stopped: %s", service, t.exception()
        )
        if t.cancelled() is False and t.exception() is not None
        else None
    )


# PromQL de verificación (alerts):
#   rate(oraculo_cpu_process_seconds_total{service="alerts"}[1m])
#   rate(oraculo_cpu_main_thread_seconds_total{service="alerts"}[1m])
#     / rate(oraculo_cpu_process_seconds_total{service="alerts"}[1m])
#   oraculo_to_thread_queue_depth{service="alerts"}
#   histogram_quantile(
#       0.95,
#       sum by (le, task) (rate(oraculo_to_thread_no_cpu_seconds_bucket{service="alerts"}[5m]))
#   )
#   rate(oraculo_to_thread_thread_cpu_seconds_sum{service="alerts"}[5m])
#     / rate(oraculo_to_thread_wall_seconds_sum{service="alerts"}[5m])
#   oraculo_event_loop_lag_seconds{service="alerts"}


async def cpu_sampler_loop(
    service: str, exported_service: str | None = None, interval_s: float = 1.0
) -> None:
    """
    Periodically exports CPU/GIL contention signals:
      - oraculo_cpu_process_seconds_total{service,exported_service}
      - oraculo_cpu_main_thread_seconds_total{service,exported_service}
      - oraculo_cpu_available_cores{service,exported_service}
      - oraculo_python_thread_count{service,exported_service}
      - oraculo_threads_active{service,exported_service}
      - oraculo_cpu_process_cores{service,exported_service}
      - oraculo_cpu_main_thread_cores{service,exported_service}
      - oraculo_cpu_main_thread_share{service,exported_service}
      - oraculo_threads_alive{service,exported_service}

    Must run in the main event-loop thread to make main-thread cpu meaningful.
    """

    labels = {"service": service, "exported_service": exported_service or service}
    last_wall = time.perf_counter()
    prev_proc = time.process_time()
    try:
        thread_time_fn = time.thread_time
        prev_thread = thread_time_fn()
    except Exception:
        # Fallback to process_time() if thread_time() is unavailable
        thread_time_fn = time.process_time
        prev_thread = prev_proc

    cores = os.cpu_count() or 1
    cpu_available_cores.labels(**labels).set(float(cores))

    while True:
        await asyncio.sleep(interval_s)

        wall_now = time.perf_counter()
        proc_now = time.process_time()
        thread_now = thread_time_fn()

        dt_wall = wall_now - last_wall
        proc_delta = proc_now - prev_proc
        thread_delta = thread_now - prev_thread

        last_wall = wall_now
        prev_proc = proc_now
        prev_thread = thread_now

        if dt_wall <= 0 or proc_delta < 0 or thread_delta < 0:
            continue

        cpu_process_seconds_total.labels(**labels).inc(proc_delta)
        cpu_main_thread_seconds_total.labels(**labels).inc(thread_delta)

        proc_cores = proc_delta / dt_wall
        thread_cores = thread_delta / dt_wall
        share = (thread_delta / proc_delta) if proc_delta > 0 else 0.0

        cpu_process_cores.labels(**labels).set(proc_cores)
        cpu_main_thread_cores.labels(**labels).set(thread_cores)
        cpu_main_thread_share.labels(**labels).set(share)

        active_threads = threading.active_count()
        python_thread_count.labels(**labels).set(active_threads)
        threads_active.labels(**labels).set(active_threads)
        threads_alive.labels(**labels).set(active_threads)


async def start_cpu_monitors(service: str, interval_s: float = 1.0) -> None:
    """Backward-compatible alias for cpu_sampler_loop."""

    await cpu_sampler_loop(service=service, exported_service=service, interval_s=interval_s)


# ---------- DB ----------
db_upsert_rule_alert_ms = Histogram(
    "oraculo_db_upsert_rule_alert_ms",
    "Latency of upsert_rule_alert DB call (ms)",
    buckets=(1, 2, 5, 10, 20, 50, 100, 200, 500, 1000),
)
db_insert_dispatch_log_ms = Histogram(
    "oraculo_db_insert_dispatch_log_ms",
    "Latency of alert_dispatch_log insert (ms)",
    buckets=(1, 2, 5, 10, 20, 50, 100, 200, 500, 1000),
)


_exporter_running = False


def run_exporter(port: int = 9000) -> None:
    """Idempotente: solo arranca una vez."""
    global _exporter_running
    if _exporter_running:
        return
    env_port = os.getenv("ORACULO_METRICS_PORT")
    if env_port:
        try:
            port = int(env_port)
        except ValueError:
            logger.warning(
                f"[metrics] Invalid ORACULO_METRICS_PORT={env_port!r}, using default {port}"
            )
    try:
        start_http_server(port)
        _exporter_running = True
        logger.info(f"[metrics] Exporter started on port {port}")
    except OSError as e:
        logger.error(f"[metrics] Failed to start exporter on port {port}: {e!s}")
