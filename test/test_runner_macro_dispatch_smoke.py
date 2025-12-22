import asyncio
import datetime as dt
import sys
import types
import time

# Stubs para dependencias opcionales utilizadas sólo durante import de runner
class _StubClientSession:
    pass


if "aiohttp" not in sys.modules:
    sys.modules["aiohttp"] = types.SimpleNamespace(ClientSession=_StubClientSession, ClientTimeout=lambda *args, **kwargs: None)
if "asyncpg" not in sys.modules:
    sys.modules["asyncpg"] = types.SimpleNamespace()
class _StubTelegramError(Exception):
    pass


class _StubBot:
    def __init__(self, *args, **kwargs) -> None:
        pass

    async def send_message(self, *args, **kwargs):
        return None


if "telegram.error" not in sys.modules:
    sys.modules["telegram.error"] = types.SimpleNamespace(TelegramError=_StubTelegramError)
if "telegram" not in sys.modules:
    sys.modules["telegram"] = types.SimpleNamespace(Bot=_StubBot, error=sys.modules["telegram.error"])

from oraculo.alerts import runner
from oraculo.alerts.cpu_worker import WorkerResult
from oraculo.detect.detectors import Event
from oraculo.rules.engine import RuleContext


class _StubTelemetry:
    def __init__(self) -> None:
        self.bump_calls: list[tuple] = []

    def bump(self, ts: float, rule: str, side: str, **kwargs) -> None:  # pragma: no cover - simple recorder
        self.bump_calls.append((ts, rule, side, kwargs))


class _StubRouter:
    def __init__(self) -> None:
        self.sent: list[tuple] = []

    async def send(self, channel: str, text: str, alert_id=None, ts_first=None) -> None:  # pragma: no cover - simple recorder
        self.sent.append((channel, text, alert_id, ts_first))


def test_macro_worker_result_dispatch_triggers_eval_and_enqueue(monkeypatch):
    now = time.time()
    event = Event(
        kind="top_traders",
        side="long",
        ts=now,
        price=27350.0,
        intensity=0.75,
        fields={
            "acc_long_ratio": 0.62,
            "acc_short_ratio": 0.38,
            "pos_long_ratio": 0.58,
            "pos_short_ratio": 0.42,
            "audit": {"src": "fixture"},
        },
    )
    worker_result = WorkerResult(
        kind="macro",
        payload=event,
        processing_seconds=0.01,
        enqueued_at=now - 0.01,
    )
    ctx = RuleContext(instrument_id="BINANCE:PERP:BTCUSDT", profile="EU", suppress_window_s=90)

    eval_calls: list[tuple] = []

    def fake_eval_rules(ev_dict, ctx_arg):
        eval_calls.append((ev_dict, ctx_arg))
        return [
            {"rule": "R30", "side": "long", "context": {"fields": ev_dict.get("fields")}},
        ]

    enqueue_calls: list[tuple] = []

    async def fake_enqueue_rule(rule: dict, event_ts: float):
        enqueue_calls.append((rule, event_ts))
        return 123, dt.datetime.fromtimestamp(event_ts)

    telemetry = _StubTelemetry()
    router = _StubRouter()

    monkeypatch.setattr(runner, "eval_rules", fake_eval_rules)

    asyncio.run(runner.dispatch_macro_event(worker_result.payload, ctx, fake_enqueue_rule, telemetry, router))

    assert len(eval_calls) == 1
    eval_ev, eval_ctx = eval_calls[0]
    assert eval_ev["type"] == "top_traders"
    assert eval_ctx is ctx

    assert len(enqueue_calls) >= 1
    first_rule, first_ts = enqueue_calls[0]
    assert first_rule["rule"] == "R30"
    assert first_ts == worker_result.payload.ts

    assert telemetry.bump_calls, "telemetry.bump should record at least one emission"
    assert router.sent, "router.send should be invoked for emitted alerts"
    assert any("#R30" in sent[1] for sent in router.sent)
