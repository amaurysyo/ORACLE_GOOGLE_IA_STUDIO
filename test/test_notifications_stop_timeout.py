import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from basis_delta_nrt.config import NotificationsCfg  # noqa: E402
from basis_delta_nrt.notifications.engine import NotificationsEngine  # noqa: E402
from basis_delta_nrt.notifications.types import NotificationEvent  # noqa: E402


def test_stop_does_not_block_on_hanging_router():
    async def _run() -> None:
        async def hanging_router(event: NotificationEvent) -> None:
            await asyncio.Event().wait()

        engine = NotificationsEngine(hanging_router, NotificationsCfg())
        engine.start()
        await engine.emit(NotificationEvent(topic="ops", event_type="hang"))

        await asyncio.wait_for(engine.stop(timeout_s=0.1), timeout=0.5)

    import asyncio

    asyncio.run(_run())
