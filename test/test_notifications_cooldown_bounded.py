import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from basis_delta_nrt.config import NotificationsCfg  # noqa: E402
from basis_delta_nrt.notifications.engine import NotificationsEngine  # noqa: E402
from basis_delta_nrt.notifications.types import NotificationEvent  # noqa: E402


def test_cooldown_map_is_bounded():
    async def _run() -> None:
        cfg = NotificationsCfg(
            cooldown_s_by_type={"ping": 60},
            cooldown_max_keys=50,
            cooldown_prune_every_n=1,
            queue_max=500,
        )

        async def router(event: NotificationEvent) -> None:
            return None

        engine = NotificationsEngine(router, cfg)
        now = datetime(2024, 1, 1, tzinfo=timezone.utc)
        for idx in range(200):
            event = NotificationEvent(topic="ops", event_type="ping", ts_utc=now, key=f"k{idx}")
            await engine.emit(event)

        assert len(engine._last_sent_by_key) <= cfg.cooldown_max_keys

    import asyncio

    asyncio.run(_run())
