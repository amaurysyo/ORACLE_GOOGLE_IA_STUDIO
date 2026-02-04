import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from basis_delta_nrt.notifications.types import NotificationEvent  # noqa: E402


def test_notification_event_normalizes_naive_ts_to_utc():
    naive = datetime(2024, 1, 1, 12, 0, 0)
    event = NotificationEvent(topic="ops", event_type="naive", ts_utc=naive)
    assert event.ts_utc.tzinfo == timezone.utc
    assert event.ts_utc == naive.replace(tzinfo=timezone.utc)


def test_notification_event_converts_timezone_to_utc():
    offset = timezone(timedelta(hours=-3))
    local_ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=offset)
    event = NotificationEvent(topic="ops", event_type="aware", ts_utc=local_ts)
    assert event.ts_utc.tzinfo == timezone.utc
    assert event.ts_utc.hour == 15
