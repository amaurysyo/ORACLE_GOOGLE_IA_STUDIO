import datetime as dt

from oraculo.alerts.runner import compute_ts_first_bucket


def test_compute_ts_first_bucket_defaults_to_90_seconds():
    event_time = dt.datetime(2024, 1, 1, 0, 1, 40, tzinfo=dt.timezone.utc)
    expected = dt.datetime(2024, 1, 1, 0, 1, 30, tzinfo=dt.timezone.utc)

    assert compute_ts_first_bucket(event_time, None) == expected
    assert compute_ts_first_bucket(event_time, 0) == expected


def test_compute_ts_first_bucket_respects_custom_window():
    event_time = dt.datetime(2024, 1, 1, 0, 5, 29, tzinfo=dt.timezone.utc)
    expected = dt.datetime(2024, 1, 1, 0, 5, 0, tzinfo=dt.timezone.utc)

    assert compute_ts_first_bucket(event_time, 60) == expected
