import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from basis_delta.data.mark_loader import _ensure_ts_utc, _to_naive_utc  # noqa: E402


def test_ensure_ts_utc_handles_naive_and_filters_without_type_error():
    start_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_dt = datetime(2024, 1, 3, tzinfo=timezone.utc)

    df = pd.DataFrame(
        {
            "ts": [
                datetime(2023, 12, 31, 23, 59),
                datetime(2024, 1, 1, 12, 0),
                datetime(2024, 1, 2, 0, 0),
            ],
            "mark_price": [1.0, 2.0, 3.0],
            "index_price": [1.1, 2.1, 3.1],
            "funding_rate": [0.01, 0.02, 0.03],
        }
    )

    df["ts"] = _ensure_ts_utc(df["ts"])
    assert str(df["ts"].dt.tz) == "UTC"

    filtered = df[(df["ts"] >= start_dt) & (df["ts"] < end_dt)]
    assert len(filtered) == 2
    assert filtered.iloc[0]["mark_price"] == 2.0


def test_to_naive_utc_strips_tzinfo_but_preserves_moment():
    aware_dt = datetime(2024, 5, 1, 12, 0, tzinfo=timezone.utc)
    naive = _to_naive_utc(aware_dt)
    assert naive.tzinfo is None
    assert naive == aware_dt.replace(tzinfo=None)
