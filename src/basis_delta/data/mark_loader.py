from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable, Sequence

import pandas as pd


def _ensure_dt_utc(value: datetime | str | pd.Timestamp) -> datetime:
    """Return a timezone-aware UTC datetime for consistent comparisons."""
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize(timezone.utc)
    else:
        ts = ts.tz_convert(timezone.utc)
    return ts.to_pydatetime()


def _ensure_ts_utc(series: pd.Series) -> pd.Series:
    """Ensure a pandas Series of timestamps is timezone-aware UTC."""
    ts = pd.to_datetime(series, utc=True)
    return ts.dt.tz_convert(timezone.utc)


def _to_naive_utc(value: datetime) -> datetime:
    """Return tz-naive datetime representing UTC, for Postgres timestamp without time zone."""
    dt = _ensure_dt_utc(value)
    return dt.replace(tzinfo=None)


def _iter_table_names(table_pattern: str, start_dt: datetime, end_dt: datetime) -> Iterable[str]:
    """Yield table names for each day in the inclusive date range."""
    start_date = _ensure_dt_utc(start_dt).date()
    end_date = (_ensure_dt_utc(end_dt) - timedelta(seconds=1)).date()

    current = start_date
    while current <= end_date:
        yield table_pattern.format(date=current)
        current += timedelta(days=1)


def _load_from_db(
    conn,
    start_dt: datetime,
    end_dt: datetime,
    table_pattern: str | None = None,
    table_names: Sequence[str] | None = None,
) -> pd.DataFrame:
    """
    Load mark/index/funding data from Postgres daily tables.

    Parameters
    ----------
    conn:
        psycopg connection.
    start_dt/end_dt:
        Range (inclusive/exclusive) to load, expected UTC.
    table_pattern:
        Format pattern with `{date}` placeholder for daily tables.
    table_names:
        Explicit table names to query (if no pattern is provided).
    """
    start_dt = _ensure_dt_utc(start_dt)
    end_dt = _ensure_dt_utc(end_dt)
    q_start = _to_naive_utc(start_dt)
    q_end = _to_naive_utc(end_dt)

    if table_pattern:
        tables: Iterable[str] = _iter_table_names(table_pattern, start_dt, end_dt)
    else:
        tables = table_names or []

    frames: list[pd.DataFrame] = []
    query = """
        SELECT ts, mark_price, index_price, funding_rate
        FROM {table}
        WHERE ts >= %(start)s AND ts < %(end)s
        ORDER BY ts
    """

    for table in tables:
        frame = pd.read_sql_query(query.format(table=table), conn, params={"start": q_start, "end": q_end})
        if frame.empty:
            continue
        frame["ts"] = _ensure_ts_utc(frame["ts"])
        frames.append(frame)

    if not frames:
        return pd.DataFrame(columns=["ts", "mark_price", "index_price", "funding_rate"])

    df = pd.concat(frames, ignore_index=True)
    df["ts"] = _ensure_ts_utc(df["ts"])

    start_ts = pd.Timestamp(start_dt).tz_convert(timezone.utc)
    end_ts = pd.Timestamp(end_dt).tz_convert(timezone.utc)
    return df[(df["ts"] >= start_ts) & (df["ts"] < end_ts)]
