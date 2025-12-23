from __future__ import annotations

import datetime as dt
import math
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from loguru import logger


def _clamp(x: Optional[float], bounds: Tuple[float, float]) -> Optional[float]:
    if x is None:
        return None
    lo, hi = bounds
    try:
        return max(lo, min(hi, float(x)))
    except Exception:
        return None


@dataclass
class SurfaceBuilderCfg:
    enabled: bool = False
    poll_s: float = 30.0
    lookback_s: float = 600.0
    lag_s: float = 60.0
    underlying: str = "BTC"
    max_expiries_per_bucket: int = 3
    delta_target: float = 0.25
    delta_tolerance: float = 0.05
    min_oi: float = 0.0
    min_quotes: int = 0
    use_oi_weight: bool = True
    clamp_iv: Tuple[float, float] = (0.0, 5.0)
    clamp_rr: Tuple[float, float] = (-2.0, 2.0)
    clamp_bf: Tuple[float, float] = (-2.0, 2.0)


class DeribitSurfaceBuilder:
    def __init__(self, cfg: SurfaceBuilderCfg) -> None:
        self.cfg = cfg
        self._last_bucket_ts: Optional[dt.datetime] = None

    @staticmethod
    def _floor_minute(ts: dt.datetime) -> dt.datetime:
        return ts.replace(second=0, microsecond=0)

    @staticmethod
    def _bucket_tenor(days: float) -> Optional[str]:
        if days <= 0:
            return None
        if days <= 7:
            return "0_7d"
        if days <= 30:
            return "7_30d"
        if days <= 90:
            return "30_90d"
        if days <= 180:
            return "90_180d"
        return None

    @staticmethod
    def _bucket_moneyness(m: float) -> Optional[str]:
        buckets = [
            (0.90, 0.95, "0.90_0.95"),
            (0.95, 1.00, "0.95_1.00"),
            (1.00, 1.05, "1.00_1.05"),
            (1.05, 1.10, "1.05_1.10"),
        ]
        for lo, hi, name in buckets:
            if lo <= m < hi:
                return name
        return None

    async def _fetch_spot(self, db: Any, bucket_ts: dt.datetime) -> Optional[float]:
        sql = """
            SELECT underlying_price
            FROM deribit.options_ticker
            WHERE event_time >= $2 - interval '1 seconds' * $3
              AND instrument_id LIKE $1
            ORDER BY event_time DESC
            LIMIT 1
        """
        prefix = f"DERIBIT:OPTIONS:{self.cfg.underlying.upper()}%"
        rows = await db.fetch(sql, prefix, bucket_ts, int(self.cfg.lookback_s))
        if not rows:
            return None
        try:
            return float(rows[0]["underlying_price"])
        except Exception:
            return None

    async def _select_expiries(self, db: Any, bucket_ts: dt.datetime) -> Dict[str, List[dt.date]]:
        sql = """
            SELECT DISTINCT expiry
            FROM deribit.options_instruments
            WHERE underlying = $1
            ORDER BY expiry ASC
        """
        rows = await db.fetch(sql, self.cfg.underlying)
        out: Dict[str, List[dt.date]] = {}
        for r in rows:
            exp = r.get("expiry") or r.get("expiration") or r.get("expiry_date")
            if exp is None:
                continue
            try:
                delta_days = (exp - bucket_ts.date()).days
            except Exception:
                continue
            tb = self._bucket_tenor(delta_days)
            if tb is None:
                continue
            out.setdefault(tb, [])
            if len(out[tb]) < int(self.cfg.max_expiries_per_bucket):
                out[tb].append(exp)
        return out

    async def _fetch_snapshot_for_expiry(
        self, db: Any, expiry: dt.date, bucket_ts: dt.datetime
    ) -> Sequence[Dict[str, Any]]:
        sql = """
            SELECT t.instrument_id,
                   t.event_time,
                   t.mark_iv,
                   t.delta,
                   t.open_interest,
                   t.bid,
                   t.ask,
                   t.underlying_price,
                   i.strike,
                   i.option_type,
                   i.expiry
            FROM deribit.options_ticker t
            JOIN deribit.options_instruments i ON i.instrument_id = t.instrument_id
            WHERE i.underlying = $1
              AND i.expiry = $2
              AND t.event_time >= $3 - interval '1 seconds' * $4
              AND t.event_time <= $3 + interval '5 seconds'
            ORDER BY t.event_time DESC
        """
        rows = await db.fetch(sql, self.cfg.underlying, expiry, bucket_ts, int(self.cfg.lookback_s))
        return [dict(r) for r in rows]

    def _select_by_delta(
        self, rows: Iterable[Dict[str, Any]], target: float, tol: float, side: str
    ) -> Optional[Dict[str, Any]]:
        best: Tuple[float, Dict[str, Any]] | None = None
        for r in rows:
            delta = r.get("delta")
            if delta is None:
                continue
            try:
                d = float(delta)
            except Exception:
                continue
            if side == "C" and d < 0:
                continue
            if side == "P" and d > 0:
                continue
            if abs(abs(d) - abs(target)) > tol:
                continue
            score = abs(abs(d) - abs(target))
            if best is None or score < best[0]:
                best = (score, r)
        return best[1] if best else None

    def _select_atm(self, rows: Iterable[Dict[str, Any]], spot: float) -> Optional[Dict[str, Any]]:
        best: Tuple[float, Dict[str, Any]] | None = None
        for r in rows:
            strike = r.get("strike")
            delta = r.get("delta")
            try:
                m = abs(float(strike) / float(spot) - 1.0) if strike is not None else None
            except Exception:
                m = None
            try:
                dscore = abs(abs(float(delta)) - 0.5) if delta is not None else None
            except Exception:
                dscore = None
            score = dscore if dscore is not None else m
            if score is None:
                continue
            if best is None or score < best[0]:
                best = (score, r)
        return best[1] if best else None

    def _compute_rr_bf_atm(
        self, rows: Sequence[Dict[str, Any]], spot: float
    ) -> Tuple[Optional[float], Optional[float], Optional[float], Dict[str, Any]]:
        audit: Dict[str, Any] = {"components": {}}
        call = self._select_by_delta(rows, self.cfg.delta_target, self.cfg.delta_tolerance, "C")
        put = self._select_by_delta(rows, -self.cfg.delta_target, self.cfg.delta_tolerance, "P")
        atm = self._select_atm(rows, spot)

        def _iv(row: Optional[Dict[str, Any]], key: str) -> Optional[float]:
            if row is None:
                return None
            audit["components"][key] = row.get("instrument_id") or row.get("strike")
            return _clamp(row.get("mark_iv"), self.cfg.clamp_iv)

        iv_call = _iv(call, "call")
        iv_put = _iv(put, "put")
        iv_atm = _iv(atm, "atm")

        rr = None
        bf = None
        if iv_call is not None and iv_put is not None:
            rr = _clamp(iv_call - iv_put, self.cfg.clamp_rr)
            if iv_atm is not None:
                bf = _clamp(0.5 * (iv_call + iv_put) - iv_atm, self.cfg.clamp_bf)
        audit["n_rows"] = len(rows)
        audit["n_used"] = len(rows)
        audit["delta_target"] = self.cfg.delta_target
        audit["delta_tolerance"] = self.cfg.delta_tolerance
        return iv_atm, rr, bf, audit

    def _compute_moneyness_iv(
        self, rows: Sequence[Dict[str, Any]], spot: float
    ) -> Dict[str, Tuple[Optional[float], Dict[str, Any], int]]:
        buckets: Dict[str, List[Tuple[float, float]]] = {}
        for r in rows:
            iv_raw = r.get("mark_iv")
            if iv_raw is None:
                continue
            try:
                iv = _clamp(iv_raw, self.cfg.clamp_iv)
                strike = float(r.get("strike"))
                oi = float(r.get("open_interest") or 0.0)
            except Exception:
                continue
            if iv is None:
                continue
            if oi < float(self.cfg.min_oi or 0.0):
                continue
            m = strike / max(spot, 1e-9)
            mb = self._bucket_moneyness(m)
            if mb is None:
                continue
            buckets.setdefault(mb, []).append((iv, oi))

        out: Dict[str, Tuple[Optional[float], Dict[str, Any], int]] = {}
        for mb, items in buckets.items():
            n = len(items)
            if n == 0:
                continue
            if self.cfg.use_oi_weight:
                num = sum(iv * max(oi, 0.0) for iv, oi in items)
                den = sum(max(oi, 0.0) for _, oi in items) or n
                iv_avg = num / den
            else:
                iv_avg = sum(iv for iv, _ in items) / n
            out[mb] = (_clamp(iv_avg, self.cfg.clamp_iv), {"n": n}, n)
        return out

    async def _upsert_surface_rows(self, db: Any, rows: Iterable[Tuple]) -> None:
        sql = """
            INSERT INTO deribit.options_iv_surface(
                underlying, event_time, tenor_bucket, moneyness_bucket,
                iv, rr_25d, bf_25d, meta
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (underlying, event_time, tenor_bucket, moneyness_bucket)
            DO UPDATE SET
                iv = EXCLUDED.iv,
                rr_25d = EXCLUDED.rr_25d,
                bf_25d = EXCLUDED.bf_25d,
                meta = EXCLUDED.meta
        """
        await db.execute_many(sql, rows)

    async def run_once(self, db: Any, ts_now: Optional[float] = None) -> int:
        if not self.cfg.enabled:
            return 0

        now_dt = dt.datetime.fromtimestamp(ts_now or dt.datetime.now(tz=dt.timezone.utc).timestamp(), tz=dt.timezone.utc)
        bucket_ts = self._floor_minute(now_dt - dt.timedelta(seconds=self.cfg.lag_s))
        if self._last_bucket_ts is not None and bucket_ts <= self._last_bucket_ts:
            return 0

        spot = await self._fetch_spot(db, bucket_ts)
        if spot is None or spot <= 0:
            logger.debug("[surface_builder] spot unavailable for bucket %s", bucket_ts)
            return 0

        expiries_by_bucket = await self._select_expiries(db, bucket_ts)
        if not expiries_by_bucket:
            logger.debug("[surface_builder] no expiries for bucket %s", bucket_ts)
            return 0

        rows_to_insert: List[Tuple] = []
        for tenor_bucket, expiries in expiries_by_bucket.items():
            for expiry in expiries:
                snap_rows = await self._fetch_snapshot_for_expiry(db, expiry, bucket_ts)
                if not snap_rows:
                    continue
                filtered_rows: List[Dict[str, Any]] = []
                for r in snap_rows:
                    if self.cfg.min_quotes and (
                        r.get("bid") in (None, 0) or r.get("ask") in (None, 0)
                    ):
                        continue
                    filtered_rows.append(r)
                if not filtered_rows:
                    continue
                iv_atm, rr, bf, audit = self._compute_rr_bf_atm(filtered_rows, spot)
                meta = {
                    "expiry": expiry.isoformat(),
                    "bucket_ts": bucket_ts.isoformat(),
                    "tenor_bucket": tenor_bucket,
                    "expiry_days": (expiry - bucket_ts.date()).days,
                    "audit": audit,
                }
                rows_to_insert.append(
                    (self.cfg.underlying, bucket_ts, tenor_bucket, "NA", iv_atm, rr, bf, meta)
                )

                m_iv = self._compute_moneyness_iv(filtered_rows, spot)
                for m_bucket, (iv_avg, m_audit, n_used) in m_iv.items():
                    meta_m = {
                        "expiry": expiry.isoformat(),
                        "bucket_ts": bucket_ts.isoformat(),
                        "tenor_bucket": tenor_bucket,
                        "moneyness_bucket": m_bucket,
                        "expiry_days": (expiry - bucket_ts.date()).days,
                        "audit": m_audit,
                        "n_used": n_used,
                    }
                    rows_to_insert.append(
                        (self.cfg.underlying, bucket_ts, tenor_bucket, m_bucket, iv_avg, None, None, meta_m)
                    )

        if not rows_to_insert:
            return 0

        await self._upsert_surface_rows(db, rows_to_insert)
        self._last_bucket_ts = bucket_ts
        return len(rows_to_insert)
