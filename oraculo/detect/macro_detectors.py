from __future__ import annotations

import asyncio
import datetime as dt
import inspect
import math
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from oraculo.detect.detectors import Event
from oraculo.detect.window_aggregates import RollingTimeWindow


@dataclass
class OISpikeCfg:
    enabled: bool = False
    poll_s: float = 5.0
    retrigger_s: float = 60.0
    oi_window_s: float = 120.0
    momentum_window_s: float = 60.0
    oi_warn_pct: float = 0.80
    oi_strong_pct: float = 1.50
    mom_warn_usd: float = 8.0
    mom_strong_usd: float = 20.0
    require_same_dir: bool = True


@dataclass
class LiqClusterCfg:
    enabled: bool = False
    poll_s: float = 5.0
    window_s: float = 60.0
    retrigger_s: float = 90.0
    warn_usd: float = 1_000_000.0
    strong_usd: float = 3_000_000.0
    confirm_s: float = 10.0
    momentum_window_s: float = 30.0
    min_move_usd: float = 25.0
    max_rebound_usd: float = 10.0
    use_usd: bool = True
    clamp_usd: float = 50_000_000.0


@dataclass
class TopTradersCfg:
    enabled: bool = False
    poll_s: float = 15.0
    retrigger_s: float = 300.0
    acc_warn: float = 0.60
    acc_strong: float = 0.70
    pos_warn: float = 0.60
    pos_strong: float = 0.70
    require_both: bool = False
    choose_by: str = "max_score"


def _clamp01(x: float) -> float:
    try:
        return max(0.0, min(1.0, float(x)))
    except Exception:
        return 0.0


def _ts_to_epoch(ts: Any) -> Optional[float]:
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return float(ts)
    if hasattr(ts, "timestamp"):
        try:
            return float(ts.timestamp())
        except Exception:
            return None
    return None


class OISpikeDetector:
    """
    Detector macro: spike de OI con momentum de precio.
    - OI preferente desde metrics_series (oi_delta_pct_doc)
    - Fallback a open_interest de futuros
    - Momentum preferente desde snapshot.wmid, fallback a metrics_series('wmid')
    """

    def __init__(self, cfg: OISpikeCfg, instrument_id: str):
        self.cfg = cfg
        self.instrument_id = instrument_id
        self.last_fire_ts: Optional[float] = None
        max_age = max(self.cfg.momentum_window_s * 3.0, self.cfg.oi_window_s * 1.5)
        self.wmid_window = RollingTimeWindow(max_age_s=max_age)
        self.last_oi_sample: Optional[Tuple[float, float, str]] = None  # (ts, val, source)

    def update_wmid(self, ts: float, wmid: Optional[float]) -> None:
        if wmid is None:
            return
        self.wmid_window.add(ts, wmid)

    def _await_if_needed(self, maybe_coro: Any) -> Any:
        if inspect.isawaitable(maybe_coro):
            loop = asyncio.new_event_loop()
            try:
                return loop.run_until_complete(maybe_coro)
            finally:
                loop.close()
        return maybe_coro

    def _fetch_one(self, db: Any, sql: str, *args: Any) -> Optional[Dict[str, Any]]:
        if db is None:
            return None
        row = None
        if hasattr(db, "fetchrow"):
            try:
                row = self._await_if_needed(db.fetchrow(sql, *args))
            except Exception:
                row = None
        if row is None and hasattr(db, "fetch"):
            try:
                rows = self._await_if_needed(db.fetch(sql, *args))
                row = rows[0] if rows else None
            except Exception:
                row = None
        if row is None:
            return None
        try:
            return dict(row)
        except Exception:
            return row  # type: ignore[return-value]

    def _fetch_oi_doc(self, db: Any) -> Optional[Tuple[float, float]]:
        sql = """
            SELECT event_time, value
            FROM oraculo.metrics_series
            WHERE metric='oi_delta_pct_doc'
              AND window_s=$1
              AND instrument_id=$2
            ORDER BY event_time DESC
            LIMIT 1
        """
        row = self._fetch_one(db, sql, int(self.cfg.oi_window_s), self.instrument_id)
        if not row:
            return None
        ts = _ts_to_epoch(row.get("event_time"))
        val = row.get("value")
        if ts is None or val is None:
            return None
        return ts, float(val)

    def _fetch_oi_fallback(self, db: Any, ts_now: float) -> Optional[Tuple[float, float]]:
        sql_now = """
            SELECT event_time, open_interest
            FROM binance_futures.open_interest
            WHERE instrument_id=$1
              AND event_time <= $2
            ORDER BY event_time DESC
            LIMIT 1
        """
        sql_prev = """
            SELECT event_time, open_interest
            FROM binance_futures.open_interest
            WHERE instrument_id=$1
              AND event_time <= $2
            ORDER BY event_time DESC
            LIMIT 1
        """
        row_now = self._fetch_one(db, sql_now, self.instrument_id, dt.datetime.fromtimestamp(ts_now, tz=dt.timezone.utc))
        row_prev = self._fetch_one(
            db,
            sql_prev,
            self.instrument_id,
            dt.datetime.fromtimestamp(ts_now - float(self.cfg.oi_window_s), tz=dt.timezone.utc),
        )
        if not row_now or not row_prev:
            return None
        ts_now_db = _ts_to_epoch(row_now.get("event_time"))
        ts_prev_db = _ts_to_epoch(row_prev.get("event_time"))
        oi_now = row_now.get("open_interest")
        oi_prev = row_prev.get("open_interest")
        if ts_now_db is None or ts_prev_db is None or oi_now is None or oi_prev in (None, 0):
            return None
        try:
            delta_pct = (float(oi_now) - float(oi_prev)) / max(float(oi_prev), 1e-9) * 100.0
        except Exception:
            return None
        return ts_now_db, float(delta_pct)

    def _fetch_wmid_series(self, db: Any, ts_now: float) -> Optional[Tuple[float, float]]:
        sql_last = """
            SELECT event_time, value
            FROM oraculo.metrics_series
            WHERE metric='wmid'
              AND window_s=1
              AND instrument_id=$1
            ORDER BY event_time DESC
            LIMIT 1
        """
        sql_ref = """
            SELECT event_time, value
            FROM oraculo.metrics_series
            WHERE metric='wmid'
              AND window_s=1
              AND instrument_id=$1
              AND event_time <= $2
            ORDER BY event_time DESC
            LIMIT 1
        """
        row_last = self._fetch_one(db, sql_last, self.instrument_id)
        if not row_last:
            return None
        ts_last = _ts_to_epoch(row_last.get("event_time"))
        wmid_last = row_last.get("value")
        if ts_last is None or wmid_last is None:
            return None
        row_ref = self._fetch_one(
            db,
            sql_ref,
            self.instrument_id,
            dt.datetime.fromtimestamp(ts_now - float(self.cfg.momentum_window_s), tz=dt.timezone.utc),
        )
        if not row_ref:
            return None
        ts_ref = _ts_to_epoch(row_ref.get("event_time"))
        wmid_ref = row_ref.get("value")
        if ts_ref is None or wmid_ref is None:
            return None
        try:
            wmid_last_f = float(wmid_last)
            wmid_ref_f = float(wmid_ref)
        except Exception:
            return None
        self.update_wmid(ts_ref, wmid_ref_f)
        self.update_wmid(ts_last, wmid_last_f)
        return ts_last, wmid_last_f

    def _compute_momentum(self, ts_now: float, snapshot: Any, db: Any) -> Tuple[Optional[float], str]:
        metric_used_price = "wmid_snapshot"
        wmid_snapshot = None
        if snapshot is not None:
            wmid_snapshot = getattr(snapshot, "wmid", None)
            if wmid_snapshot is None:
                bb = getattr(snapshot, "best_bid", None)
                ba = getattr(snapshot, "best_ask", None)
                if bb is not None and ba is not None:
                    wmid_snapshot = (float(bb) + float(ba)) / 2.0
        if wmid_snapshot is not None:
            self.update_wmid(ts_now, float(wmid_snapshot))
        momentum = self.wmid_window.delta_over(self.cfg.momentum_window_s, ts_now)
        if momentum is None and db is not None:
            metric_used_price = "wmid_series"
            row = self._fetch_wmid_series(db, ts_now)
            if row is not None:
                momentum = self.wmid_window.delta_over(self.cfg.momentum_window_s, ts_now)
        if momentum is None:
            metric_used_price = "unknown"
        return momentum, metric_used_price

    def _severity_intensity(self, oi_pct: float, momentum_usd: float) -> float:
        oi_intensity = _clamp01(
            (oi_pct - float(self.cfg.oi_warn_pct))
            / max(float(self.cfg.oi_strong_pct - self.cfg.oi_warn_pct), 1e-9)
        )
        mom_intensity = _clamp01(
            (abs(momentum_usd) - float(self.cfg.mom_warn_usd))
            / max(float(self.cfg.mom_strong_usd - self.cfg.mom_warn_usd), 1e-9)
        )
        return _clamp01(0.5 * oi_intensity + 0.5 * mom_intensity)

    def poll(self, ts_now: float, db: Any, snapshot: Any = None) -> Optional[Event]:
        if not self.cfg.enabled:
            return None

        # Retrigger guard
        if self.last_fire_ts is not None and (ts_now - self.last_fire_ts) < float(self.cfg.retrigger_s):
            return None

        oi_sample = self._fetch_oi_doc(db)
        metric_used_oi = "oi_delta_pct_doc" if oi_sample else "open_interest_fallback"
        if oi_sample is None:
            oi_sample = self._fetch_oi_fallback(db, ts_now)
        if oi_sample is None:
            return None

        oi_ts, oi_val = oi_sample
        if self.last_oi_sample is not None and oi_ts <= self.last_oi_sample[0]:
            oi_val = self.last_oi_sample[1]
            metric_used_oi = self.last_oi_sample[2]
        else:
            self.last_oi_sample = (oi_ts, oi_val, metric_used_oi)

        if oi_val < float(self.cfg.oi_warn_pct):
            return None

        momentum, metric_used_price = self._compute_momentum(ts_now, snapshot, db)
        if momentum is None:
            return None

        side: Optional[str] = None
        if momentum >= float(self.cfg.mom_warn_usd):
            side = "buy"
        elif momentum <= -float(self.cfg.mom_warn_usd):
            side = "sell"

        if self.cfg.require_same_dir and side is None:
            return None
        if side is None:
            return None

        intensity = self._severity_intensity(oi_val, momentum)
        self.last_fire_ts = ts_now

        return Event(
            kind="oi_spike",
            side=side,
            ts=ts_now,
            price=float(getattr(snapshot, "wmid", None) or getattr(snapshot, "mid", 0.0) or 0.0),
            intensity=intensity,
            fields={
                "oi_delta_pct": oi_val,
                "oi_window_s": self.cfg.oi_window_s,
                "momentum_usd": momentum,
                "momentum_window_s": self.cfg.momentum_window_s,
                "metric_used_oi": metric_used_oi,
                "metric_used_price": metric_used_price,
                "oi_warn_pct": self.cfg.oi_warn_pct,
                "oi_strong_pct": self.cfg.oi_strong_pct,
                "mom_warn_usd": self.cfg.mom_warn_usd,
                "mom_strong_usd": self.cfg.mom_strong_usd,
            },
        )


class LiqClusterDetector:
    """
    Detector macro: clusters de liquidaciones con confirmación de precio.
    Sigue patrón macro (polling) similar a OI spike.
    """

    def __init__(self, cfg: LiqClusterCfg, instrument_id: str):
        self.cfg = cfg
        self.instrument_id = instrument_id
        self.last_fire_ts: Optional[float] = None
        self.armed_side: Optional[str] = None
        self.armed_ts: Optional[float] = None
        self.armed_anchor_wmid: Optional[float] = None
        self.wmid_window = RollingTimeWindow(max(self.cfg.momentum_window_s * 3.0, self.cfg.window_s * 1.5))

    def update_wmid(self, ts: float, wmid: Optional[float]) -> None:
        if wmid is None:
            return
        self.wmid_window.add(ts, float(wmid))

    def _await_if_needed(self, maybe_coro: Any) -> Any:
        if inspect.isawaitable(maybe_coro):
            loop = asyncio.new_event_loop()
            try:
                return loop.run_until_complete(maybe_coro)
            finally:
                loop.close()
        return maybe_coro

    def _fetch(self, db: Any, sql: str, *args: Any) -> Optional[Dict[str, Any]]:
        if db is None:
            return None
        try:
            rows = None
            if hasattr(db, "fetch"):
                rows = self._await_if_needed(db.fetch(sql, *args))
            elif hasattr(db, "fetchrow"):
                row = self._await_if_needed(db.fetchrow(sql, *args))
                rows = [row] if row else []
            if not rows:
                return None
            res: Dict[str, Any] = {}
            for r in rows:
                try:
                    row_dict = dict(r)
                except Exception:
                    row_dict = r  # type: ignore[assignment]
                side = str(row_dict.get("side") or "").lower()
                res[side] = float(row_dict.get("v") or 0.0)
            return res
        except Exception:
            return None

    def _query_liqs(self, db: Any, ts_now: float) -> Optional[Tuple[float, float, str]]:
        window_s = float(self.cfg.window_s)
        sql_usd = """
            SELECT side, COALESCE(SUM(quote_qty_usd), 0) AS v
            FROM binance_futures.liquidations
            WHERE instrument_id = $1
              AND event_time >= now() - ($2::text || ' seconds')::interval
            GROUP BY side;
        """
        sql_qty = """
            SELECT side, COALESCE(SUM(qty), 0) AS v
            FROM binance_futures.liquidations
            WHERE instrument_id = $1
              AND event_time >= now() - ($2::text || ' seconds')::interval
            GROUP BY side;
        """
        data: Optional[Dict[str, Any]]
        metric_used = "qty"
        if self.cfg.use_usd:
            data = self._fetch(db, sql_usd, self.instrument_id, str(int(window_s)))
            metric_used = "quote_qty_usd"
            if data is None or not data:
                data = self._fetch(db, sql_qty, self.instrument_id, str(int(window_s)))
                metric_used = "qty"
        else:
            data = self._fetch(db, sql_qty, self.instrument_id, str(int(window_s)))
        if data is None:
            return None
        sell_v = float(data.get("sell", 0.0) or 0.0)
        buy_v = float(data.get("buy", 0.0) or 0.0)
        if metric_used == "quote_qty_usd":
            clamp = float(self.cfg.clamp_usd)
            sell_v = min(max(sell_v, 0.0), clamp)
            buy_v = min(max(buy_v, 0.0), clamp)
        return sell_v, buy_v, metric_used

    def _compute_wmid(self, ts_now: float, snapshot: Any) -> Optional[float]:
        wmid = None
        if snapshot is not None:
            wmid = getattr(snapshot, "wmid", None)
            if wmid is None:
                bb = getattr(snapshot, "best_bid", None)
                ba = getattr(snapshot, "best_ask", None)
                if bb is not None and ba is not None:
                    wmid = (float(bb) + float(ba)) / 2.0
        if wmid is not None:
            self.update_wmid(ts_now, float(wmid))
        return wmid

    def _compute_momentum(self, ts_now: float) -> Optional[float]:
        return self.wmid_window.delta_over(float(self.cfg.momentum_window_s), ts_now)

    def poll(self, ts_now: float, db: Any, instrument_id: str, snapshot: Any = None) -> Optional[Event]:
        if not self.cfg.enabled:
            return None
        _ = instrument_id  # compatibility / future use

        if self.last_fire_ts is not None and (ts_now - self.last_fire_ts) < float(self.cfg.retrigger_s):
            return None

        wmid_now = self._compute_wmid(ts_now, snapshot)
        liq_row = self._query_liqs(db, ts_now)
        if liq_row is None:
            return None
        sell_v, buy_v, metric_used = liq_row

        cluster_side: Optional[str] = None
        warn = float(self.cfg.warn_usd)
        if sell_v >= warn and sell_v >= buy_v:
            cluster_side = "sell"
        elif buy_v >= warn and buy_v > sell_v:
            cluster_side = "buy"
        else:
            self.armed_side = None
            self.armed_ts = None
            self.armed_anchor_wmid = None
            return None

        if self.armed_side != cluster_side:
            self.armed_side = cluster_side
            self.armed_ts = ts_now
            self.armed_anchor_wmid = wmid_now
            return None

        if self.armed_anchor_wmid is None:
            self.armed_anchor_wmid = wmid_now

        rebound = None
        if wmid_now is not None and self.armed_anchor_wmid is not None:
            if cluster_side == "sell":
                rebound = float(wmid_now - self.armed_anchor_wmid)
            else:
                rebound = float(self.armed_anchor_wmid - wmid_now)
        if rebound is not None and rebound > float(self.cfg.max_rebound_usd):
            self.armed_side = None
            self.armed_ts = None
            self.armed_anchor_wmid = None
            return None

        momentum = self._compute_momentum(ts_now)
        if momentum is None:
            return None
        min_move = float(self.cfg.min_move_usd)
        if cluster_side == "sell" and momentum > -min_move:
            return None
        if cluster_side == "buy" and momentum < min_move:
            return None

        if self.armed_ts is None or (ts_now - self.armed_ts) < float(self.cfg.confirm_s):
            return None

        cluster_v = sell_v if cluster_side == "sell" else buy_v
        intensity = _clamp01((cluster_v - warn) / max(float(self.cfg.strong_usd - self.cfg.warn_usd), 1e-9))

        anchor = self.armed_anchor_wmid
        armed_ts = self.armed_ts
        self.last_fire_ts = ts_now
        self.armed_side = None
        self.armed_ts = None
        self.armed_anchor_wmid = None

        momentum_usd = float(momentum)
        return Event(
            kind="liq_cluster",
            side=cluster_side,
            ts=ts_now,
            price=float(wmid_now or 0.0),
            intensity=intensity,
            fields={
                "window_s": self.cfg.window_s,
                "poll_s": self.cfg.poll_s,
                "confirm_s": self.cfg.confirm_s,
                "momentum_window_s": self.cfg.momentum_window_s,
                "min_move_usd": self.cfg.min_move_usd,
                "max_rebound_usd": self.cfg.max_rebound_usd,
                "metric_used": metric_used,
                "sell_v": sell_v,
                "buy_v": buy_v,
                "cluster_v": cluster_v,
                "warn_usd": self.cfg.warn_usd,
                "strong_usd": self.cfg.strong_usd,
                "wmid": wmid_now,
                "momentum_usd": momentum_usd,
            "armed_anchor_wmid": anchor,
            "armed_ts": armed_ts,
        },
    )


class TopTradersDetector:
    """
    Detector macro: bias de Top Traders (account vs position ratios).
    - Usa tablas binance_futures.top_trader_account_ratio y top_trader_position_ratio.
    - Puede combinar o forzar métrica (account_only/position_only) vía choose_by.
    """

    def __init__(self, cfg: TopTradersCfg, instrument_id: str):
        self.cfg = cfg
        self.instrument_id = instrument_id
        self.last_fire_ts: Optional[float] = None

    def _await_if_needed(self, maybe_coro: Any) -> Any:
        if inspect.isawaitable(maybe_coro):
            loop = asyncio.new_event_loop()
            try:
                return loop.run_until_complete(maybe_coro)
            finally:
                loop.close()
        return maybe_coro

    def _fetch_one(self, db: Any, sql: str, *args: Any) -> Optional[Dict[str, Any]]:
        if db is None:
            return None
        row = None
        if hasattr(db, "fetchrow"):
            try:
                row = self._await_if_needed(db.fetchrow(sql, *args))
            except Exception:
                row = None
        if row is None and hasattr(db, "fetch"):
            try:
                rows = self._await_if_needed(db.fetch(sql, *args))
                row = rows[0] if rows else None
            except Exception:
                row = None
        if row is None:
            return None
        try:
            return dict(row)
        except Exception:
            return row  # type: ignore[return-value]

    @staticmethod
    def _norm(x: Optional[float], warn: float, strong: float) -> float:
        if x is None:
            return 0.0
        return _clamp01((float(x) - float(warn)) / max(float(strong - warn), 1e-9))

    @staticmethod
    def _to_float(x: Any) -> Optional[float]:
        if x is None:
            return None
        try:
            return float(x)
        except Exception:
            return None

    def _ratios_from_row(self, row: Optional[Dict[str, Any]]) -> Tuple[Optional[float], Optional[float], Optional[float], Any]:
        if row is None:
            return None, None, None, None
        lr = self._to_float(row.get("long_ratio"))
        sr = self._to_float(row.get("short_ratio"))
        ts = _ts_to_epoch(row.get("event_time"))
        meta = row.get("meta")
        return lr, sr, ts, meta

    def poll(self, ts_now: float, db: Any, instrument_id: str) -> Optional[Event]:
        if not self.cfg.enabled:
            return None
        _ = instrument_id

        if self.last_fire_ts is not None and (ts_now - self.last_fire_ts) < float(self.cfg.retrigger_s):
            return None

        sql_acc = """
            SELECT event_time, long_ratio, short_ratio, meta
            FROM binance_futures.top_trader_account_ratio
            WHERE instrument_id=$1
            ORDER BY event_time DESC
            LIMIT 1;
        """
        sql_pos = """
            SELECT event_time, long_ratio, short_ratio, meta
            FROM binance_futures.top_trader_position_ratio
            WHERE instrument_id=$1
            ORDER BY event_time DESC
            LIMIT 1;
        """
        row_acc = self._fetch_one(db, sql_acc, self.instrument_id)
        row_pos = self._fetch_one(db, sql_pos, self.instrument_id)

        choose_mode = (self.cfg.choose_by or "max_score").lower()
        if choose_mode == "max_score" and (row_acc is None or row_pos is None):
            return None
        if choose_mode == "account_only" and row_acc is None:
            return None
        if choose_mode == "position_only" and row_pos is None:
            return None

        acc_long_ratio, acc_short_ratio, acc_ts, acc_meta = self._ratios_from_row(row_acc)
        pos_long_ratio, pos_short_ratio, pos_ts, pos_meta = self._ratios_from_row(row_pos)

        score_acc_long = self._norm(acc_long_ratio, self.cfg.acc_warn, self.cfg.acc_strong)
        score_acc_short = self._norm(acc_short_ratio, self.cfg.acc_warn, self.cfg.acc_strong)
        score_pos_long = self._norm(pos_long_ratio, self.cfg.pos_warn, self.cfg.pos_strong)
        score_pos_short = self._norm(pos_short_ratio, self.cfg.pos_warn, self.cfg.pos_strong)

        if self.cfg.require_both:
            long_ok = (acc_long_ratio is not None and acc_long_ratio >= self.cfg.acc_warn) and (
                pos_long_ratio is not None and pos_long_ratio >= self.cfg.pos_warn
            )
            short_ok = (acc_short_ratio is not None and acc_short_ratio >= self.cfg.acc_warn) and (
                pos_short_ratio is not None and pos_short_ratio >= self.cfg.pos_warn
            )
        else:
            long_ok = (acc_long_ratio is not None and acc_long_ratio >= self.cfg.acc_warn) or (
                pos_long_ratio is not None and pos_long_ratio >= self.cfg.pos_warn
            )
            short_ok = (acc_short_ratio is not None and acc_short_ratio >= self.cfg.acc_warn) or (
                pos_short_ratio is not None and pos_short_ratio >= self.cfg.pos_warn
            )

        if not long_ok and not short_ok:
            return None

        side: Optional[str] = None
        metric_used = "account+position"
        side_scores: Dict[str, float] = {}
        if choose_mode == "account_only":
            metric_used = "account_only"
            if long_ok:
                side_scores["long"] = score_acc_long
            if short_ok:
                side_scores["short"] = score_acc_short
        elif choose_mode == "position_only":
            metric_used = "position_only"
            if long_ok:
                side_scores["long"] = score_pos_long
            if short_ok:
                side_scores["short"] = score_pos_short
        else:
            # max_score
            if long_ok:
                side_scores["long_acc"] = score_acc_long
                side_scores["long_pos"] = score_pos_long
            if short_ok:
                side_scores["short_acc"] = score_acc_short
                side_scores["short_pos"] = score_pos_short
            if side_scores:
                best_key = max(side_scores.items(), key=lambda kv: kv[1])[0]
                side = "long" if best_key.startswith("long") else "short"
            if side is None:
                # fallback to max per-side if keys stripped
                long_max = max(score_acc_long, score_pos_long) if long_ok else -math.inf
                short_max = max(score_acc_short, score_pos_short) if short_ok else -math.inf
                if long_max > short_max and long_ok:
                    side = "long"
                elif short_ok:
                    side = "short"
            if side is not None:
                if side == "long":
                    intensity = max(score_acc_long, score_pos_long)
                else:
                    intensity = max(score_acc_short, score_pos_short)
            else:
                intensity = 0.0
        if side is None:
            if side_scores:
                best_side, intensity = max(side_scores.items(), key=lambda kv: kv[1])
                side = "long" if best_side.startswith("long") else "short"
            else:
                return None
        elif choose_mode in ("account_only", "position_only"):
            if not side_scores:
                return None
            best_side, intensity = max(side_scores.items(), key=lambda kv: kv[1])
            side = "long" if best_side.startswith("long") else "short"

        if self.last_fire_ts is not None and (ts_now - self.last_fire_ts) < float(self.cfg.retrigger_s):
            return None

        self.last_fire_ts = ts_now

        return Event(
            kind="top_traders",
            side=side,
            ts=ts_now,
            price=0.0,
            intensity=_clamp01(float(intensity)),
            fields={
                "acc_event_time": acc_ts,
                "pos_event_time": pos_ts,
                "acc_long_ratio": acc_long_ratio,
                "acc_short_ratio": acc_short_ratio,
                "pos_long_ratio": pos_long_ratio,
                "pos_short_ratio": pos_short_ratio,
                "acc_meta": acc_meta,
                "pos_meta": pos_meta,
                "acc_warn": self.cfg.acc_warn,
                "acc_strong": self.cfg.acc_strong,
                "pos_warn": self.cfg.pos_warn,
                "pos_strong": self.cfg.pos_strong,
                "require_both": self.cfg.require_both,
                "choose_by": self.cfg.choose_by,
                "metric_used": metric_used,
                "score_acc_long": score_acc_long,
                "score_acc_short": score_acc_short,
                "score_pos_long": score_pos_long,
                "score_pos_short": score_pos_short,
            },
        )
