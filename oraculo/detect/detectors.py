# ===============================================
# oraculo/detect/detectors.py
# ===============================================
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, Tuple, List
import math

# --------- DTO de evento ----------
@dataclass
class Event:
    kind: str
    side: str            # 'buy' | 'sell' | 'na'
    ts: float
    price: float
    intensity: float     # semántica depende del detector
    fields: Dict[str, Any] = field(default_factory=dict)

# --------- Slicing agresivo (iceberg/hitting) ---------
@dataclass
class SlicingAggConfig:
    gap_ms: int = 80
    k_min: int = 8
    qty_min: float = 5.0
    require_equal: bool = True          # True => iceberg ; False => hitting
    equal_tol_pct: float = 0.0          # tolerancia % para igualdad
    equal_tol_abs: float | None = 0.0   # tolerancia absoluta (en unidades de qty)

class SlicingAggDetector:
    """
    Detecta secuencias de trades agresivos en el mismo lado y mismo price.
    - require_equal=True  => iceberg  (mismo tamaño de slice, dentro de tolerancias)
    - require_equal=False => hitting  (sólo requiere mismo lado y mismo precio)
    Emite Event(kind='slicing_aggr', fields={'mode': 'iceberg'|'hitting', 'k': k}).
    """
    def __init__(self, cfg: SlicingAggConfig):
        self.cfg = cfg
        self._last_ts: Optional[float] = None
        self._last_side: Optional[str] = None
        self._last_price: Optional[float] = None
        self._k: int = 0
        self._ref_qty: Optional[float] = None
        self._acc_qty: float = 0.0

    @staticmethod
    def _equal(a: float, b: float, tol_pct: float, tol_abs: Optional[float]) -> bool:
        if tol_abs is not None and abs(a - b) <= tol_abs:
            return True
        if a == 0 or b == 0:
            return abs(a - b) <= (tol_abs or 0.0)
        return abs(a - b) / max(abs(a), abs(b)) <= max(tol_pct, 0.0)

    def on_trade(self, ts: float, side: str, price: float, qty: float) -> Optional[Event]:
        gap_s = self.cfg.gap_ms / 1000.0
        emit: Optional[Event] = None

        same_bucket = (
            self._last_ts is not None
            and (ts - self._last_ts) <= gap_s
            and self._last_side == side
            and (self._last_price == price)
        )

        if not same_bucket:
            # reset si se corta el bucket
            self._k = 0
            self._ref_qty = None
            self._acc_qty = 0.0

        self._last_ts = ts
        self._last_side = side
        self._last_price = price

        # Actualizar estado
        self._k += 1
        self._acc_qty += qty

        if self.cfg.require_equal:
            # iceberg: exigir igualdad de tamaño vs referencia
            if self._ref_qty is None:
                self._ref_qty = qty
            is_eq = self._equal(qty, self._ref_qty, self.cfg.equal_tol_pct, self.cfg.equal_tol_abs)
            if not is_eq:
                # si pierde igualdad, reinicia pero toma nueva referencia
                self._k = 1
                self._acc_qty = qty
                self._ref_qty = qty
                return None
            mode = "iceberg"
        else:
            # hitting: no chequear igualdad
            mode = "hitting"

        if self._k >= self.cfg.k_min and self._acc_qty >= self.cfg.qty_min:
            emit = Event(
                kind="slicing_aggr",
                side=side,
                ts=ts,
                price=price,
                intensity=self._acc_qty,
                fields={"mode": mode, "k": self._k},
            )
        return emit

# --------- Slicing pasivo (adds en el orderbook) ---------
@dataclass
class SlicingPassConfig:
    gap_ms: int = 120
    k_min: int = 6
    qty_min: float = 5.0

class SlicingPassiveDetector:
    """
    Agrega 'insert' consecutivos en el mismo lado y mismo precio.
    Útil para avisar de colocación de muro en varios lotes.
    Event(kind='slicing_pass').
    """
    def __init__(self, cfg: SlicingPassConfig):
        self.cfg = cfg
        self._last_ts: Optional[float] = None
        self._last_side: Optional[str] = None
        self._last_price: Optional[float] = None
        self._k: int = 0
        self._acc_qty: float = 0.0

    def on_depth(self, ts: float, side: str, price: float, qty: float) -> Optional[Event]:
        gap_s = self.cfg.gap_ms / 1000.0
        same_bucket = (
            self._last_ts is not None
            and (ts - self._last_ts) <= gap_s
            and self._last_side == side
            and (self._last_price == price)
        )
        if not same_bucket:
            self._k = 0
            self._acc_qty = 0.0

        self._last_ts = ts
        self._last_side = side
        self._last_price = price
        self._k += 1
        self._acc_qty += qty

        if self._k >= self.cfg.k_min and self._acc_qty >= self.cfg.qty_min:
            return Event(
                kind="slicing_pass",
                side=side,
                ts=ts,
                price=price,
                intensity=self._acc_qty,
                fields={"k": self._k},
            )
        return None

# --------- Absorción (agresivo absorbido por pasivo) ---------
@dataclass
class AbsorptionCfg:
    dur_s: float = 10.0
    vol_btc: float = 450.0
    max_price_drift_ticks: int = 2
    tick_size: float = 0.1

class AbsorptionDetector:
    """
    Ventana deslizante de 'dur_s' con acumulación de market BUY/SELL.
    Usa ANCLA del best del lado pasivo al primer trade del lado agresor en la ventana:
      - BUY (absorción en ASK)  -> ancla = best_ask inicial
      - SELL (absorción en BID) -> ancla = best_bid inicial
    Emite Event(kind='absorption') si el volumen en 'dur_s' supera vol_btc
    y el drift del best respecto al ancla se mantiene <= max_price_drift_ticks.
    """
    def __init__(self, cfg: AbsorptionCfg):
        self.cfg = cfg
        self._win: List[Tuple[float, str, float, float]] = []   # (ts, side, price, qty)
        # bests actuales (inyectados por on_best)
        self._best_cur: Dict[str, Optional[float]] = {"buy": None, "sell": None}  # buy->best_bid ; sell->best_ask
        # anclas (por lado agresor se fija el best del lado pasivo)
        self._best_anchor: Dict[str, Optional[float]] = {"buy": None, "sell": None}
        self._had_side_in_window: Dict[str, bool] = {"buy": False, "sell": False}

    def on_best(self, best_bid: Optional[float], best_ask: Optional[float]) -> None:
        self._best_cur["buy"] = best_bid
        self._best_cur["sell"] = best_ask

    def _maybe_set_anchor(self, side: str) -> None:
        # Para BUY (absorción en ask), el ANCLA es el ASK actual → clave "sell".
        # Para SELL (absorción en bid), el ANCLA es el BID actual → clave "buy".
        if not self._had_side_in_window[side]:
            if side == "buy":
                self._best_anchor["sell"] = self._best_cur.get("sell")
            else:
                self._best_anchor["buy"] = self._best_cur.get("buy")
            self._had_side_in_window[side] = True

    def _reset_if_expired(self, now_ts: float) -> None:
        cutoff = now_ts - self.cfg.dur_s
        changed = False
        while self._win and self._win[0][0] < cutoff:
            self._win.pop(0)
            changed = True
        if changed:
            has_buy = any(s == "buy" for _, s, _, _ in self._win)
            has_sell = any(s == "sell" for _, s, _, _ in self._win)
            if not has_buy:
                self._had_side_in_window["buy"] = False
                self._best_anchor["sell"] = None
            if not has_sell:
                self._had_side_in_window["sell"] = False
                self._best_anchor["buy"] = None

    def _drift_ok(self, side: str) -> Tuple[bool, Optional[float]]:
        tick = self.cfg.tick_size or 1.0
        if side == "buy":
            cur = self._best_cur.get("sell")     # ASK actual
            anc = self._best_anchor.get("sell")  # ASK anclado
        else:
            cur = self._best_cur.get("buy")      # BID actual
            anc = self._best_anchor.get("buy")   # BID anclado
        if cur is None or anc is None:
            return False, None
        drift_ticks = abs(cur - anc) / tick
        return drift_ticks <= float(self.cfg.max_price_drift_ticks), drift_ticks

    def on_trade(self, ts: float, side: str, price: float, qty: float) -> Optional[Event]:
        # insertar trade y mantener ventana
        self._win.append((ts, side, price, qty))
        self._reset_if_expired(ts)

        # fijar ancla en el primer trade del lado de interés tras reset
        self._maybe_set_anchor(side)

        # acumula volumen por lado en la ventana
        sum_buy = sum(q for t, s, _, q in self._win if s == "buy")
        sum_sell = sum(q for t, s, _, q in self._win if s == "sell")

        # chequeo con drift por ancla
        if sum_buy >= self.cfg.vol_btc:
            ok, drift_ticks = self._drift_ok("buy")
            if ok:
                return Event("absorption", "buy", ts, price, sum_buy,
                             {"dur_s": self.cfg.dur_s, "drift_ticks": drift_ticks})
        if sum_sell >= self.cfg.vol_btc:
            ok, drift_ticks = self._drift_ok("sell")
            if ok:
                return Event("absorption", "sell", ts, price, sum_sell,
                             {"dur_s": self.cfg.dur_s, "drift_ticks": drift_ticks})
        return None

# --------- BreakWall (con depleción/refill/basis_vel) ---------
@dataclass
class BreakWallCfg:
    n_min: int = 3
    dep_pct: float = 0.40
    basis_vel_abs_bps_s: float = 1.5
    require_depletion: bool = True
    forbid_refill_under_pct: float = 0.60
    top_levels_gate: int = 20
    tick_size: float = 0.1
    refill_window_s: float = 3.0
    refill_min_pct: float = 0.60

class BreakWallDetector:
    """
    Reacciona a secuencias de slicing_aggr y usa métricas del motor (depleción/refill/basis_vel).
    Espera snapshot con claves:
      {
        "basis_vel_bps_s": float,
        "dep_bid": float, "dep_ask": float,
        "refill_bid_3s": float, "refill_ask_3s": float,
      }
    """
    def __init__(self, cfg: BreakWallCfg):
        self.cfg = cfg
        self._k_buy = 0
        self._k_sell = 0

    def on_slicing(
        self, ts: float, ev: Event, snapshot: Any
    ) -> Tuple[Optional[Event], Optional[str]]:
        # Permitir tanto diccionarios como dataclasses Snapshot
        snap_get = snapshot.get if hasattr(snapshot, "get") else lambda k, d=None: getattr(snapshot, k, d)
        if ev.kind != "slicing_aggr":
            return None, None
        side = ev.side
        if side == "buy":
            self._k_buy += 1
            self._k_sell = 0
        else:
            self._k_sell += 1
            self._k_buy = 0

        k = self._k_buy if side == "buy" else self._k_sell
        if k < self.cfg.n_min:
            return None, None

        bv_raw = snap_get("basis_vel_bps_s")
        if bv_raw is None:
            return None, None

        bv = float(bv_raw or 0.0)
        if abs(bv) < self.cfg.basis_vel_abs_bps_s:
            return None, "basis_vel_low"

        if self.cfg.top_levels_gate and self.cfg.tick_size > 0:
            ref_px = snap_get("best_ask") if side == "buy" else snap_get("best_bid")
            if ref_px is not None:
                ticks_diff = abs(ev.price - ref_px) / self.cfg.tick_size
                if ticks_diff > float(self.cfg.top_levels_gate):
                    return None, "top_levels_gate"

        dep_ok = True
        refill_ok = True
        if self.cfg.require_depletion:
            if side == "buy":
                dep_ok = float(snap_get("dep_ask", 0.0)) >= self.cfg.dep_pct
                refill_ok = float(snap_get("refill_ask_3s", 0.0)) < self.cfg.forbid_refill_under_pct
            else:
                dep_ok = float(snap_get("dep_bid", 0.0)) >= self.cfg.dep_pct
                refill_ok = float(snap_get("refill_bid_3s", 0.0)) < self.cfg.forbid_refill_under_pct
        if not (dep_ok and refill_ok):
            if not dep_ok:
                return None, "dep_low"
            if not refill_ok:
                return None, "refill_high"
            return None, None

        return Event(
            "break_wall",
            side,
            ts,
            ev.price,
            ev.intensity,
            {"k": k, "basis_vel_bps_s": bv},
        ), None

# --------- Dominance ---------
@dataclass
class DominanceCfg:
    enabled: bool = True
    dom_pct: float = 0.80
    dom_pct_doc: float = 0.60
    metric_source: str = "legacy"
    max_spread_usd: float = 2.0
    levels: int = 1000
    hold_ms: int = 1000
    retrigger_s: int = 30

class DominanceDetector:
    """
    Lectura simplificada de dominancia (porcentaje de niveles con qty>0 en un lado).
    Requiere que el book externo ofrezca método get_head(levels) -> (bids, asks).
    """
    def __init__(self, cfg: DominanceCfg, book=None):
        self.cfg = cfg
        self._book = book  # si no se pasa, el caller debe setearlo luego
        self._last_ts_emit = 0.0
        self._last_side: Optional[str] = None

    def attach_book(self, book) -> None:
        self._book = book

    def _dominance_pct(self, bids: List[Tuple[float, float]], asks: List[Tuple[float, float]]) -> Tuple[str, float, float]:
        nz_bids = sum(1 for _, q in bids[: self.cfg.levels] if q > 0)
        nz_asks = sum(1 for _, q in asks[: self.cfg.levels] if q > 0)
        total = max(nz_bids + nz_asks, 1)
        pct_buy = nz_bids / total
        pct_sell = nz_asks / total
        if pct_buy >= self.cfg.dom_pct:
            return "buy", pct_buy, pct_sell
        if pct_sell >= self.cfg.dom_pct:
            return "sell", pct_sell, pct_buy
        return "none", 0.0, 0.0

    def maybe_emit(self, ts: float, spread_usd: float) -> Optional[Event]:
        if not self.cfg.enabled or self._book is None:
            return None
        bids, asks = self._book.get_head(self.cfg.levels)
        side, dom, _ = self._dominance_pct(bids, asks)
        if side == "none" or spread_usd > self.cfg.max_spread_usd:
            return None
        if (ts - self._last_ts_emit) * 1000.0 < self.cfg.hold_ms and side == self._last_side:
            return None
        self._last_ts_emit = ts
        self._last_side = side
        price = (bids[0][0] if bids else 0.0) if side == "buy" else (asks[0][0] if asks else 0.0)
        return Event("dominance", side, ts, price, dom * 100.0, {"levels": self.cfg.levels})

# --------- Spoofing (aparición/retirada de muro sin ejecución) ---------
@dataclass
class SpoofingCfg:
    wall_size_btc: float = 100.0
    distance_ticks_min: int = 30
    window_s: float = 3.0
    cancel_rate_min: float = 0.7
    exec_tolerance_btc: float = 2.0
    tick_size: float = 0.1

class SpoofingDetector:
    """
    Heurística:
    - Detecta 'insert' grande a >= distance_ticks_min de best.
    - En window_s, si la mayoría se cancela (cancel_rate>=min) y la 'exec' cerca del price es baja,
      emite Event(kind='spoofing').
    """
    def __init__(self, cfg: SpoofingCfg):
        self.cfg = cfg
        # key=(side, price)
        self._walls: Dict[Tuple[str, float], Dict[str, Any]] = {}

    def _ticks_away(self, side: str, price: float, best_bid: Optional[float], best_ask: Optional[float]) -> int:
        t = self.cfg.tick_size or 0.1
        if side == "buy" and best_bid is not None:
            return int(abs(price - best_bid) / t)
        if side == "sell" and best_ask is not None:
            return int(abs(price - best_ask) / t)
        return 0

    def on_depth(self, ts: float, side: str, action: str, price: float, qty: float,
                 best_bid: Optional[float], best_ask: Optional[float]) -> Optional[Event]:
        key = (side, price)
        w = self._walls.get(key)

        if action == "insert":
            dist = self._ticks_away(side, price, best_bid, best_ask)
            if qty >= self.cfg.wall_size_btc and dist >= self.cfg.distance_ticks_min:
                self._walls[key] = {
                    "ts0": ts,
                    "qty_added": qty,
                    "qty_canceled": 0.0,
                    "qty_exec": 0.0,
                    "dist": dist,
                    "active": True,
                }
        elif action == "delete":
            if w and w.get("active"):
                w["qty_canceled"] += qty
        elif action == "update":
            pass  # simplificado

        # evaluar spoof
        w = self._walls.get(key)
        if not w or not w.get("active"):
            return None
        if (ts - w["ts0"]) > self.cfg.window_s:
            total = w["qty_added"] + w["qty_canceled"]
            cancel_rate = (w["qty_canceled"] / max(total, 1e-9)) if total > 0 else 0.0
            if cancel_rate >= self.cfg.cancel_rate_min and w["qty_exec"] <= self.cfg.exec_tolerance_btc:
                w["active"] = False
                return Event(
                    "spoofing",
                    side,
                    ts,
                    price,
                    w["qty_added"],
                    {
                        "dropped": w["qty_canceled"],
                        "cancel_rate": cancel_rate,
                        "distance_ticks": w["dist"],
                    },
                )
            else:
                w["active"] = False
        return None

    def on_trade(self, ts: float, side: str, price: float, qty: float) -> None:
        # si hay pared trackeada muy cerca de este price, sumar exec
        for (s, p), w in list(self._walls.items()):
            if not w.get("active"):
                continue
            near = abs(price - p) <= self.cfg.tick_size * 1.5
            if near:
                w["qty_exec"] += qty

# --------- Depletion masivo de liquidez (timer-based) ---------
@dataclass
class DepletionCfg:
    side: str = "buy"          # 'buy' => BID depletion (R13) ; 'sell' => ASK depletion (R14)
    pct_drop: float = 0.40
    hold_ms: int = 800
    retrigger_s: int = 30
    enabled: bool = True       # permite activar/desactivar por rules.yaml
    metric_source: str = "legacy"  # "legacy" | "doc" | "auto"
    doc_window_s: float = 3.0
    dv_warn: float = 10.0
    dv_strong: float = 30.0
    clamp_abs_dv: float = 200.0
    severity_mode: str = "linear"  # "linear" | "log"
    require_negative: bool = True

class DepletionDetector:
    """
    Usa snapshot del engine (dep_bid/dep_ask/refill_* si disponibles).
    Emite Event(kind='depletion', side=cfg.side, intensity=pct_drop_observado)
    """
    def __init__(self, cfg: DepletionCfg):
        self.cfg = cfg
        self._last_emit_ts: float = 0.0
        self._arm_ts: Optional[float] = None

    def on_snapshot(self, ts: float, snap: Dict[str, Any]) -> Optional[Event]:
        from oraculo.metrics.resolve import resolve
        try:
            from oraculo.obs import metrics as obs_metrics  # type: ignore
        except Exception:  # pragma: no cover - observabilidad opcional
            obs_metrics = None

        if not self.cfg.enabled:
            return None

        side = self.cfg.side
        doc_key = "depletion_bid_doc" if side == "buy" else "depletion_ask_doc"
        legacy_key = "dep_bid" if side == "buy" else "dep_ask"

        res = resolve(snap, doc_key, fallback_key=legacy_key, source=self.cfg.metric_source)
        val = res.value
        if val is None:
            self._arm_ts = None
            return None

        used_source = res.used_source or self.cfg.metric_source
        if used_source == "legacy":
            dep = float(val)
            if dep < self.cfg.pct_drop:
                self._arm_ts = None
                return None

            if (ts - self._last_emit_ts) < self.cfg.retrigger_s:
                return None

            if self._arm_ts is None:
                self._arm_ts = ts
                return None

            if (ts - self._arm_ts) * 1000.0 >= self.cfg.hold_ms:
                self._last_emit_ts = ts
                self._arm_ts = None
                event = Event(
                    "depletion",
                    side,
                    ts,
                    price=0.0,
                    intensity=dep,
                    fields={"metric_used": res.used_key, "metric_source": "legacy"},
                )
                if obs_metrics:
                    try:
                        obs_metrics.depletion_events_total.labels(source="legacy", side=side).inc()
                    except Exception:
                        pass
                return event
            return None

        if used_source != "doc":
            self._arm_ts = None
            return None

        dv = float(val)
        if self.cfg.require_negative:
            if dv >= 0:
                self._arm_ts = None
                return None
            depletion_amount = max(0.0, -dv)
        else:
            depletion_amount = abs(dv)
        depletion_amount = min(depletion_amount, self.cfg.clamp_abs_dv)

        if depletion_amount < self.cfg.dv_warn:
            self._arm_ts = None
            return None

        if (ts - self._last_emit_ts) < self.cfg.retrigger_s:
            return None

        if self._arm_ts is None:
            self._arm_ts = ts
            return None

        if (ts - self._arm_ts) * 1000.0 < self.cfg.hold_ms:
            return None

        warn = self.cfg.dv_warn
        strong = self.cfg.dv_strong
        if self.cfg.severity_mode == "log":
            denom = math.log1p(strong / max(warn, 1e-9))
            if denom <= 0:
                intensity = 1.0
            else:
                intensity = math.log1p(depletion_amount / max(warn, 1e-9)) / denom
        else:
            intensity = (depletion_amount - warn) / max(strong - warn, 1e-9)
        intensity = max(0.0, min(1.0, intensity))

        self._last_emit_ts = ts
        self._arm_ts = None

        fields = {
            "metric_used": res.used_key,
            "metric_source": "doc",
            "dv": dv,
            "depletion_amount": depletion_amount,
            "dv_warn": warn,
            "dv_strong": strong,
            "severity_mode": self.cfg.severity_mode,
            "window_s": self.cfg.doc_window_s,
        }
        if obs_metrics:
            try:
                obs_metrics.depletion_events_total.labels(source="doc", side=side).inc()
                obs_metrics.depletion_doc_amount.observe(depletion_amount)
            except Exception:
                pass
        return Event("depletion", side, ts, price=0.0, intensity=intensity, fields=fields)

# --------- Trigger genérico de métricas de snapshot ---------
@dataclass
class MetricTrigCfg:
    metric: str
    threshold: float
    direction: str = "above"   # "above" | "below"
    hold_ms: int = 500
    retrigger_s: int = 30
    metric_doc: Optional[str] = None
    metric_source: str = "legacy"  # "legacy" | "doc" | "auto"
    doc_sign_mode: str = "legacy"  # "legacy" => invierte signo de DOC

class MetricTriggerDetector:
    """
    Dispara un 'metric_trigger' si snapshot[metric] cruza el threshold en la dirección indicada
    y mantiene 'hold_ms'. 'side' se deja 'na'.
    """
    def __init__(self, cfg: MetricTrigCfg):
        self.cfg = cfg
        self._arm_ts: Optional[float] = None
        self._last_emit: float = 0.0

    def on_snapshot(self, ts: float, snap: Dict[str, Any], book=None) -> Optional[Event]:
        from oraculo.metrics.resolve import resolve

        doc_key = self.cfg.metric_doc or (f"{self.cfg.metric}_doc" if not self.cfg.metric.endswith("_doc") else self.cfg.metric)
        transform = (lambda x: -x) if self.cfg.doc_sign_mode == "legacy" else None
        res = resolve(snap, doc_key, fallback_key=self.cfg.metric, source=self.cfg.metric_source, transform=transform)

        val = res.value
        if val is None:
            self._arm_ts = None
            return None

        cond = (val >= self.cfg.threshold) if self.cfg.direction == "above" else (val <= self.cfg.threshold)
        if not cond:
            self._arm_ts = None
            return None

        if (ts - self._last_emit) < self.cfg.retrigger_s:
            return None

        if self._arm_ts is None:
            self._arm_ts = ts
            return None

        if (ts - self._arm_ts) * 1000.0 >= self.cfg.hold_ms:
            self._last_emit = ts
            self._arm_ts = None
            return Event(
                "metric_trigger",
                side="na",
                ts=ts,
                price=0.0,
                intensity=float(val),
                fields={
                    "metric": self.cfg.metric,
                    "threshold": self.cfg.threshold,
                    "dir": self.cfg.direction,
                    "metric_used": res.used_key or self.cfg.metric,
                    "metric_source": res.used_source or self.cfg.metric_source,
                    "doc_sign_mode": self.cfg.doc_sign_mode,
                },
            )
        return None

# --------- Basis mean-revert detector ---------
@dataclass
class BasisMRcfg:
    gate_abs_bps: float = 25.0
    vel_gate_abs: float = 1.5
    retrigger_s: int = 60
    metric_source: str = "legacy"
    doc_sign_mode: str = "legacy"

class BasisMeanRevertDetector:
    """
    Señal cuando la velocidad del basis cambia de signo y el basis (en bps) se acerca a zona neutra.
    side='sell' para MR down (R17), side='buy' para MR up (R18).
    """
    def __init__(self, cfg: BasisMRcfg):
        self.cfg = cfg
        self._last_sign: Optional[int] = None
        self._last_emit: float = 0.0

    @staticmethod
    def _sign(x: float) -> int:
        return 1 if x > 0 else (-1 if x < 0 else 0)

    def on_snapshot(self, ts: float, snap: Dict[str, Any], book=None) -> Optional[Event]:
        from oraculo.metrics.resolve import resolve

        transform = (lambda x: -x) if self.cfg.doc_sign_mode == "legacy" else None
        bps_res = resolve(snap, "basis_bps_doc", fallback_key="basis_bps", source=self.cfg.metric_source, transform=transform)
        vel_res = resolve(
            snap, "basis_vel_bps_s_doc", fallback_key="basis_vel_bps_s", source=self.cfg.metric_source, transform=transform
        )

        bps_val = bps_res.value
        vel_val = vel_res.value
        if bps_val is None or vel_val is None:
            return None

        bps = float(bps_val)
        vel = float(vel_val)
        s = self._sign(vel)
        if self._last_sign is None:
            self._last_sign = s
            return None
        flipped = (s != 0 and self._last_sign != 0 and s != self._last_sign)
        self._last_sign = s
        if not flipped:
            return None
        if abs(bps) > self.cfg.gate_abs_bps or abs(vel) < self.cfg.vel_gate_abs:
            return None
        if (ts - self._last_emit) < self.cfg.retrigger_s:
            return None

        side = "buy" if s > 0 else "sell"
        self._last_emit = ts
        used_source = vel_res.used_source or bps_res.used_source
        return Event(
            "basis_mean_revert",
            side,
            ts,
            price=0.0,
            intensity=abs(vel),
            fields={
                "bps": bps,
                "metric_used_basis": bps_res.used_key or "basis_bps",
                "metric_used_vel": vel_res.used_key or "basis_vel_bps_s",
                "metric_source": used_source or self.cfg.metric_source,
                "doc_sign_mode": self.cfg.doc_sign_mode,
            },
        )

# --------- Tape pressure (ratio de agresiones) ---------
@dataclass
class TapePressureCfg:
    window_s: float = 3.0
    buy_thr: float = 0.8
    sell_thr: float = 0.2
    retrigger_s: int = 20

class TapePressureDetector:
    """
    Ventana deslizante de trades para calcular presión agresiva BUY/(BUY+SELL).
    Emite 'tape_pressure' BUY si ratio>=buy_thr, SELL si ratio<=sell_thr.
    """
    def __init__(self, cfg: TapePressureCfg):
        self.cfg = cfg
        self._win: List[Tuple[float, str, float, float]] = []
        self._last_emit_ts: float = 0.0

    def on_trade(self, ts: float, side: str, price: float, qty: float) -> Optional[Event]:
        self._win.append((ts, side, price, qty))
        cutoff = ts - self.cfg.window_s
        while self._win and self._win[0][0] < cutoff:
            self._win.pop(0)

        buy = sum(q for _, s, _, q in self._win if s == "buy")
        sell = sum(q for _, s, _, q in self._win if s == "sell")
        tot = buy + sell
        if tot <= 0:
            return None
        ratio = buy / tot

        if (ts - self._last_emit_ts) < self.cfg.retrigger_s:
            return None

        if ratio >= self.cfg.buy_thr:
            self._last_emit_ts = ts
            return Event("tape_pressure", "buy", ts, price, ratio, {"window_s": self.cfg.window_s})
        if ratio <= self.cfg.sell_thr:
            self._last_emit_ts = ts
            return Event("tape_pressure", "sell", ts, price, ratio, {"window_s": self.cfg.window_s})
        return None

# --------- Opciones (Deribit): IV spikes R19/R20 ---------
@dataclass
class IVSpikeCfg:
    """Config para detector de spikes de IV (R19/R20).

    - window_s: ventana en segundos para mantener la base.
    - up_thresh_pct: umbral de spike al alza (R19), en %.
    - down_thresh_pct: umbral de spike a la baja (R20), en % (negativo).
    - retrigger_s: tiempo mínimo entre eventos del mismo tipo.
    """
    window_s: float = 300.0
    up_thresh_pct: float = 10.0
    down_thresh_pct: float = -10.0
    retrigger_s: float = 60.0


class IVSpikeDetector:
    """Detector de R19/R20 a partir de mark_iv del ticker de opciones."""

    def __init__(self, cfg: IVSpikeCfg):
        self.cfg = cfg
        self._base_iv: Optional[float] = None
        self._base_ts: Optional[float] = None
        self._last_up_ts: Optional[float] = None
        self._last_down_ts: Optional[float] = None

    def on_iv(self, ts: float, iv: Optional[float]) -> Optional[Event]:
        if iv is None or iv <= 0:
            return None

        # Inicializa base o reancla si la ventana ha caducado
        if self._base_iv is None or self._base_ts is None or (ts - self._base_ts) > self.cfg.window_s:
            self._base_iv = iv
            self._base_ts = ts
            return None

        base = self._base_iv
        if base is None or base <= 0:
            self._base_iv = iv
            self._base_ts = ts
            return None

        delta_pct = (iv / base - 1.0) * 100.0

        # Spike al alza (R19)
        if delta_pct >= self.cfg.up_thresh_pct:
            if self._last_up_ts is None or (ts - self._last_up_ts) >= self.cfg.retrigger_s:
                self._last_up_ts = ts
                self._base_iv = iv
                self._base_ts = ts
                return Event(
                    kind="iv_spike_up",
                    side="na",
                    ts=ts,
                    price=0.0,
                    intensity=delta_pct,
                    fields={
                        "iv": iv,
                        "iv_base": base,
                        "delta_pct": delta_pct,
                        "window_s": self.cfg.window_s,
                    },
                )
            return None

        # Spike a la baja (R20)
        if delta_pct <= self.cfg.down_thresh_pct:
            if self._last_down_ts is None or (ts - self._last_down_ts) >= self.cfg.retrigger_s:
                self._last_down_ts = ts
                self._base_iv = iv
                self._base_ts = ts
                return Event(
                    kind="iv_spike_down",
                    side="na",
                    ts=ts,
                    price=0.0,
                    intensity=delta_pct,
                    fields={
                        "iv": iv,
                        "iv_base": base,
                        "delta_pct": delta_pct,
                        "window_s": self.cfg.window_s,
                    },
                )
            return None

        # Movimiento pequeño: “adelantamos” la base
        if abs(delta_pct) < abs(self.cfg.up_thresh_pct) * 0.5:
            self._base_iv = iv
            self._base_ts = ts

        return None


# --------- Opciones (Deribit): OI skew R21/R22 ---------
@dataclass
class OISkewCfg:
    """Config para skew de open interest (R21/R22)."""

    bull_ratio_min: float = 1.5   # OI_calls / OI_puts
    bear_ratio_min: float = 1.5   # OI_puts / OI_calls
    min_total_oi: float = 10.0
    retrigger_s: float = 300.0


class OISkewDetector:
    """Detector de R21/R22 a partir de OI en calls/puts agregados."""

    def __init__(self, cfg: OISkewCfg):
        self.cfg = cfg
        self._last_bull_ts: Optional[float] = None
        self._last_bear_ts: Optional[float] = None

    def on_oi(self, ts: float, oi_calls: Optional[float], oi_puts: Optional[float]) -> Optional[Event]:
        c = float(oi_calls or 0.0)
        p = float(oi_puts or 0.0)
        total = c + p
        if total < self.cfg.min_total_oi:
            return None
        if c <= 0 or p <= 0:
            return None  # evitamos divisiones raras

        ratio_cp = c / p
        ratio_pc = p / c
        now = ts

        # Sesgo alcista (calls dominan) -> R21
        if ratio_cp >= self.cfg.bull_ratio_min:
            if self._last_bull_ts is None or (now - self._last_bull_ts) >= self.cfg.retrigger_s:
                self._last_bull_ts = now
                return Event(
                    kind="oi_skew_bull",
                    side="na",
                    ts=now,
                    price=0.0,
                    intensity=ratio_cp,
                    fields={
                        "oi_calls": c,
                        "oi_puts": p,
                        "ratio_cp": ratio_cp,
                        "ratio_pc": ratio_pc,
                    },
                )
            return None

        # Sesgo bajista (puts dominan) -> R22
        if ratio_pc >= self.cfg.bear_ratio_min:
            if self._last_bear_ts is None or (now - self._last_bear_ts) >= self.cfg.retrigger_s:
                self._last_bear_ts = now
                return Event(
                    kind="oi_skew_bear",
                    side="na",
                    ts=now,
                    price=0.0,
                    intensity=ratio_pc,
                    fields={
                        "oi_calls": c,
                        "oi_puts": p,
                        "ratio_cp": ratio_cp,
                        "ratio_pc": ratio_pc,
                    },
                )
            return None

        return None
