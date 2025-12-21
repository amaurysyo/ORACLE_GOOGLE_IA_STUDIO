# ===============================================
# oraculo/detect/metrics_engine.py
# ===============================================
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
import math
import time
import threading

from .window_aggregates import RollingTimeWindow

# ------- OrderBook mínimo (price->qty) -------
class OrderBook:
    def __init__(self):
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self._lock = threading.RLock()

    def apply(self, side: str, action: str, price: float, qty: float, *, qty_is_delta: bool = False) -> None:
        """Aplica cambios al libro manteniendo cantidades absolutas.

        Los feeds de profundidad en Oráculo envían qty como delta (insert/delete) y
        necesitamos reconstruir el tamaño absoluto del nivel antes de almacenarlo.
        """
        book = self.bids if side == "buy" else self.asks
        with self._lock:
            prev = book.get(price, 0.0)

            if qty_is_delta:
                if action == "delete":
                    qty_abs = max(prev - qty, 0.0)
                else:
                    qty_abs = max(prev + qty, 0.0)

                if qty_abs <= 0:
                    action = "delete"
                elif prev == 0:
                    action = "insert"
                else:
                    action = "update"
                qty = qty_abs

            if action == "insert":
                book[price] = qty
                if book[price] <= 0:
                    book.pop(price, None)
            elif action == "update":
                book[price] = qty
                if book[price] <= 0:
                    book.pop(price, None)
            elif action == "delete":
                # qty = cantidad retirada (no la dejamos negativa)
                left = max(prev - qty, 0.0) if not qty_is_delta else 0.0
                if left <= 0:
                    book.pop(price, None)
                else:
                    book[price] = left

    def _sorted_bids(self) -> List[Tuple[float, float]]:
        with self._lock:
            return sorted(self.bids.items(), key=lambda x: x[0], reverse=True)

    def _sorted_asks(self) -> List[Tuple[float, float]]:
        with self._lock:
            return sorted(self.asks.items(), key=lambda x: x[0])

    def best(self) -> Tuple[Optional[float], Optional[float]]:
        with self._lock:
            bids = self._sorted_bids()
            asks = self._sorted_asks()
            return (bids[0][0] if bids else None, asks[0][0] if asks else None)

    def get_head(self, levels: int = 1000):
        with self._lock:
            bids = self._sorted_bids()[:levels]
            asks = self._sorted_asks()[:levels]
            return bids, asks

# ------- Snapshot -------
@dataclass
class Snapshot:
    spread_usd: Optional[float] = None
    basis_bps: Optional[float] = None
    basis_vel_bps_s: Optional[float] = None
    basis_bps_doc: Optional[float] = None
    basis_vel_bps_s_doc: Optional[float] = None
    basis_accel_bps_s2_doc: Optional[float] = None
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    wmid: Optional[float] = None
    dom_bid: Optional[float] = None
    dom_ask: Optional[float] = None
    dominance_bid_doc: Optional[float] = None
    dominance_ask_doc: Optional[float] = None
    imbalance: Optional[float] = None
    imbalance_doc: Optional[float] = None
    dep_bid: Optional[float] = None
    dep_ask: Optional[float] = None
    depletion_bid_doc: Optional[float] = None
    depletion_ask_doc: Optional[float] = None
    refill_bid_3s: Optional[float] = None
    refill_ask_3s: Optional[float] = None

class MetricsEngine:
    """
    Calcula métricas de microestructura necesarias por detectores y reglas:
    - spread, dominance (niveles no nulos), imbalance (qty_bid - qty_ask)/(suma)
    - basis_bps y basis_vel_bps_s (derivada)
    - dep_bid/dep_ask (proxy: retiradas vs actividad en ventana corta)
    - refill_*_3s (proxy: readds/retiradas en 3s)
    """
    def __init__(
        self,
        top_n: int = 1000,
        tick_size: float = 0.1,
        *,
        imbalance_doc_window_s: float = 3.0,
        dominance_doc_window_s: float = 2.0,
        depletion_doc_window_s: float = 3.0,
        basis_doc_window_s: float = 120.0,
    ):
        self.book = OrderBook()
        self.top_n = top_n
        self.tick = tick_size
        self.imbalance_doc_window_s = float(imbalance_doc_window_s)
        self.dominance_doc_window_s = float(dominance_doc_window_s)
        self.depletion_doc_window_s = float(depletion_doc_window_s)
        self.basis_doc_window_s = float(basis_doc_window_s)

        # Mark/Index para basis
        self._last_basis_bps: Optional[float] = None
        self._last_basis_ts: Optional[float] = None
        self._basis_bps: Optional[float] = None
        self._basis_vel: Optional[float] = None
        self._basis_bps_doc: Optional[float] = None
        self._basis_vel_doc: Optional[float] = None
        self._basis_accel_doc: Optional[float] = None

        max_age = max(self.imbalance_doc_window_s, self.dominance_doc_window_s, self.depletion_doc_window_s, self.basis_doc_window_s) * 2.0
        self._imbalance_win = RollingTimeWindow(max_age_s=max_age)
        self._dominance_win = RollingTimeWindow(max_age_s=max_age)
        self._bid_vol_win = RollingTimeWindow(max_age_s=max_age)
        self._ask_vol_win = RollingTimeWindow(max_age_s=max_age)
        self._basis_doc_win = RollingTimeWindow(max_age_s=max_age)

        # Ventanas de actividad por lado
        self._win_s = 3.0
        self._bins: Dict[str, List[Tuple[float, float, float]]] = {"buy": [], "sell": []}  # (ts, ins, dels)

    # ---- entradas ----
    def on_depth(self, ts: float, side: str, action: str, price: float, qty: float) -> None:
        # book (las qty que vienen del feed son deltas; convertir a abs antes de mutar)
        self.book.apply(side, action, price, qty, qty_is_delta=True)

        # activar contadores simples de ins/del
        ins, dels = 0.0, 0.0
        if action == "insert":
            ins = qty
        elif action == "delete":
            dels = qty
        # almacenar y purgar ventana 3s
        lst = self._bins[side]
        lst.append((ts, ins, dels))
        cut = ts - self._win_s
        while lst and lst[0][0] < cut:
            lst.pop(0)

    def on_trade(self, ts: float, side: str, price: float, qty: float) -> None:
        # Nada especial aquí; snapshot usa estado del libro
        pass

    def on_mark(self, ts: float, mark: Optional[float], index: Optional[float]) -> None:
        if mark is None or index in (None, 0.0):
            return
        bps = (mark / index - 1.0) * 10000.0
        self._basis_bps = bps
        if self._last_basis_bps is not None and self._last_basis_ts is not None:
            dt_s = max(ts - self._last_basis_ts, 1e-6)
            self._basis_vel = (bps - self._last_basis_bps) / dt_s
        self._last_basis_bps = bps
        self._last_basis_ts = ts
        if mark > 0.0:
            bps_doc = (index - mark) / mark * 10000.0
            self._basis_bps_doc = bps_doc
            self._basis_doc_win.add(ts, bps_doc)
            self._basis_vel_doc = self._basis_doc_win.derivative_over(self.basis_doc_window_s, ts)
            self._basis_accel_doc = self._basis_doc_win.second_derivative_over(self.basis_doc_window_s, ts)

    # ---- helpers ----
    def _dominance(self) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        bids, asks = self.book.get_head(self.top_n)
        nz_bids = sum(1 for _, q in bids if q > 0)
        nz_asks = sum(1 for _, q in asks if q > 0)
        total = nz_bids + nz_asks
        if total <= 0:
            return None, None, None
        dom_bid = nz_bids / total
        dom_ask = nz_asks / total
        return dom_bid, dom_ask, None

    def _imbalance(self) -> Optional[float]:
        bids, asks = self.book.get_head(self.top_n)
        sb = sum(q for _, q in bids)
        sa = sum(q for _, q in asks)
        tot = sb + sa
        if tot <= 0:
            return None
        return (sb - sa) / tot

    def _dep_and_refill(self) -> Tuple[float, float, float, float]:
        # proxy: dep = deletions/(insertions+deletions), refill = insertions/deletions (cap a 1.0)
        def side_vals(side: str) -> Tuple[float, float]:
            lst = self._bins[side]
            ins = sum(x[1] for x in lst)
            dels = sum(x[2] for x in lst)
            dep = dels / max(ins + dels, 1e-9)
            refill = min(ins / max(dels, 1e-9), 1.0)
            return float(dep), float(refill)

        dep_bid, refill_bid = side_vals("buy")
        dep_ask, refill_ask = side_vals("sell")
        return dep_bid, dep_ask, refill_bid, refill_ask

    # ---- salida ----
    def get_snapshot(self, now_ts: Optional[float] = None) -> Snapshot:
        ts_now = float(now_ts if now_ts is not None else time.time())
        bb, ba = self.book.best()
        spread = None
        if bb is not None and ba is not None:
            spread = max(ba - bb, 0.0)
        wmid = (ba + bb) / 2.0 if (bb is not None and ba is not None) else None

        dom_bid, dom_ask, _ = self._dominance()
        imb = self._imbalance()
        dep_bid, dep_ask, refill_bid, refill_ask = self._dep_and_refill()
        bids, asks = self.book.get_head(self.top_n)
        sb = sum(q for _, q in bids)
        sa = sum(q for _, q in asks)
        tot = sb + sa
        dom_bid_doc_inst: Optional[float] = None
        dom_ask_doc_inst: Optional[float] = None
        if tot > 0:
            dom_bid_doc_inst = sb / tot
            dom_ask_doc_inst = sa / tot
            self._dominance_win.add(ts_now, dom_bid_doc_inst)
        if imb is not None:
            self._imbalance_win.add(ts_now, imb)
        self._bid_vol_win.add(ts_now, sb)
        self._ask_vol_win.add(ts_now, sa)

        dom_bid_doc = self._dominance_win.mean_over(self.dominance_doc_window_s, ts_now) if dom_bid_doc_inst is not None else None
        dom_ask_doc = 1.0 - dom_bid_doc if dom_bid_doc is not None else None
        imbalance_doc = self._imbalance_win.mean_over(self.imbalance_doc_window_s, ts_now)
        depletion_bid_doc = self._bid_vol_win.delta_over(self.depletion_doc_window_s, ts_now)
        depletion_ask_doc = self._ask_vol_win.delta_over(self.depletion_doc_window_s, ts_now)

        return Snapshot(
            spread_usd=spread,
            basis_bps=self._basis_bps,
            basis_vel_bps_s=self._basis_vel,
            basis_bps_doc=self._basis_bps_doc,
            basis_vel_bps_s_doc=self._basis_vel_doc,
            basis_accel_bps_s2_doc=self._basis_accel_doc,
            best_bid=bb,
            best_ask=ba,
            wmid=wmid,
            dom_bid=dom_bid,
            dom_ask=dom_ask,
            dominance_bid_doc=dom_bid_doc,
            dominance_ask_doc=dom_ask_doc,
            imbalance=imb,
            imbalance_doc=imbalance_doc,
            dep_bid=dep_bid,
            dep_ask=dep_ask,
            depletion_bid_doc=depletion_bid_doc,
            depletion_ask_doc=depletion_ask_doc,
            refill_bid_3s=refill_bid,
            refill_ask_3s=refill_ask,
        )
