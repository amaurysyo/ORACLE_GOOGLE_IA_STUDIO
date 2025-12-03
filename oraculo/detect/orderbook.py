
from __future__ import annotations
import bisect
from typing import Dict, List, Tuple, Optional

class L2Book:
    """
    Lightweight L2 orderbook for top-N levels.
    Maintains two sorted lists of (price, qty) for bids (descending) and asks (ascending).
    """
    def __init__(self, depth_levels: int = 1000) -> None:
        self.N = depth_levels
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self._bid_prices: List[float] = []  # sorted DESC
        self._ask_prices: List[float] = []  # sorted ASC

    def _insert_price(self, side: str, price: float) -> None:
        if side == "buy":
            # keep DESC
            idx = len(self._bid_prices) - bisect.bisect_left(list(reversed(self._bid_prices)), price)
            self._bid_prices.insert(idx, price)
            # cap to N
            if len(self._bid_prices) > self.N:
                p_tail = self._bid_prices.pop()
                self.bids.pop(p_tail, None)
        else:
            idx = bisect.bisect_left(self._ask_prices, price)
            self._ask_prices.insert(idx, price)
            if len(self._ask_prices) > self.N:
                p_tail = self._ask_prices.pop()
                self.asks.pop(p_tail, None)

    def _delete_price(self, side: str, price: float) -> None:
        prices = self._bid_prices if side == "buy" else self._ask_prices
        try:
            prices.remove(price)
        except ValueError:
            pass

    def apply(self, side: str, action: str, price: float, qty: float) -> None:
        """
        side: 'buy'|'sell'
        action: 'insert'|'update'|'delete'
        """
        book = self.bids if side == "buy" else self.asks
        if action == "delete" or qty <= 0:
            if price in book:
                book.pop(price, None)
                self._delete_price(side, price)
            return

        # insert / update
        is_new = price not in book
        book[price] = qty
        if is_new:
            self._insert_price(side, price)

    def best(self) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
        bid = self._bid_prices[0] if self._bid_prices else None
        ask = self._ask_prices[0] if self._ask_prices else None
        bid_qty = self.bids.get(bid) if bid is not None else None
        ask_qty = self.asks.get(ask) if ask is not None else None
        return bid, bid_qty, ask, ask_qty

    def sum_topn(self, n: int | None = None) -> Tuple[float, float]:
        n = n or self.N
        sb = 0.0
        for i, p in enumerate(self._bid_prices):
            if i >= n: break
            sb += self.bids[p]
        sa = 0.0
        for i, p in enumerate(self._ask_prices):
            if i >= n: break
            sa += self.asks[p]
        return sb, sa

    def imbalance(self, n: int | None = None) -> Optional[float]:
        sb, sa = self.sum_topn(n)
        denom = sb + sa
        if denom <= 0:
            return None
        return (sb - sa) / denom

    def dominance(self, n: int | None = None) -> Tuple[Optional[float], Optional[float]]:
        sb, sa = self.sum_topn(n)
        denom = sb + sa
        if denom <= 0:
            return None, None
        return sb / denom, sa / denom  # dom_bid, dom_ask

    def depletion(self, prev_b: float, prev_a: float, n: int | None = None) -> Tuple[float, float]:
        """Return depletion percentages for (bid, ask): max(0, (prev - now)/prev)."""
        b, a = self.sum_topn(n)
        dep_bid = 0.0 if prev_b <= 0 else max(0.0, (prev_b - b) / max(prev_b, 1e-9))
        dep_ask = 0.0 if prev_a <= 0 else max(0.0, (prev_a - a) / max(prev_a, 1e-9))
        return dep_bid, dep_ask

    def spread_wmid(self) -> Tuple[Optional[float], Optional[float]]:
        bid, _, ask, _ = self.best()
        if bid is None or ask is None:
            return None, None
        spread = ask - bid
        wmid = (ask + bid) / 2.0
        return spread, wmid
