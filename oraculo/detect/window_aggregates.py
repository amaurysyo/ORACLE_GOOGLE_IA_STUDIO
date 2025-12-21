from __future__ import annotations

import math
from collections import deque
from typing import Deque, Optional, Tuple


class RollingTimeWindow:
    """
    Ventana temporal simple basada en deque.

    Mantiene muestras ordenadas (ts, value) y permite obtener agregados sobre una
    ventana temporal usando los timestamps como ancla. Se descartan muestras
    demasiado antiguas (max_age_s) para acotar memoria.
    """

    def __init__(self, max_age_s: float = 600.0):
        self.max_age_s = float(max_age_s)
        self._samples: Deque[Tuple[float, float]] = deque()

    def _trim(self, now_ts: float) -> None:
        """Descarta muestras más antiguas que max_age_s respecto a now_ts."""
        cutoff = now_ts - self.max_age_s
        while self._samples and self._samples[0][0] < cutoff:
            self._samples.popleft()

    def add(self, ts: float, value: float) -> None:
        """Añade una muestra válida (descarta NaN/inf)."""
        if value is None:
            return
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return
        self._samples.append((float(ts), float(value)))
        self._trim(ts)

    def mean_over(self, window_s: float, now_ts: Optional[float] = None) -> Optional[float]:
        """Media de las muestras dentro de [now_ts - window_s, now_ts]."""
        if not self._samples:
            return None
        anchor = self._samples[-1][0] if now_ts is None else float(now_ts)
        self._trim(anchor)
        start_ts = anchor - float(window_s)
        vals = [v for ts, v in self._samples if ts >= start_ts]
        if not vals:
            return None
        return float(sum(vals) / len(vals))

    def _find_reference(self, window_s: float, anchor_ts: float) -> Optional[Tuple[float, float]]:
        """
        Retorna la muestra más reciente con ts <= anchor_ts - window_s.
        Se itera en reversa para mantener O(n) acotado por el tamaño de ventana.
        """
        target = anchor_ts - float(window_s)
        for ts, val in reversed(self._samples):
            if ts <= target:
                return ts, val
        return None

    def delta_over(self, window_s: float, now_ts: Optional[float] = None) -> Optional[float]:
        """Diferencia value(now) - value(now - window_s) usando muestras separadas por la ventana."""
        if len(self._samples) < 2:
            return None
        anchor_ts, anchor_val = self._samples[-1]
        if now_ts is not None:
            anchor_ts = float(now_ts)
        self._trim(anchor_ts)
        if not self._samples:
            return None
        anchor_ts, anchor_val = self._samples[-1]
        ref = self._find_reference(window_s, anchor_ts)
        if ref is None:
            return None
        ref_ts, ref_val = ref
        if anchor_ts <= ref_ts:
            return None
        return float(anchor_val - ref_val)

    def derivative_over(self, window_s: float, now_ts: Optional[float] = None) -> Optional[float]:
        """Derivada aproximada (delta/dt) entre muestras separadas por ~window_s."""
        if len(self._samples) < 2:
            return None
        anchor_ts, anchor_val = self._samples[-1]
        if now_ts is not None:
            anchor_ts = float(now_ts)
        self._trim(anchor_ts)
        if not self._samples:
            return None
        anchor_ts, anchor_val = self._samples[-1]
        ref = self._find_reference(window_s, anchor_ts)
        if ref is None:
            return None
        ref_ts, ref_val = ref
        dt = max(anchor_ts - ref_ts, 1e-6)
        return float((anchor_val - ref_val) / dt)

    def second_derivative_over(self, window_s: float, now_ts: Optional[float] = None) -> Optional[float]:
        """
        Segunda derivada aproximada empleando dos derivadas consecutivas separadas
        por la misma ventana. Requiere ≥3 muestras espaciadas.
        """
        if len(self._samples) < 3:
            return None
        anchor_ts, _ = self._samples[-1]
        if now_ts is not None:
            anchor_ts = float(now_ts)
        self._trim(anchor_ts)
        if len(self._samples) < 3:
            return None

        last_ts, last_val = self._samples[-1]
        mid = self._find_reference(window_s, last_ts)
        if mid is None:
            return None
        mid_ts, mid_val = mid
        prev = self._find_reference(window_s, mid_ts)
        if prev is None:
            return None
        prev_ts, prev_val = prev

        dt1 = max(last_ts - mid_ts, 1e-6)
        dt0 = max(mid_ts - prev_ts, 1e-6)
        d1 = (last_val - mid_val) / dt1
        d0 = (mid_val - prev_val) / dt0
        dt = max(last_ts - mid_ts, 1e-6)
        return float((d1 - d0) / dt)
