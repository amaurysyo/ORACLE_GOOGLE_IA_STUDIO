from __future__ import annotations

from typing import Any, Optional
import math


def parse_ts_ms(x: Any) -> Optional[int]:
    """Parse timestamp ms from int/float/str; return None if invalid."""
    if x is None:
        return None
    if isinstance(x, bool):
        return None
    if isinstance(x, int):
        if x < 0:
            return None
        return x
    if isinstance(x, float):
        if math.isnan(x) or math.isinf(x):
            return None
        if x < 0:
            return None
        return int(x)
    if isinstance(x, str):
        value = x.strip()
        if not value:
            return None
        try:
            parsed = float(value)
        except ValueError:
            return None
        if math.isnan(parsed) or math.isinf(parsed):
            return None
        if parsed < 0:
            return None
        return int(parsed)
    return None
