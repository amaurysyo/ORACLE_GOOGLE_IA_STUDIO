from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional
import math


MetricTransform = Callable[[float], float]


@dataclass
class MetricResolution:
    value: Optional[float]
    used_key: Optional[str]
    used_source: Optional[str]


def _get(snap: Any, key: str) -> Any:
    if snap is None or key is None:
        return None
    if isinstance(snap, dict):
        return snap.get(key)
    return getattr(snap, key, None)


def _clean(val: Any) -> Optional[float]:
    try:
        fval = float(val)
    except (TypeError, ValueError):
        return None
    if math.isnan(fval) or math.isinf(fval):
        return None
    return fval


def resolve(
    snap: Any,
    primary_key: str,
    fallback_key: str | None = None,
    *,
    source: str = "auto",
    transform: MetricTransform | None = None,
) -> MetricResolution:
    """
    Devuelve el valor de una métrica seleccionando entre fuente DOC y legacy.

    - source: "legacy" | "doc" | "auto".
    - primary_key: métrica DOC (o principal) a consultar.
    - fallback_key: clave legacy para fallback. Si no se pasa, usa primary_key.
    - transform: función opcional aplicada SOLO cuando se utiliza la fuente DOC
      (p.ej. invertir signo para mantener semántica legacy).
    """
    source = (source or "auto").lower()
    fb_key = fallback_key or primary_key

    doc_val = _clean(_get(snap, primary_key))
    legacy_val = _clean(_get(snap, fb_key))

    if source == "doc":
        if doc_val is None:
            return MetricResolution(None, None, "doc")
        return MetricResolution(transform(doc_val) if transform else doc_val, primary_key, "doc")

    if source == "legacy":
        if legacy_val is None:
            return MetricResolution(None, None, "legacy")
        return MetricResolution(legacy_val, fb_key, "legacy")

    # auto: preferir DOC si existe; si no, fallback a legacy
    if doc_val is not None:
        val = transform(doc_val) if transform else doc_val
        return MetricResolution(val, primary_key, "doc")

    if legacy_val is not None:
        return MetricResolution(legacy_val, fb_key, "legacy")

    return MetricResolution(None, None, None)
