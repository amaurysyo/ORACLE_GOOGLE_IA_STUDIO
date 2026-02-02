from __future__ import annotations

import math
from bisect import bisect_left
from dataclasses import dataclass
from statistics import median
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from basis_delta_nrt.domain.ts_parse import parse_ts_ms


@dataclass(frozen=True)
class WarningItem:
    code: str
    message: str
    details: Optional[Dict[str, Any]] = None


def _numeric_fields(points: Sequence[Dict[str, Any]]) -> List[str]:
    if not points:
        return []
    fields: List[str] = []
    for key in points[0].keys():
        if key == "ts_ms":
            continue
        for point in points:
            value = point.get(key)
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                fields.append(key)
                break
    return fields


def _extract_points(series: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    points: List[Dict[str, Any]] = []
    for entry in series:
        ts_ms = parse_ts_ms(entry.get("ts_ms"))
        if ts_ms is None:
            continue
        point = dict(entry)
        point["ts_ms"] = ts_ms
        points.append(point)
    return points


def _extreme_point(points: Sequence[Dict[str, Any]], ts_ms: int) -> Optional[Dict[str, Any]]:
    selected = None
    for point in points:
        if point.get("ts_ms") == ts_ms:
            selected = point
    return selected


def _slope_per_s(points: Sequence[Dict[str, Any]], field: str) -> Optional[float]:
    if len(points) < 2:
        return None
    ts_first_ms = min(point["ts_ms"] for point in points)
    ts_last_ms = max(point["ts_ms"] for point in points)
    if ts_first_ms == ts_last_ms:
        return None
    first_point = _extreme_point(points, ts_first_ms)
    last_point = _extreme_point(points, ts_last_ms)
    if not first_point or not last_point:
        return None
    first_val = first_point.get(field)
    last_val = last_point.get(field)
    if not isinstance(first_val, (int, float)) or not isinstance(last_val, (int, float)):
        return None
    dt_s = (ts_last_ms - ts_first_ms) / 1000.0
    if dt_s == 0:
        return None
    return (last_val - first_val) / dt_s


def compute_basic_results(bundle: Dict[str, Any]) -> Dict[str, Any]:
    price_points = _extract_points(bundle.get("price_basis", []))
    delta_points = _extract_points(bundle.get("deltas", []))
    events = _extract_points(bundle.get("events", []))

    price_fields = _numeric_fields(price_points)
    delta_fields = _numeric_fields(delta_points)

    results: Dict[str, Any] = {
        "results_version": "advanced_v1",
        "price_basis": {
            "points": len(price_points),
            "ts_first_ms": min((p["ts_ms"] for p in price_points), default=None),
            "ts_last_ms": max((p["ts_ms"] for p in price_points), default=None),
            "fields": {},
        },
        "deltas": {
            "points": len(delta_points),
            "ts_first_ms": min((p["ts_ms"] for p in delta_points), default=None),
            "ts_last_ms": max((p["ts_ms"] for p in delta_points), default=None),
            "fields": {},
        },
        "events": {
            "events": len(events),
            "ts_last_ms": max((e["ts_ms"] for e in events), default=None),
            "by_kind": {},
        },
        "fields_present": {
            "price_basis": price_fields,
            "deltas": delta_fields,
            "events": ["kind"],
        },
        "warnings": [],
    }

    for field in price_fields:
        values = [p.get(field) for p in price_points if isinstance(p.get(field), (int, float))]
        results["price_basis"]["fields"][field] = {
            "min": min(values) if values else None,
            "max": max(values) if values else None,
            "slope_per_s": _slope_per_s(price_points, field),
        }

    for field in delta_fields:
        values = [p.get(field) for p in delta_points if isinstance(p.get(field), (int, float))]
        results["deltas"]["fields"][field] = {
            "min": min(values) if values else None,
            "max": max(values) if values else None,
            "slope_per_s": _slope_per_s(delta_points, field),
        }

    by_kind: Dict[str, int] = {}
    for event in events:
        kind = event.get("kind")
        if isinstance(kind, str) and kind:
            by_kind[kind] = by_kind.get(kind, 0) + 1
    results["events"]["by_kind"] = by_kind

    return results


def _extract_series(points: Sequence[Dict[str, Any]], field: str) -> List[Tuple[int, float]]:
    series: List[Tuple[int, float]] = []
    for point in points:
        value = point.get(field)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            series.append((point["ts_ms"], float(value)))
    return series


def _nearest_value(sorted_series: Sequence[Tuple[int, float]], ts_ms: int) -> Optional[Tuple[int, float]]:
    if not sorted_series:
        return None
    idx = bisect_left(sorted_series, (ts_ms, -math.inf))
    candidates = []
    if idx < len(sorted_series):
        candidates.append(sorted_series[idx])
    if idx > 0:
        candidates.append(sorted_series[idx - 1])
    if not candidates:
        return None
    return min(candidates, key=lambda item: abs(item[0] - ts_ms))


def _nearest_forward(sorted_series: Sequence[Tuple[int, float]], ts_ms: int) -> Optional[Tuple[int, float]]:
    if not sorted_series:
        return None
    idx = bisect_left(sorted_series, (ts_ms, -math.inf))
    if idx >= len(sorted_series):
        return None
    return sorted_series[idx]


def _align_nearest(
    x_series: Sequence[Tuple[int, float]],
    y_series: Sequence[Tuple[int, float]],
    max_dt_ms: int,
) -> List[Tuple[float, float]]:
    if not x_series or not y_series:
        return []
    x_sorted = sorted(x_series, key=lambda item: item[0])
    y_sorted = sorted(y_series, key=lambda item: item[0])
    pairs: List[Tuple[float, float]] = []
    for ts_ms, x_val in x_sorted:
        match = _nearest_value(y_sorted, ts_ms)
        if match and abs(match[0] - ts_ms) <= max_dt_ms:
            pairs.append((x_val, match[1]))
    return pairs


def _pearson(pairs: Sequence[Tuple[float, float]]) -> Optional[float]:
    n = len(pairs)
    if n < 2:
        return None
    xs = [p[0] for p in pairs]
    ys = [p[1] for p in pairs]
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    num = sum((x - mean_x) * (y - mean_y) for x, y in pairs)
    denom_x = sum((x - mean_x) ** 2 for x in xs)
    denom_y = sum((y - mean_y) ** 2 for y in ys)
    if denom_x <= 0 or denom_y <= 0:
        return None
    return num / math.sqrt(denom_x * denom_y)


def _guess_sample_s(points: Sequence[Dict[str, Any]]) -> Optional[int]:
    if len(points) < 2:
        return None
    times = sorted(p["ts_ms"] for p in points)
    deltas = [b - a for a, b in zip(times, times[1:]) if b > a]
    if not deltas:
        return None
    return int(median(deltas) / 1000)


def _select_field(points: Sequence[Dict[str, Any]], preferred: Sequence[str]) -> Optional[str]:
    fields = _numeric_fields(points)
    for name in preferred:
        if name in fields:
            return name
    return fields[0] if fields else None


def _compute_correlations(
    price_points: Sequence[Dict[str, Any]],
    delta_points: Sequence[Dict[str, Any]],
    sample_s: Optional[int],
) -> Dict[str, Any]:
    y_field = _select_field(price_points, ["basis_br_pct", "basis_abs"])
    x_field = _select_field(
        delta_points,
        ["delta_perp_accum", "delta_spot_accum", "delta_perp", "delta_spot"],
    )
    if not y_field or not x_field:
        return {"x_field": x_field, "y_field": y_field, "n": 0, "pearson": None}
    x_series = _extract_series(delta_points, x_field)
    y_series = _extract_series(price_points, y_field)
    inferred_sample_s = sample_s or _guess_sample_s(price_points) or _guess_sample_s(delta_points)
    max_dt_ms = int((inferred_sample_s * 500) if inferred_sample_s and inferred_sample_s > 0 else 30_000)
    pairs = _align_nearest(x_series, y_series, max_dt_ms)
    pearson = _pearson(pairs)
    return {
        "x_field": x_field,
        "y_field": y_field,
        "n": len(pairs),
        "pearson": pearson,
    }


def _compute_leadlag(
    price_points: Sequence[Dict[str, Any]],
    delta_points: Sequence[Dict[str, Any]],
    sample_s: Optional[int],
) -> Dict[str, Any]:
    y_field = _select_field(price_points, ["basis_br_pct", "basis_abs"])
    x_field = _select_field(
        delta_points,
        ["delta_perp_accum", "delta_spot_accum", "delta_perp", "delta_spot"],
    )
    if not y_field or not x_field:
        return {
            "x_field": x_field,
            "y_field": y_field,
            "n": 0,
            "best_lag_s": None,
            "best_abs_pearson": None,
            "dir": "none",
        }
    x_series = _extract_series(delta_points, x_field)
    y_series = _extract_series(price_points, y_field)
    inferred_sample_s = sample_s or _guess_sample_s(price_points) or _guess_sample_s(delta_points)
    step_s = max(int(inferred_sample_s or 0), 60)
    max_lag_s = 1800
    max_dt_ms = int((inferred_sample_s * 500) if inferred_sample_s and inferred_sample_s > 0 else 30_000)

    best_lag_s: Optional[int] = None
    best_abs: Optional[float] = None
    best_n = 0
    for lag_s in range(-max_lag_s, max_lag_s + 1, step_s):
        shifted = [(ts + lag_s * 1000, val) for ts, val in x_series]
        pairs = _align_nearest(shifted, y_series, max_dt_ms)
        pearson = _pearson(pairs)
        if pearson is None:
            continue
        abs_val = abs(pearson)
        if best_abs is None or abs_val > best_abs:
            best_abs = abs_val
            best_lag_s = lag_s
            best_n = len(pairs)

    direction = "none"
    if best_lag_s is not None and best_lag_s < 0:
        direction = "x_leads_y"
    elif best_lag_s is not None and best_lag_s > 0:
        direction = "y_leads_x"

    return {
        "x_field": x_field,
        "y_field": y_field,
        "n": best_n,
        "best_lag_s": best_lag_s,
        "best_abs_pearson": best_abs,
        "dir": direction,
    }


def _compute_event_impact(
    price_points: Sequence[Dict[str, Any]],
    events: Sequence[Dict[str, Any]],
    horizons_s: Sequence[int],
    top_k_kinds: int,
) -> Tuple[Dict[str, Any], Optional[WarningItem]]:
    price_field = _select_field(price_points, ["basis_abs", "basis_br_pct"])
    if not price_field:
        return (
            {
                "horizons_s": list(horizons_s),
                "top_k_kinds": top_k_kinds,
                "kinds": {},
            },
            None,
        )
    series = sorted(_extract_series(price_points, price_field), key=lambda item: item[0])
    grouped: Dict[str, List[int]] = {}
    for event in events:
        kind = event.get("kind")
        if isinstance(kind, str) and kind:
            grouped.setdefault(kind, []).append(event["ts_ms"])
    sorted_kinds = sorted(grouped.items(), key=lambda item: len(item[1]), reverse=True)
    truncated = len(sorted_kinds) > top_k_kinds
    selected = sorted_kinds[:top_k_kinds]

    kinds_output: Dict[str, Any] = {}
    for kind, timestamps in selected:
        matched_counts = {str(h): 0 for h in horizons_s}
        returns_by_h: Dict[str, List[float]] = {str(h): [] for h in horizons_s}
        for ts_ms in timestamps:
            p0_tuple = _nearest_value(series, ts_ms)
            if not p0_tuple:
                continue
            p0 = p0_tuple[1]
            if p0 <= 0:
                continue
            for horizon_s in horizons_s:
                target_ts = ts_ms + horizon_s * 1000
                pf_tuple = _nearest_forward(series, target_ts)
                if not pf_tuple:
                    continue
                pf = pf_tuple[1]
                ret = (pf - p0) / p0
                matched_counts[str(horizon_s)] += 1
                returns_by_h[str(horizon_s)].append(ret)
        mean_return = {}
        hit_rate = {}
        for horizon_s in horizons_s:
            key = str(horizon_s)
            vals = returns_by_h[key]
            mean_return[key] = sum(vals) / len(vals) if vals else None
            hit_rate[key] = (
                sum(1 for v in vals if v > 0) / len(vals)
                if vals
                else None
            )
        kinds_output[kind] = {
            "n_events": len(timestamps),
            "matched": matched_counts,
            "mean_return": mean_return,
            "hit_rate": hit_rate,
        }

    warning = None
    if truncated:
        warning = WarningItem(
            code="RESULTS_TRUNCATED_TOP_KINDS",
            message="Event impact kinds truncated to top_k_kinds",
            details={"top_k_kinds": top_k_kinds},
        )

    output = {
        "horizons_s": list(horizons_s),
        "top_k_kinds": top_k_kinds,
        "kinds": kinds_output,
    }
    return output, warning


def compute_advanced_results(bundle: Dict[str, Any]) -> Dict[str, Any]:
    price_points = _extract_points(bundle.get("price_basis", []))
    delta_points = _extract_points(bundle.get("deltas", []))
    events = _extract_points(bundle.get("events", []))
    sample_s = bundle.get("bundle_meta", {}).get("sample_s")

    correlations = _compute_correlations(price_points, delta_points, sample_s)
    leadlag = _compute_leadlag(price_points, delta_points, sample_s)
    event_impact, impact_warning = _compute_event_impact(price_points, events, [300, 900, 3600], 10)

    warnings: List[WarningItem] = []
    if correlations.get("n", 0) < 2 or leadlag.get("n", 0) < 2:
        warnings.append(
            WarningItem(
                code="RESULTS_INSUFFICIENT_DATA",
                message="Insufficient data for advanced metrics",
            )
        )
    if impact_warning:
        warnings.append(impact_warning)

    return {
        "correlations": correlations,
        "leadlag": leadlag,
        "event_impact": event_impact,
        "warnings": warnings,
    }


def compute_results(bundle: Dict[str, Any]) -> Dict[str, Any]:
    basic = compute_basic_results(bundle)
    advanced = compute_advanced_results(bundle)

    warnings = list(basic.get("warnings", []))
    for warning in advanced.get("warnings", []):
        warnings.append(
            {
                "code": warning.code,
                "message": warning.message,
                "details": warning.details,
            }
        )

    basic["advanced"] = {
        "correlations": advanced["correlations"],
        "leadlag": advanced["leadlag"],
        "event_impact": advanced["event_impact"],
    }
    basic["warnings"] = warnings
    basic["results_version"] = "advanced_v1"
    return basic
