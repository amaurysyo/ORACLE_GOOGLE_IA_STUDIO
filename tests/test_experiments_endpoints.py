from __future__ import annotations

from typing import Any
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from basis_delta_nrt.api.experiments import build_run_response, create_run
from basis_delta_nrt.domain.experiment_results import compute_basic_results, compute_results


def _assert_no_list_of_dicts(payload: Any, path: tuple[str, ...] = ()) -> None:
    if isinstance(payload, list):
        if path and path[-1] == "warnings":
            return
        assert not any(isinstance(item, dict) for item in payload)
        for item in payload:
            _assert_no_list_of_dicts(item, path)
    elif isinstance(payload, dict):
        for key, value in payload.items():
            _assert_no_list_of_dicts(value, path + (str(key),))


def _bundle_with_series() -> dict[str, Any]:
    price_basis = [
        {"ts_ms": 0, "basis_br_pct": 1.0, "basis_abs": 100.0},
        {"ts_ms": 60_000, "basis_br_pct": 2.0, "basis_abs": 101.0},
        {"ts_ms": 120_000, "basis_br_pct": 3.0, "basis_abs": 102.0},
        {"ts_ms": 180_000, "basis_br_pct": 4.0, "basis_abs": 103.0},
        {"ts_ms": 240_000, "basis_br_pct": 5.0, "basis_abs": 104.0},
        {"ts_ms": 300_000, "basis_br_pct": 6.0, "basis_abs": 105.0},
        {"ts_ms": 360_000, "basis_br_pct": 7.0, "basis_abs": 106.0},
        {"ts_ms": 420_000, "basis_br_pct": 8.0, "basis_abs": 107.0},
        {"ts_ms": 480_000, "basis_br_pct": 9.0, "basis_abs": 108.0},
        {"ts_ms": 540_000, "basis_br_pct": 10.0, "basis_abs": 109.0},
    ]
    deltas = [
        {"ts_ms": 0, "delta_perp_accum": 10.0},
        {"ts_ms": 60_000, "delta_perp_accum": 11.0},
        {"ts_ms": 120_000, "delta_perp_accum": 12.0},
        {"ts_ms": 180_000, "delta_perp_accum": 13.0},
        {"ts_ms": 240_000, "delta_perp_accum": 14.0},
        {"ts_ms": 300_000, "delta_perp_accum": 15.0},
        {"ts_ms": 360_000, "delta_perp_accum": 16.0},
        {"ts_ms": 420_000, "delta_perp_accum": 17.0},
        {"ts_ms": 480_000, "delta_perp_accum": 18.0},
        {"ts_ms": 540_000, "delta_perp_accum": 19.0},
    ]
    events = [
        {"ts_ms": 60_000, "kind": "SlipIce"},
        {"ts_ms": 120_000, "kind": "SlipIce"},
        {"ts_ms": 180_000, "kind": "Rain"},
    ]
    return {
        "price_basis": price_basis,
        "deltas": deltas,
        "events": events,
        "bundle_meta": {"sample_s": 60},
        "price_basis_source": "price_basis",
        "deltas_source": "deltas",
        "events_source": "events",
    }


def test_temporal_extremes_and_slope() -> None:
    bundle = {
        "price_basis": [
            {"ts_ms": 3000, "basis_abs": 3.0},
            {"ts_ms": 1000, "basis_abs": 1.0},
            {"ts_ms": 2000, "basis_abs": 2.0},
        ],
        "deltas": [],
        "events": [
            {"ts_ms": 2000, "kind": "A"},
            {"ts_ms": 1000, "kind": "B"},
        ],
    }
    results = compute_basic_results(bundle)
    assert results["price_basis"]["ts_first_ms"] == 1000
    assert results["price_basis"]["ts_last_ms"] == 3000
    slope = results["price_basis"]["fields"]["basis_abs"]["slope_per_s"]
    assert slope == 1.0
    assert results["events"]["ts_last_ms"] == 2000
    assert results["events"]["by_kind"] == {"A": 1, "B": 1}


def test_advanced_results_structure_and_size() -> None:
    bundle = _bundle_with_series()
    results = compute_results(bundle)
    assert results["results_version"] == "advanced_v1"
    assert "advanced" in results
    advanced = results["advanced"]
    assert "correlations" in advanced
    assert "leadlag" in advanced
    assert "event_impact" in advanced
    assert len(advanced["event_impact"]["kinds"]) <= 10
    assert len(advanced["event_impact"]["horizons_s"]) <= 4
    _assert_no_list_of_dicts(results)


def test_sources_used_contract() -> None:
    bundle = _bundle_with_series()
    run = create_run(bundle)
    response = build_run_response(run)
    assert response["results_version"] == "advanced_v1"
    assert response["sources_used"] == {
        "price_basis": "price_basis",
        "deltas": "deltas",
        "events": "events",
    }
