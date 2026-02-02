from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from basis_delta_nrt.domain.experiment_results import compute_results


def prune_runs(runs: List[Dict[str, Any]], keep_latest: int) -> List[Dict[str, Any]]:
    """Return runs ordered by created_at with pruning applied."""
    if keep_latest <= 0:
        return []
    ordered = sorted(
        runs,
        key=lambda run: run.get("created_at", ""),
    )
    if len(ordered) <= keep_latest:
        return ordered
    return ordered[-keep_latest:]


def build_run_response(run: Dict[str, Any]) -> Dict[str, Any]:
    sources_used = {
        "price_basis": run.get("price_basis_source", "unknown"),
        "deltas": run.get("deltas_source", "unknown"),
        "events": run.get("events_source", "unknown"),
    }
    return {
        "run_id": run.get("run_id"),
        "created_at": run.get("created_at"),
        "results_version": run.get("results_version"),
        "results_json": run.get("results_json"),
        "sources_used": sources_used,
    }


def create_run(bundle: Dict[str, Any]) -> Dict[str, Any]:
    results = compute_results(bundle)
    return {
        "run_id": bundle.get("run_id", "run"),
        "created_at": datetime.utcnow().isoformat(),
        "results_version": "advanced_v1",
        "results_json": results,
        "price_basis_source": bundle.get("price_basis_source", "price_basis"),
        "deltas_source": bundle.get("deltas_source", "deltas"),
        "events_source": bundle.get("events_source", "events"),
    }
