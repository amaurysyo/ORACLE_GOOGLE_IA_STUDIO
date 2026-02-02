from __future__ import annotations

from typing import Any, Dict


def save_run(run: Dict[str, Any], results_json: Dict[str, Any]) -> Dict[str, Any]:
    stored = dict(run)
    stored["results_version"] = "advanced_v1"
    stored["results_json"] = results_json
    return stored
