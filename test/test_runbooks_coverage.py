from __future__ import annotations

import re
from pathlib import Path
from typing import Set

ALLOWLIST = {"metrics", "metrics_doc"}
RULES_PATH = Path("config/rules.yaml")
RUNBOOKS_DIR = Path("docs/runbooks")


def _load_detectors() -> Set[str]:
    text = RULES_PATH.read_text(encoding="utf-8")
    try:  # Prefer PyYAML when available for full YAML support.
        import yaml  # type: ignore
    except ImportError:
        yaml = None

    if yaml:
        data = yaml.safe_load(text) or {}
        detectors = data.get("detectors", {})
        return set(detectors.keys())

    # Fallback: parse top-level detector keys manually to avoid external deps.
    detectors: Set[str] = set()
    in_detectors = False
    for line in text.splitlines():
        if re.match(r"^detectors\\s*:", line):
            in_detectors = True
            continue

        if not in_detectors:
            continue

        if re.match(r"^[^\\s]", line):
            break

        match = re.match(r"^\\s{2}([A-Za-z0-9_]+):", line)
        if match:
            detectors.add(match.group(1))

    return detectors


def test_runbook_exists_for_each_detector() -> None:
    detectors = _load_detectors()
    missing = []

    for detector in sorted(detectors - ALLOWLIST):
        expected = RUNBOOKS_DIR / f"{detector}.md"
        if not expected.exists():
            missing.append(
                f"Missing runbook for detector: {detector} → expected {expected}"
            )

    assert not missing, "\\n".join(missing)
