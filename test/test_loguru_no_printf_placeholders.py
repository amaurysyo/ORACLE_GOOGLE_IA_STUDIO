from __future__ import annotations

import re
from pathlib import Path


PRINTF_PLACEHOLDER_RE = re.compile(r"%(?:\d+)?(?:\.\d+)?[sdf]")


def test_loguru_no_printf_placeholders() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    targets = [repo_root / "oraculo", repo_root / "scripts"]
    offenders: list[str] = []

    for base in targets:
        for path in base.rglob("*.py"):
            try:
                content = path.read_text(encoding="utf-8")
            except Exception:
                continue

            if "from loguru import logger" not in content:
                continue

            if PRINTF_PLACEHOLDER_RE.search(content):
                offenders.append(str(path.relative_to(repo_root)))

    assert not offenders, f"Files with printf-style placeholders in Loguru logs: {offenders}"
