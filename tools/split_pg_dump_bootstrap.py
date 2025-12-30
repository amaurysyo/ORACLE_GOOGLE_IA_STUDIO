from __future__ import annotations

import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


"""
After updating the dump, run:
    python tools/split_pg_dump_bootstrap.py
    python tools/export_timescale_layer.py
to regenerate full bootstrap set.
"""

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_PATH = PROJECT_ROOT / "SQL" / "oraculo_schema_only.sql"
BOOTSTRAP_DIR = PROJECT_ROOT / "SQL" / "bootstrap"

DOC_HEADER = (
    "-- Generated from SQL/oraculo_schema_only.sql by tools/split_pg_dump_bootstrap.py"
)
TABLE_PREAMBLE_MARKER = "SET default_table_access_method = heap;"
METADATA_PATTERN = re.compile(r"-- Name: (.*?); Type: ([^;]+); Schema: ([^;]+); Owner:")
REQUIRED_TYPES = ("instrument_id_t", "side_t", "action_t", "severity_t")
EXTENSION_LINE = "CREATE EXTENSION IF NOT EXISTS timescaledb;"

OUTPUT_KEYS = [
    "00_extensions.sql",
    "10_core_schema.sql",
    "20_core_tables.sql",
    "30_core_functions_views.sql",
]

TYPE_ROUTING = {
    "EXTENSION": "00_extensions.sql",
    "SCHEMA": "10_core_schema.sql",
    "TYPE": "10_core_schema.sql",
    "DOMAIN": "10_core_schema.sql",
    "ENUM": "10_core_schema.sql",
    "TABLE": "20_core_tables.sql",
    "SEQUENCE": "20_core_tables.sql",
    "INDEX": "20_core_tables.sql",
    "CONSTRAINT": "20_core_tables.sql",
    "FK CONSTRAINT": "20_core_tables.sql",
    "SEQUENCE OWNED BY": "20_core_tables.sql",
    "DEFAULT": "20_core_tables.sql",
    "FUNCTION": "30_core_functions_views.sql",
    "PROCEDURE": "30_core_functions_views.sql",
    "VIEW": "30_core_functions_views.sql",
    "MATERIALIZED VIEW": "30_core_functions_views.sql",
    "TRIGGER": "30_core_functions_views.sql",
}


def read_blocks(lines: Iterable[str]) -> List[List[str]]:
    blocks: List[List[str]] = []
    current: List[str] = []

    for line in lines:
        if line.startswith("-- TOC entry "):
            if current:
                blocks.append(current)
            current = [line]
            continue

        if METADATA_PATTERN.match(line):
            if not current:
                current = [line]
            elif current[0].startswith("-- TOC entry "):
                current.append(line)
            else:
                blocks.append(current)
                current = [line]
            continue

        if current:
            current.append(line)
    if current:
        blocks.append(current)
    return blocks


def parse_metadata(block: Iterable[str]) -> Optional[Tuple[str, str, str]]:
    for line in block:
        match = METADATA_PATTERN.search(line)
        if match:
            return match.group(1), match.group(2).strip(), match.group(3).strip()
    return None


def classify_block(type_name: str) -> Optional[str]:
    return TYPE_ROUTING.get(type_name.upper())


def is_timescale_internal(schema: str, block_text: str) -> bool:
    if schema.startswith(
        (
            "_timescaledb_",
            "timescaledb_information",
            "timescaledb_experimental",
            "_timescaledb_functions",
        )
    ):
        return True

    return "_timescaledb_" in block_text


def validate_required_types(outputs: Dict[str, List[str]]) -> None:
    body_text = "\n".join(
        segment for segments in outputs.values() for segment in segments
    )

    for type_name in REQUIRED_TYPES:
        reference_pattern = re.compile(rf"\bpublic\.{re.escape(type_name)}\b")
        definition_pattern = re.compile(
            rf"CREATE\s+(?:TYPE|DOMAIN)\s+public\.{re.escape(type_name)}\b",
            re.IGNORECASE,
        )

        has_reference = bool(reference_pattern.search(body_text))
        has_definition = bool(definition_pattern.search(body_text))

        if has_reference and not has_definition:
            raise ValueError(
                f"Bootstrap output references public.{type_name} but no CREATE TYPE/DOMAIN/ENUM is present. "
                "Ensure custom types are included in 10_core_schema.sql."
            )


def strip_metadata_and_extract_preamble(
    block: List[str],
) -> Tuple[List[str], List[str]]:
    idx = 0
    while idx < len(block) and block[idx].lstrip().startswith("--"):
        idx += 1

    while idx < len(block) and not block[idx].strip():
        idx += 1

    cleaned: List[str] = []
    preamble: List[str] = []

    for line in block[idx:]:
        if line.strip() == TABLE_PREAMBLE_MARKER:
            preamble.append(line.rstrip("\n"))
            continue
        cleaned.append(line)

    while cleaned and not cleaned[0].strip():
        cleaned.pop(0)
    while cleaned and cleaned[0].strip() == "--":
        cleaned.pop(0)
    while cleaned and not cleaned[-1].strip():
        cleaned.pop()
    while cleaned and cleaned[-1].strip() == "--":
        cleaned.pop()

    return cleaned, preamble


def write_outputs(outputs: Dict[str, List[str]], table_preamble: List[str]) -> None:
    BOOTSTRAP_DIR.mkdir(parents=True, exist_ok=True)

    for key in OUTPUT_KEYS:
        path = BOOTSTRAP_DIR / key
        segments: List[str] = []
        if key == "20_core_tables.sql" and table_preamble:
            segments.append("\n".join(table_preamble))
        segments.extend(outputs.get(key, []))

        body = "\n\n".join(segment.rstrip() for segment in segments if segment.strip())
        content = f"{DOC_HEADER}\n\n{body}\n"
        path.write_text(content, encoding="utf-8")


def main() -> None:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Expected dump at {INPUT_PATH}")

    lines = INPUT_PATH.read_text(encoding="utf-8").splitlines(keepends=True)
    blocks = read_blocks(lines)

    outputs: Dict[str, List[str]] = defaultdict(list)
    table_preamble: List[str] = []
    skipped: List[Tuple[str, str, str]] = []

    for block in blocks:
        metadata = parse_metadata(block)
        if not metadata:
            continue

        name, type_name, schema = metadata
        block_text = "".join(block)

        if is_timescale_internal(schema, block_text):
            continue

        target = classify_block(type_name)

        if not target:
            skipped.append(metadata)
            continue

        cleaned_lines, preamble = strip_metadata_and_extract_preamble(block)
        table_preamble.extend(preamble)

        if cleaned_lines:
            outputs[target].append("".join(cleaned_lines))

    # Ensure extensions file always contains at least the Timescale statement.
    extensions = outputs.setdefault("00_extensions.sql", [])
    if not any(EXTENSION_LINE in segment for segment in extensions):
        extensions.insert(0, f"{EXTENSION_LINE}\n")

    validate_required_types(outputs)

    write_outputs(outputs, table_preamble)

    if skipped:
        skipped_labels = ", ".join(f"{t}:{n}" for n, t, _ in skipped)
        print(f"Skipped {len(skipped)} block(s): {skipped_labels}")


if __name__ == "__main__":
    main()
