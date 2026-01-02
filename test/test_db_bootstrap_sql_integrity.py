from __future__ import annotations

from pathlib import Path
import re


SCHEMA_DUMP_PATH = Path("SQL/oraculo_schema_only.sql")
BOOTSTRAP_DIR = Path("SQL/bootstrap")
CORE_BOOTSTRAP_FILES = [
    BOOTSTRAP_DIR / "10_core_schema.sql",
    BOOTSTRAP_DIR / "20_core_tables.sql",
    BOOTSTRAP_DIR / "30_core_functions.sql",
    BOOTSTRAP_DIR / "31_core_views.sql",
]
TIMESCALE_BOOTSTRAP = BOOTSTRAP_DIR / "40_timescale.sql"


def test_schema_dump_is_schema_only_and_portable() -> None:
    assert SCHEMA_DUMP_PATH.exists(), f"Schema-only dump is missing: {SCHEMA_DUMP_PATH}"
    sql = SCHEMA_DUMP_PATH.read_text(encoding="utf-8")

    violations = []

    schema_only_patterns = {
        r"^\s*COPY\s": "Found COPY statement (dump must be schema-only)",
        r"Data for Name:": "Found data section header (dump must be schema-only)",
        r"^\s*GRANT\s": "Found GRANT statement (omit ownership/privileges)",
        r"^\s*REVOKE\s": "Found REVOKE statement (omit ownership/privileges)",
        r"OWNER TO": "Found owner statement (omit ownership/privileges)",
    }

    for pattern, message in schema_only_patterns.items():
        flags = re.MULTILINE
        if pattern == r"OWNER TO":
            flags |= re.IGNORECASE

        if re.search(pattern, sql, flags):
            violations.append(f"{message}: pattern `{pattern}` matched")

    assert not violations, "Unexpected content in schema-only dump:\n" + "\n".join(violations)


def test_bootstrap_files_exist() -> None:
    expected_files = [
        BOOTSTRAP_DIR / "00_extensions.sql",
        *CORE_BOOTSTRAP_FILES,
        TIMESCALE_BOOTSTRAP,
    ]

    missing = [str(path) for path in expected_files if not path.exists()]

    assert not missing, "Missing bootstrap files:\n" + "\n".join(missing)


def test_core_bootstrap_files_do_not_reference_internal_timescaledb_schema() -> None:
    offending = []
    for path in CORE_BOOTSTRAP_FILES:
        if "_timescaledb_" in path.read_text(encoding="utf-8"):
            offending.append(str(path))

    assert not offending, (
        "Core bootstrap files must not reference _timescaledb_ internals:\n"
        + "\n".join(offending)
    )


def test_timescale_bootstrap_contains_hypertable_creation_and_usage() -> None:
    assert TIMESCALE_BOOTSTRAP.exists(), f"Timescale bootstrap file is missing: {TIMESCALE_BOOTSTRAP}"
    sql = TIMESCALE_BOOTSTRAP.read_text(encoding="utf-8")

    assert "create_hypertable(" in sql, "Timescale bootstrap must create hypertable(s)"

    timescaledb_tokens = ["timescaledb", "timescaledb.compress", "continuous"]
    assert any(token in sql for token in timescaledb_tokens), (
        "Timescale bootstrap must reference Timescale features to be meaningful:\n"
        + ", ".join(timescaledb_tokens)
    )


def test_timescale_bootstrap_excludes_internal_relations() -> None:
    assert TIMESCALE_BOOTSTRAP.exists(), f"Timescale bootstrap file is missing: {TIMESCALE_BOOTSTRAP}"
    sql = TIMESCALE_BOOTSTRAP.read_text(encoding="utf-8")

    assert 'create_hypertable(\'"_timescaledb_' not in sql, (
        "Timescale bootstrap must not attempt to hypertablize internal Timescale relations"
    )

    materialized_pattern = r"create_hypertable\([^;]*_materialized_hypertable_"
    assert not re.search(materialized_pattern, sql, re.IGNORECASE | re.DOTALL), (
        "Timescale bootstrap must not target materialized hypertables"
    )

    internal_schema_pattern = r"create_hypertable\([^;]*\"?_timescaledb_"
    assert not re.search(internal_schema_pattern, sql, re.IGNORECASE | re.DOTALL), (
        "Timescale bootstrap must not target _timescaledb_* schemas"
    )


def test_timescale_compression_settings_require_enablement() -> None:
    assert TIMESCALE_BOOTSTRAP.exists(), f"Timescale bootstrap file is missing: {TIMESCALE_BOOTSTRAP}"
    sql = TIMESCALE_BOOTSTRAP.read_text(encoding="utf-8")

    alter_statements = re.finditer(r"ALTER\s+TABLE\b.*?;", sql, re.IGNORECASE | re.DOTALL)
    violations = []
    for statement_match in alter_statements:
        statement = statement_match.group(0)
        normalized = statement.lower()
        has_segment_or_order = (
            "timescaledb.compress_segmentby" in normalized
            or "timescaledb.compress_orderby" in normalized
        )
        if has_segment_or_order and "timescaledb.compress = true" not in normalized:
            violations.append(statement.strip())

    assert not violations, (
        "Compression settings must be paired with timescaledb.compress = true "
        "in the same ALTER TABLE statement:\n" + "\n\n".join(violations)
    )
