from __future__ import annotations

import asyncio
import json
import os
import numbers
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

import asyncpg

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = PROJECT_ROOT / "SQL" / "bootstrap" / "40_timescale.sql"
ALLOWED_HYPERTABLE_SCHEMAS = {
    "public",
    "oraculo",
    "oraculo_bt",
    "oraculo_audit",
    "binance_futures",
    "binance_spot",
    "deribit",
}
DISALLOWED_HYPERTABLE_SCHEMAS = {
    "timescaledb_information",
    "timescaledb_experimental",
    "_timescaledb_functions",
}
INTERNAL_SCHEMA_PREFIX = "_timescaledb_"
MATERIALIZED_HYPERTABLE_PREFIX = "_materialized_hypertable_"


@dataclass
class PartitionDimension:
    column_name: str
    partitions: Optional[int]


@dataclass
class Hypertable:
    schema: str
    name: str
    time_column: str
    chunk_interval: object
    space_partitions: List[PartitionDimension] = field(default_factory=list)


@dataclass
class CompressionSettings:
    enabled: bool
    segment_by: List[str] = field(default_factory=list)
    order_by: List[str] = field(default_factory=list)


@dataclass
class CompressionPolicy:
    schema: str
    name: str
    compress_after: object


@dataclass
class RetentionPolicy:
    schema: str
    name: str
    drop_after: object


@dataclass
class ContinuousAggregate:
    schema: str
    name: str
    materialization_schema: Optional[str]
    materialization_name: Optional[str]
    definition: str


@dataclass
class RefreshPolicy:
    schema: str
    name: str
    start_offset: object
    end_offset: object
    schedule_interval: object


async def fetch_table_columns(conn: asyncpg.Connection, schema: str, table: str) -> List[str]:
    rows = await conn.fetch(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        """,
        schema,
        table,
    )
    return [row["column_name"] for row in rows]


def quote_ident(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def qualify_name(schema: str, name: str) -> str:
    return f"{quote_ident(schema)}.{quote_ident(name)}"


def literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def format_timedelta(value: timedelta) -> str:
    total_seconds = int(value.total_seconds())
    microseconds = value.microseconds

    days, remainder = divmod(total_seconds, 86_400)
    hours, remainder = divmod(remainder, 3_600)
    minutes, seconds = divmod(remainder, 60)

    day_part = f"{days} day{'s' if days != 1 else ''} " if days else ""
    time_part = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    if microseconds:
        time_part += f".{microseconds:06d}"

    return f"{day_part}{time_part}".strip()


def format_interval_literal(value: object) -> str:
    if value is None:
        raise ValueError("Interval-like value is required")

    if isinstance(value, bool):
        raise ValueError("Boolean values are not valid interval specifications")

    if isinstance(value, timedelta):
        return f"INTERVAL {literal(format_timedelta(value))}"

    if isinstance(value, dict):
        if "time" in value and "unit" in value:
            inner = f"{value['time']} {value['unit']}"
            return f"INTERVAL {literal(inner)}"
        if "value" in value:
            return format_interval_literal(value["value"])

    if isinstance(value, numbers.Real):
        numeric_value = float(value)
        if numeric_value.is_integer():
            return str(int(numeric_value))
        return str(numeric_value)

    text = str(value)
    try:
        numeric = float(text)
    except ValueError:
        return f"INTERVAL {literal(text)}"

    if numeric.is_integer():
        return str(int(numeric))
    return text


def is_allowed_hypertable_target(schema: str, name: str) -> bool:
    if schema.startswith(INTERNAL_SCHEMA_PREFIX):
        return False
    if schema in DISALLOWED_HYPERTABLE_SCHEMAS:
        return False
    if name.startswith(MATERIALIZED_HYPERTABLE_PREFIX):
        return False
    if schema not in ALLOWED_HYPERTABLE_SCHEMAS:
        return False
    return True


async def fetch_hypertables(
    conn: asyncpg.Connection, dimension_columns: Sequence[str], hypertable_columns: Sequence[str]
) -> List[Hypertable]:
    select_dimension_fields = ["hypertable_schema", "hypertable_name", "column_name"]
    if "interval_length" in dimension_columns:
        select_dimension_fields.append("interval_length")
    if "num_partitions" in dimension_columns:
        select_dimension_fields.append("num_partitions")
    if "number_partitions" in dimension_columns:
        select_dimension_fields.append("number_partitions")
    if "is_time_dimension" in dimension_columns:
        select_dimension_fields.append("is_time_dimension")
    if "dimension_type" in dimension_columns:
        select_dimension_fields.append("dimension_type")
    if "dimension_number" in dimension_columns:
        select_dimension_fields.append("dimension_number")

    dimensions = await conn.fetch(
        f"SELECT {', '.join(select_dimension_fields)} FROM timescaledb_information.dimensions"
    )

    chunk_interval_column = "chunk_time_interval" if "chunk_time_interval" in hypertable_columns else None
    hypertables_raw: Dict[tuple[str, str], object] = {}

    if chunk_interval_column:
        rows = await conn.fetch(
            f"""
            SELECT hypertable_schema, hypertable_name, {chunk_interval_column} AS chunk_time_interval
            FROM timescaledb_information.hypertables
            """
        )
        for row in rows:
            key = (row["hypertable_schema"], row["hypertable_name"])
            if not is_allowed_hypertable_target(*key):
                continue
            hypertables_raw[key] = row["chunk_time_interval"]

    hypertables: List[Hypertable] = []
    for row in dimensions:
        is_time_dimension = False
        if "is_time_dimension" in row:
            is_time_dimension = bool(row["is_time_dimension"])
        elif "dimension_type" in row:
            is_time_dimension = row["dimension_type"] == "time"
        elif "dimension_number" in row:
            is_time_dimension = row["dimension_number"] == 0

        key = (row["hypertable_schema"], row["hypertable_name"])
        if not is_allowed_hypertable_target(*key):
            continue

        if is_time_dimension:
            interval_value = row.get("interval_length")
            if interval_value is None:
                interval_value = hypertables_raw.get(key)

            hypertables.append(
                Hypertable(
                    schema=row["hypertable_schema"],
                    name=row["hypertable_name"],
                    time_column=row["column_name"],
                    chunk_interval=interval_value,
                )
            )

    partitions_map: Dict[tuple[str, str], List[PartitionDimension]] = {}
    for row in dimensions:
        is_time_dimension = False
        if "is_time_dimension" in row:
            is_time_dimension = bool(row["is_time_dimension"])
        elif "dimension_type" in row:
            is_time_dimension = row["dimension_type"] == "time"
        elif "dimension_number" in row:
            is_time_dimension = row["dimension_number"] == 0

        if is_time_dimension:
            continue

        key = (row["hypertable_schema"], row["hypertable_name"])
        if not is_allowed_hypertable_target(*key):
            continue

        partitions = row.get("num_partitions") or row.get("number_partitions")
        partitions_map.setdefault(key, []).append(
            PartitionDimension(column_name=row["column_name"], partitions=partitions)
        )

    for hypertable in hypertables:
        hypertable.space_partitions.extend(
            partitions_map.get((hypertable.schema, hypertable.name), [])
        )

    return hypertables


async def fetch_compression_settings(
    conn: asyncpg.Connection, hypertable_columns: Sequence[str]
) -> Dict[tuple[str, str], CompressionSettings]:
    compression_states: Dict[tuple[str, str], str] = {}
    compression_settings: Dict[tuple[str, str], CompressionSettings] = {}

    if "compression_state" in hypertable_columns:
        rows = await conn.fetch(
            """
            SELECT hypertable_schema, hypertable_name, compression_state
            FROM timescaledb_information.hypertables
            """
        )
        for row in rows:
            key = (row["hypertable_schema"], row["hypertable_name"])
            if not is_allowed_hypertable_target(*key):
                continue
            compression_states[(row["hypertable_schema"], row["hypertable_name"])] = row["compression_state"]

    compression_settings_available = bool(
        await conn.fetchval(
            """
            SELECT to_regclass('timescaledb_information.compression_settings') IS NOT NULL
            """
        )
    )

    if compression_settings_available:
        rows = await conn.fetch(
            """
            SELECT
                hypertable_schema,
                hypertable_name,
                attname,
                segmentby_column_index,
                orderby_column_index,
                orderby_asc,
                orderby_nullsfirst
            FROM timescaledb_information.compression_settings
            """
        )

        grouped: Dict[tuple[str, str], CompressionSettings] = {}
        for row in rows:
            key = (row["hypertable_schema"], row["hypertable_name"])
            if not is_allowed_hypertable_target(*key):
                continue
            settings = grouped.setdefault(
                key, CompressionSettings(enabled=False, segment_by=[], order_by=[])
            )
            segment_idx = row.get("segmentby_column_index")
            order_idx = row.get("orderby_column_index")

            if segment_idx is not None:
                while len(settings.segment_by) <= segment_idx:
                    settings.segment_by.append("")
                settings.segment_by[segment_idx] = row["attname"]

            if order_idx is not None:
                while len(settings.order_by) <= order_idx:
                    settings.order_by.append("")
                direction = "ASC" if row.get("orderby_asc", True) else "DESC"
                nulls = " NULLS FIRST" if row.get("orderby_nullsfirst") else ""
                settings.order_by[order_idx] = f"{row['attname']} {direction}{nulls}".strip()

        for key, settings in grouped.items():
            state = compression_states.get(key)
            settings.enabled = True if state is None else bool(state) and state.lower().startswith("enabled")
            compression_settings[key] = settings

    for key, state in compression_states.items():
        compression_settings.setdefault(
            key, CompressionSettings(enabled=state.lower().startswith("enabled"), segment_by=[], order_by=[])
        )

    return compression_settings


async def fetch_jobs(conn: asyncpg.Connection) -> List[asyncpg.Record]:
    exists = await conn.fetchval(
        "SELECT to_regclass('timescaledb_information.jobs') IS NOT NULL"
    )
    if not exists:
        return []

    rows = await conn.fetch(
        """
        SELECT *
        FROM timescaledb_information.jobs
        """
    )
    return rows


async def fetch_compression_policies(jobs: Iterable[asyncpg.Record]) -> List[CompressionPolicy]:
    policies: List[CompressionPolicy] = []
    for job in jobs:
        if job.get("proc_name") != "policy_compression":
            continue

        config = normalize_job_config(job.get("config"))
        hypertable_schema = job.get("hypertable_schema")
        hypertable_name = job.get("hypertable_name")
        if not hypertable_schema or not hypertable_name:
            continue
        if not is_allowed_hypertable_target(hypertable_schema, hypertable_name):
            continue
        compress_after = config.get("compress_after")
        if hypertable_schema and hypertable_name and compress_after is not None:
            policies.append(
                CompressionPolicy(
                    schema=hypertable_schema,
                    name=hypertable_name,
                    compress_after=compress_after,
                )
            )

    return policies


async def fetch_retention_policies(
    jobs: Iterable[asyncpg.Record],
    cagg_materialization_lookup: Dict[tuple[str, str], tuple[str, str]],
) -> List[RetentionPolicy]:
    policies: List[RetentionPolicy] = []

    for job in jobs:
        if job.get("proc_name") != "policy_retention":
            continue

        config = normalize_job_config(job.get("config"))
        drop_after = config.get("drop_after")
        schema = job.get("hypertable_schema") or job.get("table_schema")
        name = job.get("hypertable_name") or job.get("table_name")

        if (schema, name) in cagg_materialization_lookup:
            schema, name = cagg_materialization_lookup[(schema, name)]

        if schema and name and drop_after is not None:
            policies.append(RetentionPolicy(schema=schema, name=name, drop_after=drop_after))

    return policies


async def fetch_caggs(conn: asyncpg.Connection) -> List[ContinuousAggregate]:
    has_view = bool(await conn.fetchval("SELECT to_regclass('timescaledb_information.continuous_aggregates') IS NOT NULL"))
    if not has_view:
        return []

    rows = await conn.fetch(
        """
        SELECT view_schema, view_name, materialization_hypertable_schema, materialization_hypertable_name
        FROM timescaledb_information.continuous_aggregates
        """
    )

    caggs: List[ContinuousAggregate] = []
    for row in rows:
        definition = await conn.fetchval(
            "SELECT pg_get_viewdef(format('%I.%I', $1::text, $2::text)::regclass, true)",
            row["view_schema"],
            row["view_name"],
        )
        caggs.append(
            ContinuousAggregate(
                schema=row["view_schema"],
                name=row["view_name"],
                materialization_schema=row.get("materialization_hypertable_schema"),
                materialization_name=row.get("materialization_hypertable_name"),
                definition=definition,
            )
        )

    return caggs


async def fetch_refresh_policies(
    jobs: Iterable[asyncpg.Record],
    cagg_materialization_lookup: Dict[tuple[str, str], tuple[str, str]],
) -> List[RefreshPolicy]:
    policies: List[RefreshPolicy] = []

    for job in jobs:
        if job.get("proc_name") != "policy_refresh_continuous_aggregate":
            continue

        config = normalize_job_config(job.get("config"))
        schema = job.get("hypertable_schema") or job.get("table_schema")
        name = job.get("hypertable_name") or job.get("table_name")

        if (schema, name) in cagg_materialization_lookup:
            schema, name = cagg_materialization_lookup[(schema, name)]

        start_offset = config.get("start_offset")
        end_offset = config.get("end_offset")
        schedule_interval = config.get("schedule_interval") or job.get("schedule_interval")

        if schema and name and None not in (start_offset, end_offset, schedule_interval):
            policies.append(
                RefreshPolicy(
                    schema=schema,
                    name=name,
                    start_offset=start_offset,
                    end_offset=end_offset,
                    schedule_interval=schedule_interval,
                )
            )

    return policies


def build_policy_guard(proc_name: str, schema: str, name: str, job_columns: Sequence[str]) -> str:
    schema_literal = literal(schema)
    name_literal = literal(name)
    predicates: List[str] = []

    if "hypertable_schema" in job_columns and "hypertable_name" in job_columns:
        predicates.append(
            f"(hypertable_schema = {schema_literal} AND hypertable_name = {name_literal})"
        )
    if "table_schema" in job_columns and "table_name" in job_columns:
        predicates.append(f"(table_schema = {schema_literal} AND table_name = {name_literal})")

    predicate_sql = f" AND ({' OR '.join(predicates)})" if predicates else ""
    return (
        "EXISTS ("
        "SELECT 1 FROM timescaledb_information.jobs "
        f"WHERE proc_name = {literal(proc_name)}{predicate_sql}"
        ")"
    )


def normalize_job_config(config: object) -> dict:
    if isinstance(config, dict):
        return config
    if isinstance(config, str):
        try:
            parsed = json.loads(config)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def render_hypertables(hypertables: Iterable[Hypertable]) -> List[str]:
    statements: List[str] = []

    for hypertable in sorted(hypertables, key=lambda h: (h.schema, h.name)):
        chunk_interval_literal = format_interval_literal(hypertable.chunk_interval)
        qualified_table = qualify_name(hypertable.schema, hypertable.name)

        partition_args = ""
        if hypertable.space_partitions:
            first_partition = hypertable.space_partitions[0]
            partition_args = (
                f", partitioning_column => {literal(first_partition.column_name)}, "
                f"number_partitions => {first_partition.partitions or 1}"
            )

        statement = (
            "DO $$\nBEGIN\n"
            "    IF NOT EXISTS (\n"
            "        SELECT 1\n"
            "        FROM timescaledb_information.hypertables\n"
            f"        WHERE hypertable_schema = {literal(hypertable.schema)}\n"
            f"          AND hypertable_name = {literal(hypertable.name)}\n"
            "    ) THEN\n"
            f"        PERFORM create_hypertable('{qualified_table}', {literal(hypertable.time_column)}, "
            f"chunk_time_interval => {chunk_interval_literal}{partition_args}, if_not_exists => TRUE);\n"
            "    END IF;\n"
            "END\n$$;"
        )
        statements.append(statement)

        if len(hypertable.space_partitions) > 1:
            for partition in hypertable.space_partitions[1:]:
                statements.append(
                    "SELECT add_dimension("
                    f"'{qualified_table}', {literal(partition.column_name)}, number_partitions => {partition.partitions or 1});"
                )

    return statements


def render_compression_settings(
    settings_map: Dict[tuple[str, str], CompressionSettings],
    compression_policy_targets: Iterable[tuple[str, str]],
) -> List[str]:
    statements: List[str] = []
    targets: set[tuple[str, str]] = set()

    for key, settings in settings_map.items():
        if settings.enabled or settings.segment_by or settings.order_by:
            if is_allowed_hypertable_target(*key):
                targets.add(key)

    for policy_target in compression_policy_targets:
        if is_allowed_hypertable_target(*policy_target):
            targets.add(policy_target)

    for schema, name in sorted(targets):
        settings = settings_map.get((schema, name), CompressionSettings(enabled=False, segment_by=[], order_by=[]))
        qualified = qualify_name(schema, name)

        option_parts = ["timescaledb.compress = true"]

        segment_val = ", ".join(quote_ident(col) for col in settings.segment_by if col)
        if segment_val:
            option_parts.append(f"timescaledb.compress_segmentby = {literal(segment_val)}")

        order_val = ", ".join(settings.order_by)
        if order_val:
            option_parts.append(f"timescaledb.compress_orderby = {literal(order_val)}")

        options_sql = ",\n        ".join(option_parts)
        statements.append(
            f"ALTER TABLE {qualified}\n"
            "    SET (\n"
            f"        {options_sql}\n"
            "    );"
        )

    return statements


def render_compression_policies(
    policies: Iterable[CompressionPolicy], job_columns: Sequence[str]
) -> List[str]:
    statements: List[str] = []
    for policy in sorted(policies, key=lambda p: (p.schema, p.name)):
        condition = build_policy_guard("policy_compression", policy.schema, policy.name, job_columns)
        statements.append(
            "DO $$\nBEGIN\n"
            f"    IF NOT {condition} THEN\n"
            f"        PERFORM add_compression_policy('{qualify_name(policy.schema, policy.name)}', {format_interval_literal(policy.compress_after)});\n"
            "    END IF;\n"
            "END\n$$;"
        )

    return statements


def render_retention_policies(
    policies: Iterable[RetentionPolicy], job_columns: Sequence[str]
) -> List[str]:
    statements: List[str] = []
    for policy in sorted(policies, key=lambda p: (p.schema, p.name)):
        condition = build_policy_guard("policy_retention", policy.schema, policy.name, job_columns)
        statements.append(
            "DO $$\nBEGIN\n"
            f"    IF NOT {condition} THEN\n"
            f"        PERFORM add_retention_policy('{qualify_name(policy.schema, policy.name)}', {format_interval_literal(policy.drop_after)});\n"
            "    END IF;\n"
            "END\n$$;"
        )

    return statements


def render_caggs(caggs: Iterable[ContinuousAggregate]) -> List[str]:
    statements: List[str] = []
    for cagg in sorted(caggs, key=lambda c: (c.schema, c.name)):
        qualified = qualify_name(cagg.schema, cagg.name)
        view_body = cagg.definition.rstrip().rstrip(";")
        statements.append(
            "DO $$\nBEGIN\n"
            f"    IF to_regclass('{qualified}') IS NULL THEN\n"
            f"        CREATE MATERIALIZED VIEW {qualified}\n"
            "            WITH (timescaledb.continuous) AS\n"
            f"{view_body}\n"
            "        WITH NO DATA;\n"
            "    END IF;\n"
            "END\n$$;"
        )

    return statements


def render_refresh_policies(
    policies: Iterable[RefreshPolicy], job_columns: Sequence[str]
) -> List[str]:
    statements: List[str] = []
    for policy in sorted(policies, key=lambda p: (p.schema, p.name)):
        condition = build_policy_guard(
            "policy_refresh_continuous_aggregate", policy.schema, policy.name, job_columns
        )
        statements.append(
            "DO $$\nBEGIN\n"
            f"    IF NOT {condition} THEN\n"
            f"        PERFORM add_continuous_aggregate_policy('{qualify_name(policy.schema, policy.name)}', "
            f"start_offset => {format_interval_literal(policy.start_offset)}, "
            f"end_offset => {format_interval_literal(policy.end_offset)}, "
            f"schedule_interval => {format_interval_literal(policy.schedule_interval)});\n"
            "    END IF;\n"
            "END\n$$;"
        )

    return statements


def ensure_no_internal_names(content: str) -> None:
    patterns = [
        r"create_hypertable\([^;]*_timescaledb_internal",
        r"create_hypertable\([^;]*_materialized_hypertable_",
        r"ALTER\s+TABLE\s+\"?_timescaledb_",
    ]

    for pattern in patterns:
        if re.search(pattern, content, re.IGNORECASE | re.DOTALL):
            raise ValueError(f"Generated SQL contains forbidden target pattern: {pattern}")


async def generate() -> str:
    dsn = os.environ.get("PG_DSN")
    if not dsn:
        raise RuntimeError("PG_DSN environment variable is required")

    conn = await asyncpg.connect(dsn)
    try:
        dimension_columns = await fetch_table_columns(conn, "timescaledb_information", "dimensions")
        hypertable_columns = await fetch_table_columns(conn, "timescaledb_information", "hypertables")
        job_columns = await fetch_table_columns(conn, "timescaledb_information", "jobs")

        hypertables = await fetch_hypertables(conn, dimension_columns, hypertable_columns)
        compression_settings = await fetch_compression_settings(conn, hypertable_columns)
        jobs = await fetch_jobs(conn)
        caggs = await fetch_caggs(conn)

        materialization_lookup: Dict[tuple[str, str], tuple[str, str]] = {}
        for cagg in caggs:
            if cagg.materialization_schema and cagg.materialization_name:
                materialization_lookup[(cagg.materialization_schema, cagg.materialization_name)] = (
                    cagg.schema,
                    cagg.name,
                )

        compression_policies = await fetch_compression_policies(jobs)
        retention_policies = await fetch_retention_policies(jobs, materialization_lookup)
        refresh_policies = await fetch_refresh_policies(jobs, materialization_lookup)

    finally:
        await conn.close()

    segments: List[str] = []
    segments.append("-- Generated by tools/export_timescale_layer.py")
    segments.append("CREATE EXTENSION IF NOT EXISTS timescaledb;")

    compression_policy_targets = {(policy.schema, policy.name) for policy in compression_policies}

    segments.extend(render_hypertables(hypertables))
    segments.extend(render_compression_settings(compression_settings, compression_policy_targets))
    segments.extend(render_compression_policies(compression_policies, job_columns))
    segments.extend(render_caggs(caggs))
    segments.extend(render_refresh_policies(refresh_policies, job_columns))
    segments.extend(render_retention_policies(retention_policies, job_columns))

    content = "\n\n".join(segment for segment in segments if segment.strip()) + "\n"
    ensure_no_internal_names(content)
    return content


def write_output(content: str) -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(content, encoding="utf-8")


def main() -> None:
    content = asyncio.run(generate())
    write_output(content)


if __name__ == "__main__":
    main()
