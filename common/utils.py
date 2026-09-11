from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

logger = logging.getLogger(__name__)


def load_dataframe(
    spark: SparkSession,
    input_path: str,
    input_format: str = "csv",
    csv_options: Optional[dict] = None,
) -> DataFrame:
    """Load the source CSV/Parquet data as a Spark DataFrame."""
    is_remote_path = "://" in input_path
    if not is_remote_path:
        source_path = Path(input_path)
        if not source_path.exists():
            raise FileNotFoundError(f"Input not found: {source_path.resolve()}")

    options = csv_options or {"header": "true", "inferSchema": "true"}

    reader = spark.read.options(**options) if input_format == "csv" else spark.read
    return reader.format(input_format).load(input_path)

def store_df_to_table(spark,
    df: DataFrame,
    table_name: str,
    column_config: dict,
    create_table_sql: str,
    write_mode: str = "overwrite",
) -> None:
    """
    Create a Spark table from the supplied SQL and store the DataFrame
    using the table's actual column order and the types defined in
    column_config.

    Parameters
    ----------
    df : DataFrame
        Source DataFrame.

    table_name : str
        Fully qualified target table name, e.g.
        'my_database.silver_customer_data_v2'.

    column_config : dict
        Configuration containing source column, target type, and description.

    create_table_sql : str
        CREATE TABLE SQL generated from the configuration.

    write_mode : str
        Spark write mode. Defaults to 'overwrite'.
    """

    # Create database
    database_name = table_name.rsplit(".", 1)[0]

    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {database_name}"
    )

    # Create table
    spark.sql(create_table_sql)

    # Get the actual column order from the target table.
    table_columns = spark.table(table_name).columns

    # Validate that every table column has configuration.
    missing_config = [
        column
        for column in table_columns
        if column not in column_config
    ]

    if missing_config:
        raise ValueError(
            f"Missing column configuration for: {missing_config}"
        )

    # Validate that every configured source column exists in the DataFrame.
    missing_source_columns = [
        column_config[column]["source"]
        for column in table_columns
        if column_config[column]["source"] not in df.columns
    ]

    if missing_source_columns:
        raise ValueError(
            f"Source columns not found in DataFrame: "
            f"{missing_source_columns}"
        )

    # Map source columns to target names and types.
    # The order follows the actual target table.
    table_df = df.select(
        *[
            F.col(column_config[column]["source"])
            .cast(column_config[column].get("type", "STRING"))
            .alias(column)
            for column in table_columns
        ]
    )

    # Write to the existing table.
    table_df.write \
        .mode(write_mode) \
        .insertInto(table_name)

def build_create_table_sql(
    table_name: str,
    column_config: dict,
    location: str,
    table_format: str = "PARQUET",
    partition_columns: list[str] | None = None,
    table_comment: str | None = None,
) -> str:
    partition_columns = partition_columns or []
    columns_sql = []

    for column_name, config in column_config.items():
        data_type = config.get("type", "STRING")
        description = config.get("description")

        column_sql = f"`{column_name}` {data_type}"
        if description:
            # Spark SQL string literals escape quotes with a backslash, not by doubling them.
            safe_description = description.replace("'", "\\'")
            column_sql += f" COMMENT '{safe_description}'"

        columns_sql.append(column_sql)

    partition_sql = (
        f"\nPARTITIONED BY ({', '.join(f'`{c}`' for c in partition_columns)})"
        if partition_columns
        else ""
    )
    safe_table_comment = table_comment.replace("'", "\\'") if table_comment else ""
    comment_sql = f"\nCOMMENT '{safe_table_comment}'" if table_comment else ""

    return f"""
CREATE TABLE IF NOT EXISTS {table_name}(
    {', '.join(columns_sql)}
)
USING {table_format}{partition_sql}
LOCATION '{location}'{comment_sql}
""".strip()


def validate_required_columns(df: DataFrame, required_columns: Iterable[str]) -> None:
    """Raise if any required column is missing from the source dataframe."""
    normalized_required_columns = set(required_columns)
    missing_columns = sorted(normalized_required_columns - set(df.columns))
    if missing_columns:
        raise ValueError(f"Missing required source columns: {missing_columns}")
    logger.info("All required columns are present: %s", sorted(normalized_required_columns))


def apply_expected_casts(df: DataFrame, expected_types: Optional[Dict[str, str]] = None) -> DataFrame:
    """Cast chosen columns to the requested Spark SQL types."""
    if expected_types is None:
        return df
    logger.info("Applying expected casts: %s", expected_types)
    typed_df = df
    for column_name, target_type in expected_types.items():
        if column_name in typed_df.columns:
            typed_df = typed_df.withColumn(column_name, F.col(column_name).cast(target_type))
    return typed_df


def clean_dataframe(
    df: DataFrame,
    source_key_columns: Optional[List[str]] = None,
    required_columns: Optional[List[str]] = None,
    expected_types: Optional[Dict[str, str]] = None,
) -> DataFrame:
    """Trim string values, remove null rows, and drop duplicate rows."""
    if required_columns:
        validate_required_columns(df, required_columns)

    typed_df = apply_expected_casts(df, expected_types)
    string_columns = [
        field.name for field in typed_df.schema.fields if isinstance(field.dataType, StringType)
    ]

    cleaned_df = typed_df
    for column_name in string_columns:
        cleaned_df = cleaned_df.withColumn(
            column_name,
            F.when(F.trim(F.col(column_name)) == "", F.lit(None)).otherwise(F.trim(F.col(column_name))),
        )

    non_null_expression = [F.col(column_name).isNotNull() for column_name in cleaned_df.columns]
    if non_null_expression:
        cleaned_df = cleaned_df.filter(F.coalesce(*non_null_expression))

    dedup_columns = source_key_columns or cleaned_df.columns
    return cleaned_df.dropDuplicates(dedup_columns)


def compute_quality_report(rows_before: int, rows_after: int, duplicates_removed: int = 0) -> dict:
    """Create a standard quality report dictionary for pytest assertions and notebook output."""
    removed_rows = max(rows_before - rows_after, 0)
    if duplicates_removed:
        removed_rows = max(removed_rows, duplicates_removed)
    return {
        "rows_before": rows_before,
        "rows_after": rows_after,
        "duplicates_or_empty_rows_removed": removed_rows,
    }


def get_null_counts(df: DataFrame) -> DataFrame:
    """Return a dataframe with null counts per column."""
    null_count_expressions = [
        F.sum(F.col(column_name).isNull().cast("long")).alias(column_name)
        for column_name in df.columns
    ]
    return df.agg(*null_count_expressions) if null_count_expressions else df.limit(0)


def write_output(df: DataFrame, output_path: str, mode: str = "overwrite") -> int:
    """Write data to parquet and return row count."""
    df.write.mode(mode).parquet(output_path)
    return df.count()


def build_data_dictionary_entry(
    spark: SparkSession,
    table_name: str,
    column_config: dict,
    table_metadata: Optional[dict] = None,
) -> dict:
    """Describe a catalog table via DESCRIBE EXTENDED and merge in curated metadata.

    column_config values may include: source, type, description, unit, valid_values, valid_range.
    table_metadata keys: comment, grain, primary_key, freshness, caveats.
    """
    table_metadata = table_metadata or {}
    described_rows = spark.sql(f"DESCRIBE EXTENDED {table_name}").collect()

    columns = []
    for row in described_rows:
        column_name = (row["col_name"] or "").strip()
        # DESCRIBE EXTENDED lists a blank row then '# Detailed Table Information' after the columns.
        if not column_name or column_name.startswith("#"):
            break
        extra = column_config.get(column_name, {})
        columns.append(
            {
                "name": column_name,
                "type": row["data_type"],
                "description": extra.get("description", ""),
                "unit": extra.get("unit"),
                "valid_values": extra.get("valid_values"),
                "valid_range": extra.get("valid_range"),
            }
        )

    return {
        "table": table_name,
        "comment": table_metadata.get("comment", ""),
        "grain": table_metadata.get("grain", ""),
        "primary_key": table_metadata.get("primary_key", []),
        "freshness": table_metadata.get("freshness", ""),
        "caveats": table_metadata.get("caveats", []),
        "columns": columns,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def write_data_dictionary(entry: dict, output_dir: Path | str) -> None:
    """Merge a table's data dictionary entry into the JSON manifest and regenerate the Markdown doc."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "data_dictionary.json"
    md_path = output_dir / "data_dictionary.md"

    manifest = json.loads(json_path.read_text()) if json_path.exists() else {}
    manifest[entry["table"]] = entry

    json_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    md_path.write_text(_render_data_dictionary_markdown(manifest))


def _render_data_dictionary_markdown(manifest: dict) -> str:
    """Render the JSON manifest as a human-readable Markdown data dictionary."""
    lines = [
        "# Data Dictionary",
        "",
        "Auto-generated from `DESCRIBE EXTENDED` plus curated metadata. Do not edit by hand —"
        " update the column/table metadata in the transformation module instead.",
        "",
    ]
    for table_name in sorted(manifest):
        entry = manifest[table_name]
        lines.append(f"## {table_name}")
        lines.append("")
        if entry.get("comment"):
            lines.append(entry["comment"])
            lines.append("")
        primary_key = entry.get("primary_key") or []
        lines.append(f"- **Grain:** {entry.get('grain') or '_not set_'}")
        lines.append(f"- **Primary key:** {', '.join(primary_key) if primary_key else '_not set_'}")
        lines.append(f"- **Freshness:** {entry.get('freshness') or '_not set_'}")
        lines.append(f"- **Generated at:** {entry.get('generated_at')}")
        caveats = entry.get("caveats") or []
        if caveats:
            lines.append("- **Caveats:**")
            for caveat in caveats:
                lines.append(f"  - {caveat}")
        lines.append("")
        lines.append("| Column | Type | Description | Unit | Valid values | Valid range |")
        lines.append("| --- | --- | --- | --- | --- | --- |")
        for column in entry.get("columns", []):
            valid_values = column.get("valid_values")
            lines.append(
                f"| `{column['name']}` | {column['type']} | {column.get('description') or ''} "
                f"| {column.get('unit') or ''} | {', '.join(valid_values) if valid_values else ''} "
                f"| {column.get('valid_range') or ''} |"
            )
        lines.append("")
    return "\n".join(lines)


