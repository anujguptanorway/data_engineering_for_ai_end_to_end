"""Explore the MinIO-hosted Parquet tables with DuckDB, without spinning up Spark.

Usage:
    uv run python -m common.duckdb_inspect          # print schema + sample rows per table
    uv run python -m common.duckdb_inspect --ui     # open the DuckDB web UI at http://localhost:4213
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from urllib.parse import urlparse

import duckdb
from dotenv import load_dotenv

from common.spark import get_required_env

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def connect() -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection configured to read Parquet data from MinIO over S3."""
    load_dotenv(PROJECT_ROOT / ".env", override=True)
    access_key = get_required_env("MINIO_ACCESS_KEY")
    secret_key = get_required_env("MINIO_SECRET_KEY")
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
    parsed_endpoint = urlparse(endpoint)

    con = duckdb.connect()
    con.sql("INSTALL httpfs; LOAD httpfs;")
    # A SECRET (vs. SET s3_*) is stored instance-wide, so the UI's query sessions see it too.
    con.sql(f"""
        CREATE OR REPLACE SECRET minio (
            TYPE s3,
            KEY_ID '{access_key}',
            SECRET '{secret_key}',
            ENDPOINT '{parsed_endpoint.netloc}',
            URL_STYLE 'path',
            USE_SSL {'true' if parsed_endpoint.scheme == 'https' else 'false'}
        );
    """)
    return con


def discover_tables(con: duckdb.DuckDBPyConnection, bucket: str) -> dict[str, str]:
    """Map "schema.table" (mirroring the bronze/silver/gold folder layout) -> s3 glob path.

    Collapses Hive-partition dirs (e.g. region=East) into the parent table's glob.
    """
    files = con.sql(f"SELECT file FROM glob('s3://{bucket}/**/*.parquet')").fetchall()
    table_roots: set[str] = set()
    for (file_path,) in files:
        relative_path = file_path.removeprefix(f"s3://{bucket}/")
        parts = relative_path.split("/")[:-1]
        table_parts = []
        for part in parts:
            if "=" in part:
                break
            table_parts.append(part)
        if table_parts:
            table_roots.add("/".join(table_parts))

    tables = {}
    for root in sorted(table_roots):
        schema, _, rest = root.partition("/")
        table_name = rest.replace("/", "_") if rest else schema
        qualified_name = f"{schema}.{table_name}" if rest else table_name
        tables[qualified_name] = f"s3://{bucket}/{root}/**/*.parquet"
    return tables


def create_views(con: duckdb.DuckDBPyConnection, tables: dict[str, str]) -> None:
    """Register each discovered table as a queryable DuckDB view under its own schema."""
    schemas = {table_name.split(".", 1)[0] for table_name in tables if "." in table_name}
    for schema in schemas:
        con.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    for table_name, glob_path in tables.items():
        con.sql(
            f"CREATE OR REPLACE VIEW {table_name} AS "
            f"SELECT * FROM read_parquet('{glob_path}', hive_partitioning=true)"
        )


def print_summary(con: duckdb.DuckDBPyConnection, tables: dict[str, str]) -> None:
    for table_name, glob_path in tables.items():
        print(f"=== {table_name} ===")
        row_count = con.sql(
            f"SELECT count(*) FROM read_parquet('{glob_path}', hive_partitioning=true)"
        ).fetchone()[0]
        print(f"rows: {row_count}")
        con.sql(f"SELECT * FROM read_parquet('{glob_path}', hive_partitioning=true) LIMIT 0").show()
        con.sql(f"SELECT * FROM read_parquet('{glob_path}', hive_partitioning=true) LIMIT 5").show()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ui", action="store_true", help="Open the DuckDB web UI instead of printing")
    args = parser.parse_args()

    bucket = os.environ.get("MINIO_BUCKET", "data-bucket")
    con = connect()
    tables = discover_tables(con, bucket)
    if not tables:
        print(f"No Parquet tables found under s3://{bucket}/")
        return

    create_views(con, tables)

    if args.ui:
        con.sql("INSTALL ui; LOAD ui;")
        con.sql("CALL start_ui();")
        print(f"DuckDB UI running at http://localhost:4213 with views: {', '.join(tables)}")
        input("Press Enter to stop the UI server...\n")
    else:
        print_summary(con, tables)


if __name__ == "__main__":
    main()
