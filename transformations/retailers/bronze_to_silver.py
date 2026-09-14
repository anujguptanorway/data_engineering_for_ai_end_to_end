from __future__ import annotations

import os

from dotenv import load_dotenv

from common.spark import PROJECT_ROOT, build_spark_session
from common.utils import (
    build_create_table_sql,
    build_data_dictionary_entry,
    compute_quality_report,
    get_null_counts,
    load_dataframe,
    remove_duplicates,
    required_columns,
    store_df_to_table,
    validate_required_columns,
    write_data_dictionary,
)

load_dotenv(PROJECT_ROOT / ".env")  # populates os.environ from .env regardless of how the shell was invoked
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "data-bucket")
DEFAULT_INPUT = f"s3a://{MINIO_BUCKET}/bronze/retailers_input.csv"
DEFAULT_OUTPUT = f"s3a://{MINIO_BUCKET}/silver/retailers"
DOCS_DIR = PROJECT_ROOT / "docs"

SILVER_TABLE_NAME = "silver.retailers"

# Column metadata: "source"/"type"/"is_required" drive the catalog table, the rest feeds
# the data dictionary. required_columns() derives the validation list from "is_required"
# instead of a separate hardcoded list.
SILVER_COLUMN_CONFIG = {
    "retailer_id": {
        "source": "retailer_id",
        "type": "INT",
        "is_required": True,
        "description": "Unique identifier for the retailer.",
        "valid_range": ">= 1, non-null",
    },
    "retailer_name": {
        "source": "retailer_name",
        "type": "STRING",
        "is_required": True,
        "description": "Retailer's display name.",
    },
    "region": {
        "source": "region",
        "type": "STRING",
        "is_required": True,
        "description": "Sales region the retailer belongs to.",
    },
    "revenue": {
        "source": "revenue",
        "type": "DECIMAL(18,2)",
        "is_required": True,
        "description": "Retailer revenue, cleaned and cast from the bronze source.",
        "unit": "USD (assumed \u2014 confirm currency with source system)",
        "valid_range": ">= 0",
    },
    "signup_date": {
        "source": "signup_date",
        "type": "DATE",
        "is_required": True,
        "description": "Date the retailer signed up.",
        "valid_range": "not in the future",
    },
}

SILVER_TABLE_METADATA = {
    "comment": "Cleaned, validated, and deduplicated retailer data (bronze to silver).",
    "grain": "One row per retailer_id.",
    "primary_key": ["retailer_id"],
    "freshness": (
        "Updated daily by the `retailers-bronze-to-gold` Prefect deployment "
        "(cron 0 2 * * * UTC), ahead of the gold stage."
    ),
    "caveats": [
        "region enum values have not been confirmed against the source system; treat as free text.",
    ],
}


def run_transformation_clean(input_path: str, output_path: str) -> dict:
    spark = build_spark_session()
    try:
        source_df = load_dataframe(spark, input_path)
        rows_before = source_df.count()
        cleaned_df = remove_duplicates(source_df, subset_columns=["retailer_id"])
        null_counts = get_null_counts(source_df)
        print("Null counts before cleaning:")
        null_counts.show()

        req_columns = required_columns(SILVER_COLUMN_CONFIG)
        validate_required_columns(source_df, req_columns)

        create_table_sql = build_create_table_sql(
            table_name=SILVER_TABLE_NAME,
            column_config=SILVER_COLUMN_CONFIG,
            location=output_path,
            table_comment=SILVER_TABLE_METADATA["comment"],
        )
        store_df_to_table(
            spark,
            df=cleaned_df,
            table_name=SILVER_TABLE_NAME,
            column_config=SILVER_COLUMN_CONFIG,
            create_table_sql=create_table_sql,
        )

        dictionary_entry = build_data_dictionary_entry(
            spark, SILVER_TABLE_NAME, SILVER_COLUMN_CONFIG, SILVER_TABLE_METADATA
        )
        write_data_dictionary(dictionary_entry, DOCS_DIR)

        rows_after = cleaned_df.count()
        return compute_quality_report(rows_before, rows_after, duplicates_removed=rows_before - rows_after)
    finally:
        spark.stop()


if __name__ == "__main__":
    report = run_transformation_clean(DEFAULT_INPUT, DEFAULT_OUTPUT)
    print(f"Transformation complete: {report}")
