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
    store_df_to_table,
    validate_required_columns,
    write_data_dictionary,
    required_columns,
)

load_dotenv(PROJECT_ROOT / ".env")  # populates os.environ from .env regardless of how the shell was invoked
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "data-bucket")
DEFAULT_INPUT = f"s3a://{MINIO_BUCKET}/bronze/input.csv"
DEFAULT_OUTPUT = f"s3a://{MINIO_BUCKET}/silver/customer_orders"
DOCS_DIR = PROJECT_ROOT / "docs"

EXPECTED_TYPES = {
    "customer_id": "int",
    "amount": "decimal(18,2)",
    "order_date": "date",
}

SILVER_TABLE_NAME = "silver.customer_orders"

# Column metadata: "source"/"type"/"is_required" drive the catalog table, the rest feeds the data dictionary.
SILVER_COLUMN_CONFIG = {
    "customer_id": {
        "source": "customer_id",
        "type": "INT",
        "is_required": True,
        "description": "Unique identifier for the customer who placed the order.",
        "valid_range": ">= 1, non-null",
    },
    "customer_name": {
        "source": "customer_name",
        "type": "STRING",
        "is_required": True,
        "description": "Customer's display name.",
    },
    "region": {
        "source": "region",
        "type": "STRING",
        "is_required": True,
        "description": "Sales region the order belongs to.",
    },
    "amount": {
        "source": "amount",
        "type": "DECIMAL(18,2)",
        "is_required": True,
        "description": "Order amount, cleaned and cast from the bronze source.",
        "unit": "USD (assumed \u2014 confirm currency with source system)",
        "valid_range": ">= 0",
    },
    "order_date": {
        "source": "order_date",
        "type": "DATE",
        "is_required": True,
        "description": "Date the order was placed.",
        "valid_range": "not in the future",
    },
}

SILVER_TABLE_METADATA = {
    "comment": "Cleaned, validated, and deduplicated customer order data (bronze to silver).",
    "grain": "One row per customer_id.",
    "primary_key": ["customer_id"],
    "freshness": (
        "Updated daily by the `customer-bronze-to-gold` Prefect deployment "
        "(cron 0 2 * * * UTC), ahead of the gold stage."
    ),
    "caveats": [
        "Despite covering 'orders', deduplication is keyed only on customer_id "
        "(clean_dataframe with source_key_columns=['customer_id']), so there is at most "
        "one row per customer, not one row per order.",
        "region enum values have not been confirmed against the source system; treat as free text.",
    ],
}


def run_transformation_clean(input_path: str, output_path: str) -> dict:
    spark = build_spark_session()
    try:
        source_df = load_dataframe(spark, input_path)
        rows_before = source_df.count()
        cleaned_df = remove_duplicates(source_df, subset_columns=["customer_id"])
        null_counts = get_null_counts(source_df)
        print("Null counts after cleaning:")
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
