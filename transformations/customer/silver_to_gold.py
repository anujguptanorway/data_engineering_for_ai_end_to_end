from __future__ import annotations
import os
from dotenv import load_dotenv
from common.spark import PROJECT_ROOT, build_spark_session
from common.utils import (
    build_create_table_sql,
    build_data_dictionary_entry,
    clean_dataframe,
    compute_quality_report,
    load_dataframe,
    store_df_to_table,
    write_data_dictionary,
)

load_dotenv(PROJECT_ROOT / ".env")  # populates os.environ from .env regardless of how the shell was invoked
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "data-bucket")
DEFAULT_INPUT = f"s3a://{MINIO_BUCKET}/silver/customer_orders"
DEFAULT_OUTPUT = f"s3a://{MINIO_BUCKET}/gold/customer_orders"
DOCS_DIR = PROJECT_ROOT / "docs"

EXPECTED_TYPES = {
    "customer_id": "int",
    "amount": "decimal(18,2)",
    "order_date": "date",
}
REQUIRED_COLUMNS = ["customer_id", "customer_name", "region", "amount", "order_date"]

GOLD_TABLE_NAME = "gold.customer_orders"

# Column metadata: "source"/"type" drive the catalog table, the rest feeds the data dictionary.
GOLD_COLUMN_CONFIG = {
    "customer_id": {
        "source": "customer_id",
        "type": "INT",
        "description": "Unique identifier for the customer who placed the order.",
        "valid_range": ">= 1, non-null",
    },
    "customer_name": {
        "source": "customer_name",
        "type": "STRING",
        "description": "Customer's display name.",
    },
    "region": {
        "source": "region",
        "type": "STRING",
        "description": "Sales region the order belongs to.",
    },
    "amount": {
        "source": "amount",
        "type": "DECIMAL(18,2)",
        "description": "Total order amount.",
        "unit": "USD (assumed \u2014 confirm currency with source system)",
        "valid_range": ">= 0",
    },
    "order_date": {
        "source": "order_date",
        "type": "DATE",
        "description": "Date the order was placed.",
        "valid_range": "not in the future",
    },
}

GOLD_TABLE_METADATA = {
    "comment": "Business-ready customer order data for downstream reporting and analytics.",
    "grain": "One row per customer_id.",
    "primary_key": ["customer_id"],
    "freshness": "Updated daily by the `customer-bronze-to-gold` Prefect deployment (cron 0 2 * * * UTC).",
    "caveats": [
        "Despite the table name, deduplication upstream (clean_dataframe with "
        "source_key_columns=['customer_id']) keeps only one row per customer_id, "
        "not one row per order. A customer with multiple orders is not fully represented.",
        "region enum values have not been confirmed against the source system; treat as free text.",
    ],
}


def run_transformation_clean(input_path: str, output_path: str) -> dict:
    spark = build_spark_session()
    try:
        source_df = load_dataframe(spark, input_path,"parquet")
        rows_before = source_df.count()

        cleaned_df = clean_dataframe(
            source_df,
            source_key_columns=["customer_id"],
            required_columns=REQUIRED_COLUMNS,
            expected_types=EXPECTED_TYPES,
        )
        rows_after = cleaned_df.count()

        create_table_sql = build_create_table_sql(
            table_name=GOLD_TABLE_NAME,
            column_config=GOLD_COLUMN_CONFIG,
            location=output_path,
            table_comment=GOLD_TABLE_METADATA["comment"],
        )
        store_df_to_table(
            spark,
            df=cleaned_df,
            table_name=GOLD_TABLE_NAME,
            column_config=GOLD_COLUMN_CONFIG,
            create_table_sql=create_table_sql,
        )

        dictionary_entry = build_data_dictionary_entry(
            spark, GOLD_TABLE_NAME, GOLD_COLUMN_CONFIG, GOLD_TABLE_METADATA
        )
        write_data_dictionary(dictionary_entry, DOCS_DIR)

        return compute_quality_report(rows_before, rows_after)
    finally:
        spark.stop()

if __name__ == "__main__":
    report = run_transformation_clean(DEFAULT_INPUT, DEFAULT_OUTPUT)
    print(f"Transformation complete: {report}")
