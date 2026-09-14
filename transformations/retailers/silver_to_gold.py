from __future__ import annotations

import os

from dotenv import load_dotenv

from common.spark import PROJECT_ROOT, build_spark_session
from common.utils import (
    build_create_table_sql,
    build_data_dictionary_entry,
    get_null_counts,
    group_by_agg,
    load_dataframe,
    remove_duplicates,
    required_columns,
    store_df_to_table,
    validate_required_columns,
    write_data_dictionary,
)

load_dotenv(PROJECT_ROOT / ".env")  # populates os.environ from .env regardless of how the shell was invoked
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "data-bucket")
DEFAULT_INPUT = f"s3a://{MINIO_BUCKET}/silver/retailers"
DEFAULT_OUTPUT = f"s3a://{MINIO_BUCKET}/gold/retailers_summary"
DOCS_DIR = PROJECT_ROOT / "docs"

GOLD_TABLE_NAME = "gold.retailer_region_summary"

# The aggregate output (region, sum_revenue) has different columns than the silver table,
# so it gets its own config/table (see transformations/customer/silver_to_gold.py's
# GOLD_REGION_SUMMARY_COLUMN_CONFIG for the reference this is modeled on).
GOLD_REGION_SUMMARY_COLUMN_CONFIG = {
    "region": {
        "source": "region",
        "type": "STRING",
        "is_required": True,
        "description": "Sales region the retailer belongs to.",
    },
    "sum_revenue": {
        "source": "sum_revenue",
        "type": "DECIMAL(18,2)",
        "is_required": True,
        "description": "Sum of retailer revenue for the region.",
    },
}
GOLD_REGION_SUMMARY_METADATA = {
    "comment": "Region-level retailer revenue totals, derived from silver.retailers.",
    "grain": "One row per region.",
    "primary_key": ["region"],
}


def run_transformation_clean(input_path: str, output_path: str) -> dict:
    spark = build_spark_session()
    try:
        source_df = load_dataframe(spark, input_path, "parquet")
        cleaned_df = remove_duplicates(source_df, subset_columns=["retailer_id"])
        null_counts = get_null_counts(source_df)
        print("Null counts before cleaning:")
        null_counts.show()

        regional_totals = group_by_agg(cleaned_df, ["region"], {"revenue": "sum"})
        print("Total revenue by region:")
        regional_totals.show()

        req_columns = required_columns(GOLD_REGION_SUMMARY_COLUMN_CONFIG)
        validate_required_columns(regional_totals, req_columns)

        create_table_sql = build_create_table_sql(
            table_name=GOLD_TABLE_NAME,
            column_config=GOLD_REGION_SUMMARY_COLUMN_CONFIG,
            location=output_path,
            table_comment=GOLD_REGION_SUMMARY_METADATA["comment"],
        )
        store_df_to_table(
            spark,
            df=regional_totals,
            table_name=GOLD_TABLE_NAME,
            column_config=GOLD_REGION_SUMMARY_COLUMN_CONFIG,
            create_table_sql=create_table_sql,
        )

        dictionary_entry = build_data_dictionary_entry(
            spark, GOLD_TABLE_NAME, GOLD_REGION_SUMMARY_COLUMN_CONFIG, GOLD_REGION_SUMMARY_METADATA
        )
        write_data_dictionary(dictionary_entry, DOCS_DIR)
    finally:
        spark.stop()


if __name__ == "__main__":
    report = run_transformation_clean(DEFAULT_INPUT, DEFAULT_OUTPUT)
    print(f"Transformation complete: {report}")
