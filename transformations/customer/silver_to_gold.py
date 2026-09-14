from __future__ import annotations
import os
from dotenv import load_dotenv
from pyspark.sql import functions as F
from common.spark import PROJECT_ROOT, build_spark_session
from common.utils import (
    build_create_table_sql,
    build_data_dictionary_entry,
    get_null_counts,
    group_by_agg,
    load_dataframe,
    remove_duplicates,
    store_df_to_table,
    validate_required_columns,
    write_data_dictionary,
    required_columns,
)

load_dotenv(PROJECT_ROOT / ".env")  # populates os.environ from .env regardless of how the shell was invoked
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "data-bucket")
DEFAULT_INPUT = f"s3a://{MINIO_BUCKET}/silver/customer_orders"
DEFAULT_OUTPUT = f"s3a://{MINIO_BUCKET}/gold/customer_orders_summary"
DOCS_DIR = PROJECT_ROOT / "docs"

# regional_totals has different columns (region, sum_amount) than GOLD_COLUMN_CONFIG, so it needs its own config/table.
GOLD_REGION_SUMMARY_TABLE_NAME = "gold.region_order_summary"
GOLD_REGION_SUMMARY_COLUMN_CONFIG = {
    "region": {
        "source": "region",
        "type": "STRING",
        "is_required": True,
        "description": "Sales region the order belongs to.",
    },
    "sum_amount": {
        "source": "sum_amount",
        "type": "DECIMAL(18,2)",
        "is_required": True,
        "description": "Sum of order amounts for the region.",
    },
    "sum_amount_doubled": {
        "source": "sum_amount_doubled",
        "type": "DECIMAL(18,2)",
        "is_required": True,
        "description": "Sum of order amounts for the region, doubled.",
    },
}
GOLD_REGION_SUMMARY_METADATA = {
    "comment": "Region-level order amount totals, derived from gold.customer_orders.",
    "grain": "One row per region.",
    "primary_key": ["region"],
}


def run_transformation_clean(input_path: str, output_path: str) -> dict:
    spark = build_spark_session()
    try:
        source_df = load_dataframe(spark, input_path, "parquet")
        cleaned_df = remove_duplicates(source_df, subset_columns=["customer_id"])
        null_counts = get_null_counts(source_df)
        print("Null after before cleaning:")
        null_counts.show()
        regional_totals = group_by_agg(cleaned_df, ["region"], {"amount": "sum"})
        regional_totals = regional_totals.withColumn("sum_amount_doubled", F.col("sum_amount") * 2)
        print("Total amount by region:")
        regional_totals.show()

# Final check before loading df to table
        req_columns = required_columns(GOLD_REGION_SUMMARY_COLUMN_CONFIG)
        validate_required_columns(regional_totals, req_columns)
        region_summary_output_path = output_path.rsplit("/", 1)[0] + "/region_order_summary"
        region_summary_create_table_sql = build_create_table_sql(
            table_name=GOLD_REGION_SUMMARY_TABLE_NAME,
            column_config=GOLD_REGION_SUMMARY_COLUMN_CONFIG,
            location=region_summary_output_path,
            table_comment=GOLD_REGION_SUMMARY_METADATA["comment"],
        )
        store_df_to_table(
            spark,
            df=regional_totals,
            table_name=GOLD_REGION_SUMMARY_TABLE_NAME,
            column_config=GOLD_REGION_SUMMARY_COLUMN_CONFIG,
            create_table_sql=region_summary_create_table_sql,
        )

        dictionary_entry = build_data_dictionary_entry(
            spark, GOLD_REGION_SUMMARY_TABLE_NAME, GOLD_REGION_SUMMARY_COLUMN_CONFIG, GOLD_REGION_SUMMARY_METADATA
        )
        write_data_dictionary(dictionary_entry, DOCS_DIR)
    finally:
        spark.stop()

if __name__ == "__main__":
    report = run_transformation_clean(DEFAULT_INPUT, DEFAULT_OUTPUT)
    print(f"Transformation complete: {report}")
