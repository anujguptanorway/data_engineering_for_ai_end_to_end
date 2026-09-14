import pytest

from common.utils import group_by_agg, remove_duplicates, required_columns, validate_required_columns
from transformations.customer.bronze_to_silver import SILVER_COLUMN_CONFIG
from transformations.customer.silver_to_gold import GOLD_REGION_SUMMARY_COLUMN_CONFIG

# run_transformation_clean() itself needs build_spark_session() (live MinIO/S3A + Hive
# catalog), so these tests exercise the same cleaning/validation/aggregation steps the
# flows apply, against a local Spark session and in-memory data instead.


class TestBronzeToSilverFlow:
    def test_bronze_to_silver_dedup_and_validation(self, spark):
        bronze_df = spark.createDataFrame(
            [
                (101, "Alice", "east", 100.0, "2024-01-01"),
                (101, "Alice", "east", 100.0, "2024-01-01"),  # duplicate customer row
                (102, "Bob", "west", 50.0, "2024-01-02"),
            ],
            ["customer_id", "customer_name", "region", "amount", "order_date"],
        )

        silver_df = remove_duplicates(bronze_df, subset_columns=["customer_id"])
        validate_required_columns(silver_df, required_columns(SILVER_COLUMN_CONFIG))

        assert silver_df.count() == 2
        assert sorted(row["customer_id"] for row in silver_df.collect()) == [101, 102]

    def test_bronze_to_silver_raises_when_required_column_missing(self, spark):
        bronze_df = spark.createDataFrame(
            [(101, "Alice", 100.0)],
            ["customer_id", "customer_name", "amount"],
        )
        with pytest.raises(ValueError, match="region"):
            validate_required_columns(bronze_df, required_columns(SILVER_COLUMN_CONFIG))


class TestSilverToGoldFlow:
    def test_silver_to_gold_region_totals(self, spark):
        silver_df = spark.createDataFrame(
            [
                (101, "east", 100.0),
                (101, "east", 100.0),  # duplicate customer row
                (102, "east", 50.0),
                (103, "west", 30.0),
            ],
            ["customer_id", "region", "amount"],
        )

        deduped = remove_duplicates(silver_df, subset_columns=["customer_id"])
        regional_totals = group_by_agg(deduped, ["region"], {"amount": "sum"})
        regional_totals = regional_totals.withColumn(
            "sum_amount_doubled", regional_totals["sum_amount"] * 2
        )
        validate_required_columns(regional_totals, required_columns(GOLD_REGION_SUMMARY_COLUMN_CONFIG))

        rows = {
            row["region"]: (row["sum_amount"], row["sum_amount_doubled"])
            for row in regional_totals.collect()
        }
        assert rows == {"east": (150.0, 300.0), "west": (30.0, 60.0)}

    def test_silver_to_gold_raises_when_required_column_missing(self, spark):
        regional_totals = spark.createDataFrame([("east", 150.0)], ["region", "sum_amount"])
        with pytest.raises(ValueError, match="sum_amount_doubled"):
            validate_required_columns(regional_totals, required_columns(GOLD_REGION_SUMMARY_COLUMN_CONFIG))
