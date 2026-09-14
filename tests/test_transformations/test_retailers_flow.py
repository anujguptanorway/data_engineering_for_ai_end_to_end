import pytest

from common.utils import group_by_agg, remove_duplicates, required_columns, validate_required_columns
from transformations.retailers.bronze_to_silver import SILVER_COLUMN_CONFIG
from transformations.retailers.silver_to_gold import GOLD_REGION_SUMMARY_COLUMN_CONFIG

# run_transformation_clean() itself needs build_spark_session() (live MinIO/S3A + Hive
# catalog), so these tests exercise the same cleaning/validation/aggregation steps the
# flows apply, against a local Spark session and in-memory data instead.


class TestBronzeToSilverFlow:
    def test_bronze_to_silver_dedup_and_validation(self, spark):
        bronze_df = spark.createDataFrame(
            [
                (101, "Acme Retail", "east", 1000.0, "2024-01-01"),
                (101, "Acme Retail", "east", 1000.0, "2024-01-01"),  # duplicate retailer row
                (102, "Best Goods", "west", 500.0, "2024-01-02"),
            ],
            ["retailer_id", "retailer_name", "region", "revenue", "signup_date"],
        )

        silver_df = remove_duplicates(bronze_df, subset_columns=["retailer_id"])
        validate_required_columns(silver_df, required_columns(SILVER_COLUMN_CONFIG))

        assert silver_df.count() == 2
        assert sorted(row["retailer_id"] for row in silver_df.collect()) == [101, 102]

    def test_bronze_to_silver_raises_when_required_column_missing(self, spark):
        bronze_df = spark.createDataFrame(
            [(101, "Acme Retail", 1000.0)],
            ["retailer_id", "retailer_name", "revenue"],
        )
        with pytest.raises(ValueError, match="region"):
            validate_required_columns(bronze_df, required_columns(SILVER_COLUMN_CONFIG))


class TestSilverToGoldFlow:
    def test_silver_to_gold_region_totals(self, spark):
        silver_df = spark.createDataFrame(
            [
                (101, "east", 1000.0),
                (101, "east", 1000.0),  # duplicate retailer row
                (102, "east", 500.0),
                (103, "west", 300.0),
            ],
            ["retailer_id", "region", "revenue"],
        )

        deduped = remove_duplicates(silver_df, subset_columns=["retailer_id"])
        regional_totals = group_by_agg(deduped, ["region"], {"revenue": "sum"})
        validate_required_columns(regional_totals, required_columns(GOLD_REGION_SUMMARY_COLUMN_CONFIG))

        rows = {row["region"]: row["sum_revenue"] for row in regional_totals.collect()}
        assert rows == {"east": 1500.0, "west": 300.0}

    def test_silver_to_gold_raises_when_required_column_missing(self, spark):
        regional_totals = spark.createDataFrame([("east", 1500.0)], ["region", "sum_revenue"])
        # No column is actually missing here; drop sum_revenue to prove the check works.
        with pytest.raises(ValueError, match="sum_revenue"):
            validate_required_columns(regional_totals.drop("sum_revenue"), required_columns(GOLD_REGION_SUMMARY_COLUMN_CONFIG))
