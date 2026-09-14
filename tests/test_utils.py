from pathlib import Path

import pytest
from dotenv import load_dotenv

from common.spark import PROJECT_ROOT
from common.utils import (
    compute_quality_report,
    get_duplicate_rows,
    get_null_counts,
    group_by_agg,
    load_dataframe,
    remove_duplicates,
    validate_required_columns,
)

load_dotenv(PROJECT_ROOT / ".env")
FIXTURES_DIR = Path(__file__).parent / "fixtures"

#Sample tests for common utility functions
def test_csv_load_dataframe(spark):
    df = load_dataframe(spark, str(FIXTURES_DIR / "csv" / "raw_customers.csv"), input_format="csv")
    assert df.columns == ["customer_id", "customer_name", "amount"]
    assert df.count() == 4

def test_validate_required_columns_passes_when_present(spark):
    # No exception should be raised when every required column exists.
    df = spark.createDataFrame([(1, "Alice")], ["customer_id", "customer_name"])
    validate_required_columns(df, ["customer_id", "customer_name"])

def test_validate_required_columns_raises_when_missing(spark):
    # "region" isn't a column in df, so this must raise with the missing name in the message.
    df = spark.createDataFrame([(1, "Alice")], ["customer_id", "customer_name"])
    with pytest.raises(ValueError, match="region"):
        validate_required_columns(df, ["customer_id", "region"])

def test_remove_duplicates_keys_on_subset_columns(spark):
    # Two rows share customer_id 101 but differ in customer_name; dedup should still collapse them.
    df = spark.createDataFrame(
        [(101, "Alice"), (101, "Alice Duplicate"), (102, "Bob")],
        ["customer_id", "customer_name"],
    )
    deduped = remove_duplicates(df, subset_columns=["customer_id"])
    assert deduped.count() == 2


def test_get_duplicate_rows_returns_only_repeated_keys(spark):
    # customer_id 102 is unique and must be excluded from the result.
    df = spark.createDataFrame(
        [(101, "Alice"), (101, "Alice"), (102, "Bob")],
        ["customer_id", "customer_name"],
    )
    duplicates = get_duplicate_rows(df, subset_columns=["customer_id"])
    assert duplicates.count() == 2
    assert {row["customer_id"] for row in duplicates.collect()} == {101}


def test_group_by_agg_sums_per_group(spark):
    # Verifies both the grouping (per region) and the "sum_<column>" output naming convention.
    df = spark.createDataFrame(
        [("east", 100), ("east", 50), ("west", 30)],
        ["region", "amount"],
    )
    totals = group_by_agg(df, ["region"], {"amount": "sum"})
    rows = {row["region"]: row["sum_amount"] for row in totals.collect()}
    assert rows == {"east": 150, "west": 30}


def test_get_null_counts_counts_nulls_per_column(spark):
    # Each row has exactly one null, in a different column, so both counts should be 1.
    df = spark.createDataFrame(
        [(1, None), (None, "Bob")],
        ["customer_id", "customer_name"],
    )
    counts = get_null_counts(df).collect()[0]
    assert counts["customer_id"] == 1
    assert counts["customer_name"] == 1


def test_compute_quality_report_counts_removed_rows():
    # Without an explicit duplicates_removed, the removed count falls back to rows_before - rows_after.
    report = compute_quality_report(rows_before=10, rows_after=7)
    assert report == {
        "rows_before": 10,
        "rows_after": 7,
        "duplicates_or_empty_rows_removed": 3,
    }


def test_compute_quality_report_uses_explicit_duplicates_removed():
    # An explicit duplicates_removed larger than rows_before - rows_after should win (max of the two).
    report = compute_quality_report(rows_before=10, rows_after=9, duplicates_removed=5)
    assert report["duplicates_or_empty_rows_removed"] == 5
