from pathlib import Path

from dotenv import load_dotenv

from common.spark import PROJECT_ROOT
from common.utils import (
    apply_expected_casts,
    clean_dataframe,
    compute_quality_report,
    load_dataframe,
)

load_dotenv(PROJECT_ROOT / ".env")
FIXTURES_DIR = Path(__file__).parent / "fixtures"


def test_csv_load_dataframe(spark):
    df = load_dataframe(spark, str(FIXTURES_DIR / "csv" / "raw_customers.csv"), input_format="csv")
    assert df.columns == ["customer_id", "customer_name", "amount"]
    assert df.count() == 4

def test_parquet_load_dataframe(spark):
    df = load_dataframe(spark, str(FIXTURES_DIR / "parquet" / "part-00000-83b9f5a8-973e-43a6-b94d-bc82d0b67698-c000.snappy.parquet"), input_format="parquet")
    df.show()
    


def test_clean_dataframe_trims_and_deduplicates(spark):
    cleaned = clean_dataframe(
        load_dataframe(spark, str(FIXTURES_DIR / "csv" / "raw_customers.csv"), input_format="csv"),
        source_key_columns=["customer_id"],
        required_columns=["customer_id", "customer_name", "amount"],
    )

    assert cleaned.count() == 2
    rows = cleaned.orderBy("customer_id").collect()
    assert rows[0]["customer_id"] == 101
    assert rows[0]["customer_name"] == "Alice"
    assert rows[1]["customer_id"] == 102
    assert rows[1]["customer_name"] == "Bob"


def test_apply_expected_casts_and_quality_report(spark):
    typed_order_df = load_dataframe(spark, str(FIXTURES_DIR / "csv" / "typed_orders.csv"), input_format="csv")
    typed = apply_expected_casts(
        typed_order_df,
        {"customer_id": "int", "order_date": "date", "amount": "decimal(18,2)"},
    )

    assert str(typed.schema["customer_id"].dataType) == "IntegerType()"
    assert str(typed.schema["order_date"].dataType) == "DateType()"
    assert str(typed.schema["amount"].dataType) == "DecimalType(18,2)"

    report = compute_quality_report(3, 2, duplicates_removed=1)
    assert report["rows_before"] == 3
    assert report["rows_after"] == 2
    assert report["duplicates_or_empty_rows_removed"] == 1
