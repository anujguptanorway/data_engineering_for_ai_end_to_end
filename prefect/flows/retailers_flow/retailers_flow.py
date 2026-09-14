from __future__ import annotations

import os

from dotenv import load_dotenv

from common.spark import PROJECT_ROOT
from prefect import flow, task
from transformations.retailers.bronze_to_silver import run_transformation_clean as run_transformation_silver
from transformations.retailers.silver_to_gold import run_transformation_clean as run_transformation_gold

load_dotenv(PROJECT_ROOT / ".env")

MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "data-bucket")
BRONZE_DEFAULT_INPUT = f"s3a://{MINIO_BUCKET}/bronze/retailers_input.csv"
SILVER_INPUT_OUTPUT = f"s3a://{MINIO_BUCKET}/silver/retailers"
GOLD_DEFAULT_OUTPUT = f"s3a://{MINIO_BUCKET}/gold/retailers_summary"


@task(
    name="transform-retailers-to-silver",
    retries=2,
    retry_delay_seconds=30,
    log_prints=True,
)
def transform_retailers_to_silver(input_path: str, output_path: str) -> dict:
    """Run the Spark transformation as a retryable Prefect task."""
    report = run_transformation_silver(input_path, output_path)
    print(f"Transformation quality report: {report}")
    return report


@task(
    name="transform-retailers-to-gold",
    retries=2,
    retry_delay_seconds=30,
    log_prints=True,
)
def transform_retailers_to_gold(
    input_path: str,
    output_path: str,
    upstream_report: dict | None = None,
) -> dict:
    """Run the Spark transformation as a retryable Prefect task."""
    report = run_transformation_gold(input_path, output_path)
    print(f"Transformation quality report: {report}")
    return report


@flow(name="retailers-data-pipeline-bronze-to-gold", log_prints=True)
def retailers_data_pipeline(
    bronze_input: str = BRONZE_DEFAULT_INPUT,
    silver_input_output: str = SILVER_INPUT_OUTPUT,
    gold_output: str = GOLD_DEFAULT_OUTPUT,
) -> dict:
    """Orchestrate the bronze-to-silver-to-gold retailers data transformation."""

    # Bronze -> Silver
    silver_report = transform_retailers_to_silver(bronze_input, silver_input_output)

    # Silver -> Gold; passing silver_report creates the dependency edge
    gold_report = transform_retailers_to_gold(
        silver_input_output,
        gold_output,
        silver_report,
        wait_for=[silver_report],
    )

    return {"silver": silver_report, "gold": gold_report}


if __name__ == "__main__":
    retailers_data_pipeline(
        bronze_input=BRONZE_DEFAULT_INPUT,
        silver_input_output=SILVER_INPUT_OUTPUT,
        gold_output=GOLD_DEFAULT_OUTPUT,
    )
