# Project Guidelines

Spark + Prefect medallion (bronze/silver/gold) pipeline against MinIO (S3-compatible).
See [README.md](../README.md) for full architecture, setup, and usage docs — only
gotchas not already covered there are listed below.

## Build and Test

- Dependency/environment management is `uv`, not pip — there is no `requirements.txt`.
  Use `uv sync`, `uv add <pkg>` / `uv add --dev <pkg>`, `uv run pytest`. Always commit
  `uv.lock` alongside any `pyproject.toml` dependency change.
- `export JAVA_HOME` (from `.env`) before running PySpark directly — `.env` only sets it
  as a shell var, not exported, so the subprocess Java gateway fails with "Unable to
  locate a Java Runtime" even though `java -version` works interactively.
- `SPARK_LOCAL_IP=127.0.0.1` is required locally and in CI to avoid Spark
  driver/executor network timeouts on macOS runners/dev machines.
- `run_transformation_clean()` in each `transformations/customer/*.py` module calls
  `build_spark_session()`, which requires live MinIO credentials/endpoint and downloads
  `hadoop-aws`/`aws-java-sdk-bundle` jars — never call it from unit tests. Test the same
  steps (`remove_duplicates`, `validate_required_columns`, `group_by_agg`, etc.) directly
  against the local `spark` fixture instead, as in
  `tests/test_transformations/test_customer_flow.py`.
- CI (`.github/workflows/ci.yml`) runs `uv run ruff check .` before `uv run pytest`. Run
  it locally before pushing (`uv run ruff check --fix .` to auto-fix what it can).

## Conventions

- `ruff` is a dev dependency (`[dependency-groups].dev` + `[tool.ruff]` in
  `pyproject.toml`) and runs in CI — keep code passing `uv run ruff check .`.
- Per-table column configs (`SILVER_COLUMN_CONFIG`, `GOLD_REGION_SUMMARY_COLUMN_CONFIG`)
  are the single source of truth: each entry has `source`, `type`, `is_required`, plus
  data-dictionary metadata (`description`, `unit`, `valid_range`). Derive required-column
  lists from this config via `required_columns()` in `common/utils.py` — don't hardcode a
  separate list.
- Spark SQL string literals escape a single quote with a backslash (`.replace("'", "\\'")`),
  not a doubled quote (`''`) — `build_create_table_sql` relies on this; doubling causes a
  `PARSE_SYNTAX_ERROR` at runtime even though the Python is valid.
- Every Silver/Gold run also registers a catalog table and regenerates
  `docs/data_dictionary.{json,md}` — keep `*_COLUMN_CONFIG`/`*_TABLE_METADATA` accurate,
  since both are derived from them.
- No CI deploy job is intentional: the local Prefect server/worker only run on the
  developer's own machine and aren't reachable from CI. Only add one if the project moves
  to Prefect Cloud or a publicly reachable Prefect server.
