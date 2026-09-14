# Data Dictionary

Auto-generated from `DESCRIBE EXTENDED` plus curated metadata. Do not edit by hand — update the column/table metadata in the transformation module instead.

## gold.customer_orders

Business-ready customer order data for downstream reporting and analytics.

- **Grain:** One row per customer_id.
- **Primary key:** customer_id
- **Freshness:** Updated daily by the `customer-bronze-to-gold` Prefect deployment (cron 0 2 * * * UTC).
- **Generated at:** 2026-09-11T20:12:21.919364+00:00
- **Caveats:**
  - Despite the table name, deduplication upstream (clean_dataframe with source_key_columns=['customer_id']) keeps only one row per customer_id, not one row per order. A customer with multiple orders is not fully represented.
  - region enum values have not been confirmed against the source system; treat as free text.

| Column | Type | Description | Unit | Valid values | Valid range |
| --- | --- | --- | --- | --- | --- |
| `customer_id` | int | Unique identifier for the customer who placed the order. |  |  | >= 1, non-null |
| `customer_name` | string | Customer's display name. |  |  |  |
| `region` | string | Sales region the order belongs to. |  |  |  |
| `amount` | decimal(18,2) | Total order amount. | USD (assumed — confirm currency with source system) |  | >= 0 |
| `order_date` | date | Date the order was placed. |  |  | not in the future |

## gold.region_order_summary

Region-level order amount totals, derived from gold.customer_orders.

- **Grain:** One row per region.
- **Primary key:** region
- **Freshness:** _not set_
- **Generated at:** 2026-09-14T10:13:04.784060+00:00

| Column | Type | Description | Unit | Valid values | Valid range |
| --- | --- | --- | --- | --- | --- |
| `region` | string | Sales region the order belongs to. |  |  |  |
| `sum_amount` | decimal(18,2) | Sum of order amounts for the region. |  |  |  |
| `sum_amount_doubled` | decimal(18,2) | Sum of order amounts for the region, doubled. |  |  |  |

## silver.customer_orders

Cleaned, validated, and deduplicated customer order data (bronze to silver).

- **Grain:** One row per customer_id.
- **Primary key:** customer_id
- **Freshness:** Updated daily by the `customer-bronze-to-gold` Prefect deployment (cron 0 2 * * * UTC), ahead of the gold stage.
- **Generated at:** 2026-09-14T09:58:24.447756+00:00
- **Caveats:**
  - Despite covering 'orders', deduplication is keyed only on customer_id (clean_dataframe with source_key_columns=['customer_id']), so there is at most one row per customer, not one row per order.
  - region enum values have not been confirmed against the source system; treat as free text.

| Column | Type | Description | Unit | Valid values | Valid range |
| --- | --- | --- | --- | --- | --- |
| `customer_id` | int | Unique identifier for the customer who placed the order. |  |  | >= 1, non-null |
| `customer_name` | string | Customer's display name. |  |  |  |
| `amount` | decimal(18,2) | Order amount, cleaned and cast from the bronze source. | USD (assumed — confirm currency with source system) |  | >= 0 |
| `order_date` | date | Date the order was placed. |  |  | not in the future |
| `region` | string | Sales region the order belongs to. |  |  |  |
