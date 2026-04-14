# E-Commerce Data Warehouse (MySQL)

This project implements a mini SQL data warehouse with:
- OLTP transactional schema
- Staging layer
- Star schema (fact + dimensions)
- ETL with cleaning and transformation
- Incremental loading
- SCD Type 1 and Type 2
- Procedure automation and scheduled event
- Analytics queries

## Project Structure

`ecommerce-data-warehouse/`

- `1_oltp_schema.sql`
- `2_staging_tables.sql`
- `3_data_warehouse_schema.sql`
- `4_etl_pipeline.sql`
- `5_scd_logic.sql`
- `6_incremental_load.sql`
- `7_procedure.sql`
- `run_all.sql`
- `sample_data/data.sql`
- `sample_data/import_from_csv.sql`
- `queries/analytics.sql`
- `dashboard/powerbi.pbix` (placeholder)

## Run Order

Execute scripts in this exact order:

1. `1_oltp_schema.sql`
2. `2_staging_tables.sql`
3. `3_data_warehouse_schema.sql`
4. `sample_data/data.sql`
5. `4_etl_pipeline.sql`
6. `5_scd_logic.sql`
7. `6_incremental_load.sql`
8. `7_procedure.sql`
9. `CALL run_etl();`
10. `queries/analytics.sql`

## Notes

- Database name: `ecommerce_dw_demo`
- Incremental extract is driven by `etl_log.last_loaded_time`.
- Scheduler event (`ev_run_etl_hourly`) may require elevated privileges.
- `etl_error_log` captures ETL failures from `run_etl`.

## One-Command Run

```sql
SOURCE C:/Users/sande/ecommerce-data-warehouse/run_all.sql;
```

## One-Run in MySQL Workbench

`run_all.sql` uses `SOURCE`, which is for mysql CLI.
For MySQL Workbench query tab, run this file instead:

- `C:/Users/sande/ecommerce-data-warehouse/run_all_workbench.sql`

## Import Your Own CSV/Excel Data

- Put CSV files in a folder, for example:
  - `C:/Users/sande/ecommerce-data-warehouse/sample_data/csv`
- Required files:
  - `users.csv`
  - `products.csv`
  - `orders.csv`
  - `order_items.csv`
  - `payments.csv`
- Open and edit `sample_data/import_from_csv.sql`:
  - set `@base_path` to your CSV folder
- Run:

```sql
SOURCE C:/Users/sande/ecommerce-data-warehouse/sample_data/import_from_csv.sql;
```

Excel files (`.xlsx`) should be saved as CSV first.

## Quick Validation

```sql
USE ecommerce_dw_demo;
SELECT COUNT(*) AS fact_sales_rows FROM fact_sales;
SELECT phase, status, rows_affected FROM etl_log ORDER BY etl_id DESC LIMIT 10;
```
