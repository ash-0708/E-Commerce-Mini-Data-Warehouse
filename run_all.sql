-- run_all.sql
-- Run from MySQL client with SOURCE command:
-- SOURCE C:/Users/sande/ecommerce-data-warehouse/run_all.sql;

SOURCE C:/Users/sande/ecommerce-data-warehouse/1_oltp_schema.sql;
SOURCE C:/Users/sande/ecommerce-data-warehouse/2_staging_tables.sql;
SOURCE C:/Users/sande/ecommerce-data-warehouse/3_data_warehouse_schema.sql;
SOURCE C:/Users/sande/ecommerce-data-warehouse/sample_data/data.sql;
SOURCE C:/Users/sande/ecommerce-data-warehouse/4_etl_pipeline.sql;
SOURCE C:/Users/sande/ecommerce-data-warehouse/5_scd_logic.sql;
SOURCE C:/Users/sande/ecommerce-data-warehouse/6_incremental_load.sql;
SOURCE C:/Users/sande/ecommerce-data-warehouse/7_procedure.sql;

USE ecommerce_dw_demo;
CALL run_etl();

SOURCE C:/Users/sande/ecommerce-data-warehouse/queries/analytics.sql;
