-- 5_scd_logic.sql
USE ecommerce_dw_demo;

DELIMITER $$

CREATE PROCEDURE sp_etl_load(IN p_batch_id CHAR(36))
BEGIN
    -- dim_date
    INSERT INTO dim_date (date_key, full_date, day_num, month_num, month_name, quarter_num, year_num, week_of_year, is_weekend)
    SELECT DISTINCT
        CAST(DATE_FORMAT(DATE(so.order_date), '%Y%m%d') AS UNSIGNED),
        DATE(so.order_date),
        DAY(so.order_date),
        MONTH(so.order_date),
        MONTHNAME(so.order_date),
        QUARTER(so.order_date),
        YEAR(so.order_date),
        WEEK(so.order_date, 3),
        CASE WHEN DAYOFWEEK(so.order_date) IN (1,7) THEN 1 ELSE 0 END
    FROM staging_orders so
    LEFT JOIN dim_date dd ON dd.full_date = DATE(so.order_date)
    WHERE dd.date_key IS NULL;

    -- SCD Type 1 (product overwrite)
    INSERT INTO dim_product (product_id, product_name, category, price, source_last_updated)
    SELECT sp.product_id, sp.product_name, sp.category, sp.price, sp.last_updated
    FROM staging_products sp
    ON DUPLICATE KEY UPDATE
        product_name = VALUES(product_name),
        category = VALUES(category),
        price = VALUES(price),
        source_last_updated = VALUES(source_last_updated);

    -- SCD Type 2 (customer history)
    UPDATE dim_customer dc
    JOIN staging_users su
      ON dc.user_id = su.user_id
     AND dc.is_active = 1
    SET dc.end_date = NOW(),
        dc.is_active = 0
    WHERE COALESCE(dc.name, '') <> COALESCE(su.name, '')
       OR COALESCE(dc.email, '') <> COALESCE(su.email, '')
       OR COALESCE(dc.city, '') <> COALESCE(su.city, '')
       OR COALESCE(dc.country, '') <> COALESCE(su.country, '');

    INSERT INTO dim_customer (user_id, name, email, city, country, start_date, end_date, is_active, source_last_updated)
    SELECT su.user_id, su.name, su.email, su.city, su.country, NOW(), NULL, 1, su.last_updated
    FROM staging_users su
    LEFT JOIN dim_customer dc_active
      ON dc_active.user_id = su.user_id
     AND dc_active.is_active = 1
    WHERE dc_active.customer_key IS NULL;

    INSERT INTO etl_log(batch_id, phase, last_loaded_time, rows_affected, status, message)
    VALUES (
        p_batch_id, 'SCD_LOAD', NOW(),
        ROW_COUNT(),
        'SUCCESS', 'SCD Type 1 and Type 2 completed'
    );
END$$

DELIMITER ;
