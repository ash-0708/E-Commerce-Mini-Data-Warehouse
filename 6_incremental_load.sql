-- 6_incremental_load.sql
USE ecommerce_dw_demo;

DELIMITER $$

CREATE PROCEDURE sp_etl_fact_incremental(IN p_batch_id CHAR(36))
BEGIN
    INSERT INTO fact_sales (
        source_order_item_id,
        customer_key,
        product_key,
        date_key,
        order_id,
        quantity,
        unit_price,
        total_amount
    )
    SELECT
        soi.order_item_id,
        dc.customer_key,
        dp.product_key,
        CAST(DATE_FORMAT(DATE(so.order_date), '%Y%m%d') AS UNSIGNED),
        so.order_id,
        soi.quantity,
        soi.price,
        ROUND(soi.quantity * soi.price, 2)
    FROM staging_order_items soi
    JOIN staging_orders so
      ON so.order_id = soi.order_id
    JOIN dim_customer dc
      ON dc.user_id = so.user_id
     AND dc.is_active = 1
    JOIN dim_product dp
      ON dp.product_id = soi.product_id
    LEFT JOIN fact_sales fs
      ON fs.source_order_item_id = soi.order_item_id
     AND fs.date_key = CAST(DATE_FORMAT(DATE(so.order_date), '%Y%m%d') AS UNSIGNED)
    WHERE fs.sale_id IS NULL
      AND so.status <> 'CANCELLED';

    INSERT INTO etl_log(batch_id, phase, last_loaded_time, rows_affected, status, message)
    VALUES (
        p_batch_id, 'FACT_INCREMENTAL', NOW(),
        ROW_COUNT(),
        'SUCCESS', 'Incremental fact load completed'
    );
END$$

DELIMITER ;
