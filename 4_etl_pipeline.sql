-- 4_etl_pipeline.sql
USE ecommerce_dw_demo;

DELIMITER $$

CREATE PROCEDURE sp_etl_extract(IN p_batch_id CHAR(36), IN p_last_loaded DATETIME)
BEGIN
    TRUNCATE TABLE staging_users;
    TRUNCATE TABLE staging_products;
    TRUNCATE TABLE staging_orders;
    TRUNCATE TABLE staging_order_items;
    TRUNCATE TABLE staging_payments;

    INSERT INTO staging_users
    SELECT * FROM users WHERE last_updated > p_last_loaded;

    INSERT INTO staging_products
    SELECT * FROM products WHERE last_updated > p_last_loaded;

    INSERT INTO staging_orders
    SELECT * FROM orders WHERE last_updated > p_last_loaded;

    INSERT INTO staging_order_items
    SELECT oi.*
    FROM order_items oi
    JOIN staging_orders so ON so.order_id = oi.order_id;

    INSERT INTO staging_payments
    SELECT p.*
    FROM payments p
    JOIN staging_orders so ON so.order_id = p.order_id;

    INSERT INTO etl_log(batch_id, phase, last_loaded_time, rows_affected, status, message)
    VALUES (
        p_batch_id, 'EXTRACT', NOW(),
        (SELECT COUNT(*) FROM staging_orders),
        'SUCCESS', 'Incremental extract completed'
    );
END$$

CREATE PROCEDURE sp_etl_transform(IN p_batch_id CHAR(36))
BEGIN
    UPDATE staging_users
    SET country = 'Unknown'
    WHERE country IS NULL OR TRIM(country) = '';

    UPDATE staging_users
    SET city = 'Unknown'
    WHERE city IS NULL OR TRIM(city) = '';

    DELETE su1
    FROM staging_users su1
    JOIN staging_users su2
      ON su1.email = su2.email
     AND su1.user_id > su2.user_id;

    INSERT INTO etl_log(batch_id, phase, last_loaded_time, rows_affected, status, message)
    VALUES (
        p_batch_id, 'TRANSFORM', NOW(),
        (SELECT COUNT(*) FROM staging_users),
        'SUCCESS', 'Data cleansing and dedup completed'
    );
END$$

DELIMITER ;
