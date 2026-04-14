-- MINI DATA WAREHOUSE - E-COMMERCE (MySQL 8+)
-- End-to-end: OLTP -> Staging -> Star Schema -> ETL -> Automation -> Analytics

-- =========================================================
-- 0) DATABASE SETUP
-- =========================================================
DROP DATABASE IF EXISTS ecommerce_dw_demo;
CREATE DATABASE ecommerce_dw_demo;
USE ecommerce_dw_demo;

SET sql_safe_updates = 0;

-- =========================================================
-- 1) OLTP SCHEMA (NORMALIZED TRANSACTIONAL TABLES)
-- =========================================================
CREATE TABLE users (
    user_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(120) NOT NULL,
    email VARCHAR(180) NOT NULL,
    city VARCHAR(100),
    country VARCHAR(100),
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_updated DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_users_email (email)
);

CREATE TABLE products (
    product_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    product_name VARCHAR(180) NOT NULL,
    category VARCHAR(80) NOT NULL,
    price DECIMAL(12,2) NOT NULL,
    last_updated DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    order_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    user_id BIGINT NOT NULL,
    order_date DATETIME NOT NULL,
    total_amount DECIMAL(12,2) NOT NULL DEFAULT 0,
    status VARCHAR(30) NOT NULL,
    last_updated DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(user_id)
);

CREATE TABLE order_items (
    order_item_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    order_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(12,2) NOT NULL,
    CONSTRAINT fk_order_items_order FOREIGN KEY (order_id) REFERENCES orders(order_id),
    CONSTRAINT fk_order_items_product FOREIGN KEY (product_id) REFERENCES products(product_id)
);

CREATE TABLE payments (
    payment_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    order_id BIGINT NOT NULL,
    payment_method VARCHAR(40) NOT NULL,
    payment_status VARCHAR(40) NOT NULL,
    payment_date DATETIME NOT NULL,
    CONSTRAINT fk_payments_order FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

CREATE INDEX idx_orders_last_updated ON orders(last_updated);
CREATE INDEX idx_users_last_updated ON users(last_updated);
CREATE INDEX idx_products_last_updated ON products(last_updated);
CREATE INDEX idx_orders_order_date ON orders(order_date);

-- =========================================================
-- 2) SYNTHETIC DATA GENERATION (500-1000+ ROWS)
-- =========================================================
DROP TABLE IF EXISTS helper_numbers;
CREATE TABLE helper_numbers (
    n INT PRIMARY KEY
);

INSERT INTO helper_numbers (n)
SELECT a.n + b.n * 10 + c.n * 100 + d.n * 1000 + 1 AS n
FROM
    (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
     UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) a
CROSS JOIN
    (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
     UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) b
CROSS JOIN
    (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
     UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) c
CROSS JOIN
    (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
     UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d
WHERE a.n + b.n * 10 + c.n * 100 + d.n * 1000 < 10000;

-- Users: 1000
INSERT INTO users (name, email, city, country, created_at, last_updated)
SELECT
    CONCAT('User_', n) AS name,
    CONCAT('user', n, '@demo.com') AS email,
    ELT(1 + (n MOD 10), 'Delhi','Mumbai','Bengaluru','Hyderabad','Pune','Chennai','Kolkata','Jaipur','Ahmedabad','Lucknow') AS city,
    CASE WHEN n % 25 = 0 THEN NULL ELSE ELT(1 + (n MOD 5), 'India','USA','UK','UAE','Canada') END AS country,
    DATE_SUB(NOW(), INTERVAL (n MOD 365) DAY),
    DATE_SUB(NOW(), INTERVAL (n MOD 60) DAY)
FROM helper_numbers
WHERE n <= 1000;

-- Products: 300
INSERT INTO products (product_name, category, price, last_updated)
SELECT
    CONCAT('Product_', n),
    ELT(1 + (n MOD 8), 'Electronics','Books','Home','Fashion','Beauty','Sports','Toys','Grocery'),
    ROUND(50 + RAND() * 5000, 2),
    DATE_SUB(NOW(), INTERVAL (n MOD 50) DAY)
FROM helper_numbers
WHERE n <= 300;

-- Orders: 2500
INSERT INTO orders (user_id, order_date, total_amount, status, last_updated)
SELECT
    1 + (n MOD 1000) AS user_id,
    DATE_SUB(NOW(), INTERVAL (n MOD 365) DAY) + INTERVAL (n MOD 24) HOUR,
    0,
    ELT(1 + (n MOD 4), 'PLACED','SHIPPED','DELIVERED','CANCELLED'),
    DATE_SUB(NOW(), INTERVAL (n MOD 30) DAY)
FROM helper_numbers
WHERE n <= 2500;

-- Order Items: 6000
INSERT INTO order_items (order_id, product_id, quantity, price)
SELECT
    1 + (n MOD 2500) AS order_id,
    1 + (n MOD 300) AS product_id,
    1 + (n MOD 5) AS quantity,
    ROUND(50 + RAND() * 5000, 2) AS price
FROM helper_numbers
WHERE n <= 6000;

-- Payments: one payment per order
INSERT INTO payments (order_id, payment_method, payment_status, payment_date)
SELECT
    o.order_id,
    ELT(1 + (o.order_id MOD 4), 'CARD', 'UPI', 'NETBANKING', 'WALLET'),
    CASE WHEN o.status = 'CANCELLED' THEN 'FAILED' ELSE 'SUCCESS' END,
    o.order_date + INTERVAL (o.order_id MOD 180) MINUTE
FROM orders o;

-- Refresh order totals from order_items
UPDATE orders o
JOIN (
    SELECT order_id, ROUND(SUM(quantity * price), 2) AS amt
    FROM order_items
    GROUP BY order_id
) t ON t.order_id = o.order_id
SET o.total_amount = t.amt;

-- =========================================================
-- 3) STAGING LAYER
-- =========================================================
CREATE TABLE staging_users LIKE users;
CREATE TABLE staging_products LIKE products;
CREATE TABLE staging_orders LIKE orders;
CREATE TABLE staging_order_items LIKE order_items;
CREATE TABLE staging_payments LIKE payments;

-- =========================================================
-- 4) STAR SCHEMA (DW / OLAP)
-- =========================================================
CREATE TABLE dim_customer (
    customer_key BIGINT PRIMARY KEY AUTO_INCREMENT,
    user_id BIGINT NOT NULL,
    name VARCHAR(120) NOT NULL,
    email VARCHAR(180) NOT NULL,
    city VARCHAR(100),
    country VARCHAR(100),
    start_date DATETIME NOT NULL,
    end_date DATETIME NULL,
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    source_last_updated DATETIME NOT NULL,
    KEY idx_dim_customer_user_id (user_id),
    KEY idx_dim_customer_active (user_id, is_active)
);

CREATE TABLE dim_product (
    product_key BIGINT PRIMARY KEY AUTO_INCREMENT,
    product_id BIGINT NOT NULL,
    product_name VARCHAR(180) NOT NULL,
    category VARCHAR(80) NOT NULL,
    price DECIMAL(12,2) NOT NULL,
    source_last_updated DATETIME NOT NULL,
    UNIQUE KEY uq_dim_product_product_id (product_id)
);

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,               -- YYYYMMDD
    full_date DATE NOT NULL,
    day_num TINYINT NOT NULL,
    month_num TINYINT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    quarter_num TINYINT NOT NULL,
    year_num SMALLINT NOT NULL,
    week_of_year TINYINT NOT NULL,
    is_weekend TINYINT(1) NOT NULL,
    UNIQUE KEY uq_dim_date_full_date (full_date)
);

CREATE TABLE fact_sales (
    sale_id BIGINT NOT NULL AUTO_INCREMENT,
    source_order_item_id BIGINT NOT NULL,
    customer_key BIGINT NOT NULL,
    product_key BIGINT NOT NULL,
    date_key INT NOT NULL,
    order_id BIGINT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(12,2) NOT NULL,
    total_amount DECIMAL(14,2) NOT NULL,
    load_dts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (sale_id, date_key),
    UNIQUE KEY uq_fact_sales_source_oi (source_order_item_id, date_key),
    KEY idx_fact_sales_date_key (date_key),
    KEY idx_fact_sales_customer_key (customer_key),
    KEY idx_fact_sales_product_key (product_key)
)
PARTITION BY RANGE (date_key) (
    PARTITION p_before_2025 VALUES LESS THAN (20250101),
    PARTITION p_2025 VALUES LESS THAN (20260101),
    PARTITION p_2026 VALUES LESS THAN (20270101),
    PARTITION p_2027 VALUES LESS THAN (20280101),
    PARTITION p_max VALUES LESS THAN MAXVALUE
);

-- =========================================================
-- 5) ETL CONTROL + ERROR LOGGING
-- =========================================================
CREATE TABLE etl_log (
    etl_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    batch_id CHAR(36) NOT NULL,
    phase VARCHAR(40) NOT NULL,
    last_loaded_time DATETIME NOT NULL,
    rows_affected BIGINT NOT NULL DEFAULT 0,
    status VARCHAR(20) NOT NULL,
    message VARCHAR(500),
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE etl_error_log (
    error_id BIGINT PRIMARY KEY AUTO_INCREMENT,
    batch_id CHAR(36),
    procedure_name VARCHAR(100),
    error_message TEXT,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- =========================================================
-- 6) ETL PROCEDURES (EXTRACT, TRANSFORM, LOAD)
--    Includes SCD Type 1 and SCD Type 2
-- =========================================================
DELIMITER $$

CREATE PROCEDURE sp_etl_extract(IN p_batch_id CHAR(36), IN p_last_loaded DATETIME)
BEGIN
    TRUNCATE TABLE staging_users;
    TRUNCATE TABLE staging_products;
    TRUNCATE TABLE staging_orders;
    TRUNCATE TABLE staging_order_items;
    TRUNCATE TABLE staging_payments;

    INSERT INTO staging_users
    SELECT *
    FROM users
    WHERE last_updated > p_last_loaded;

    INSERT INTO staging_products
    SELECT *
    FROM products
    WHERE last_updated > p_last_loaded;

    INSERT INTO staging_orders
    SELECT *
    FROM orders
    WHERE last_updated > p_last_loaded;

    INSERT INTO staging_order_items
    SELECT oi.*
    FROM order_items oi
    JOIN staging_orders so ON so.order_id = oi.order_id;

    INSERT INTO staging_payments
    SELECT p.*
    FROM payments p
    JOIN staging_orders so ON so.order_id = p.order_id;

    INSERT INTO etl_log(batch_id, phase, last_loaded_time, rows_affected, status, message)
    VALUES (p_batch_id, 'EXTRACT', NOW(),
            (SELECT COUNT(*) FROM staging_orders),
            'SUCCESS',
            'Incremental extract completed');
END$$

CREATE PROCEDURE sp_etl_transform(IN p_batch_id CHAR(36))
BEGIN
    -- Data cleaning example
    UPDATE staging_users
    SET country = 'Unknown'
    WHERE country IS NULL OR TRIM(country) = '';

    UPDATE staging_users
    SET city = 'Unknown'
    WHERE city IS NULL OR TRIM(city) = '';

    -- Deduplicate on email (keep lowest user_id)
    DELETE su1
    FROM staging_users su1
    JOIN staging_users su2
      ON su1.email = su2.email
     AND su1.user_id > su2.user_id;

    INSERT INTO etl_log(batch_id, phase, last_loaded_time, rows_affected, status, message)
    VALUES (p_batch_id, 'TRANSFORM', NOW(),
            (SELECT COUNT(*) FROM staging_users),
            'SUCCESS',
            'Data cleansing and dedup completed');
END$$

CREATE PROCEDURE sp_etl_load(IN p_batch_id CHAR(36))
BEGIN
    -- ---------------------------
    -- DIM_DATE incremental upsert
    -- ---------------------------
    INSERT INTO dim_date (date_key, full_date, day_num, month_num, month_name, quarter_num, year_num, week_of_year, is_weekend)
    SELECT DISTINCT
        CAST(DATE_FORMAT(DATE(so.order_date), '%Y%m%d') AS UNSIGNED) AS date_key,
        DATE(so.order_date) AS full_date,
        DAY(so.order_date) AS day_num,
        MONTH(so.order_date) AS month_num,
        MONTHNAME(so.order_date) AS month_name,
        QUARTER(so.order_date) AS quarter_num,
        YEAR(so.order_date) AS year_num,
        WEEK(so.order_date, 3) AS week_of_year,
        CASE WHEN DAYOFWEEK(so.order_date) IN (1,7) THEN 1 ELSE 0 END AS is_weekend
    FROM staging_orders so
    LEFT JOIN dim_date dd
      ON dd.full_date = DATE(so.order_date)
    WHERE dd.date_key IS NULL;

    -- --------------------------------------------
    -- DIM_PRODUCT (SCD Type 1 - overwrite changes)
    -- --------------------------------------------
    INSERT INTO dim_product (product_id, product_name, category, price, source_last_updated)
    SELECT sp.product_id, sp.product_name, sp.category, sp.price, sp.last_updated
    FROM staging_products sp
    ON DUPLICATE KEY UPDATE
        product_name = VALUES(product_name),
        category = VALUES(category),
        price = VALUES(price),
        source_last_updated = VALUES(source_last_updated);

    -- ------------------------------------------------------------
    -- DIM_CUSTOMER (SCD Type 2 - preserve historical customer data)
    -- ------------------------------------------------------------
    -- 1) Expire active records if attributes changed
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

    -- 2) Insert brand-new users and changed users as new active rows
    INSERT INTO dim_customer (user_id, name, email, city, country, start_date, end_date, is_active, source_last_updated)
    SELECT su.user_id, su.name, su.email, su.city, su.country, NOW(), NULL, 1, su.last_updated
    FROM staging_users su
    LEFT JOIN dim_customer dc_active
      ON dc_active.user_id = su.user_id
     AND dc_active.is_active = 1
    WHERE dc_active.customer_key IS NULL;

    -- --------------------------------------------
    -- FACT_SALES incremental load (idempotent)
    -- --------------------------------------------
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
        soi.order_item_id AS source_order_item_id,
        dc.customer_key,
        dp.product_key,
        CAST(DATE_FORMAT(DATE(so.order_date), '%Y%m%d') AS UNSIGNED) AS date_key,
        so.order_id,
        soi.quantity,
        soi.price,
        ROUND(soi.quantity * soi.price, 2) AS total_amount
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
    WHERE fs.sale_id IS NULL
      AND so.status <> 'CANCELLED';

    INSERT INTO etl_log(batch_id, phase, last_loaded_time, rows_affected, status, message)
    VALUES (p_batch_id, 'LOAD', NOW(),
            ROW_COUNT(),
            'SUCCESS',
            'Dimensions and fact load completed');
END$$

CREATE PROCEDURE run_etl()
BEGIN
    DECLARE v_batch_id CHAR(36);
    DECLARE v_last_loaded DATETIME;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        INSERT INTO etl_error_log(batch_id, procedure_name, error_message)
        VALUES (v_batch_id, 'run_etl', 'SQL exception occurred, transaction rolled back');
        INSERT INTO etl_log(batch_id, phase, last_loaded_time, rows_affected, status, message)
        VALUES (v_batch_id, 'RUN', NOW(), 0, 'FAILED', 'ETL failed and rolled back');
    END;

    SET v_batch_id = UUID();
    SET v_last_loaded = COALESCE(
        (SELECT MAX(last_loaded_time) FROM etl_log WHERE status = 'SUCCESS'),
        '1900-01-01 00:00:00'
    );

    START TRANSACTION;
        CALL sp_etl_extract(v_batch_id, v_last_loaded);
        CALL sp_etl_transform(v_batch_id);
        CALL sp_etl_load(v_batch_id);
        INSERT INTO etl_log(batch_id, phase, last_loaded_time, rows_affected, status, message)
        VALUES (v_batch_id, 'RUN', NOW(), 1, 'SUCCESS', 'ETL pipeline completed successfully');
    COMMIT;
END$$

DELIMITER ;

-- =========================================================
-- 7) INITIAL ETL RUN
-- =========================================================
CALL run_etl();

-- =========================================================
-- 8) SCHEDULED JOB SIMULATION (MySQL EVENT)
-- =========================================================
-- Optional (requires SUPER/SYSTEM_VARIABLES_ADMIN privilege):
-- SET GLOBAL event_scheduler = ON;
-- If you cannot set GLOBAL, enable it in MySQL config and restart service.

DROP EVENT IF EXISTS ev_run_etl_hourly;
CREATE EVENT ev_run_etl_hourly
ON SCHEDULE EVERY 1 HOUR
DO
    CALL run_etl();

-- =========================================================
-- 9) SAMPLE CHANGE SIMULATION (TO PROVE SCD + INCREMENTAL)
-- =========================================================
-- SCD Type 2 example: customer attribute change
UPDATE users
SET city = 'Delhi NCR',
    country = 'India',
    last_updated = NOW()
WHERE user_id = 101;

-- SCD Type 1 example on product (overwrite current value)
UPDATE products
SET price = price + 100,
    last_updated = NOW()
WHERE product_id = 20;

-- New order to be captured incrementally
INSERT INTO orders (user_id, order_date, total_amount, status, last_updated)
VALUES (101, NOW(), 0, 'DELIVERED', NOW());

SET @new_order_id = LAST_INSERT_ID();

INSERT INTO order_items (order_id, product_id, quantity, price)
VALUES
(@new_order_id, 20, 2, 1499.00),
(@new_order_id, 35, 1, 999.00);

UPDATE orders o
JOIN (
    SELECT order_id, SUM(quantity * price) AS amt
    FROM order_items
    WHERE order_id = @new_order_id
    GROUP BY order_id
) x ON x.order_id = o.order_id
SET o.total_amount = x.amt,
    o.last_updated = NOW();

INSERT INTO payments(order_id, payment_method, payment_status, payment_date)
VALUES (@new_order_id, 'CARD', 'SUCCESS', NOW());

-- Run incremental ETL again
CALL run_etl();

-- =========================================================
-- 10) ANALYTICAL QUERIES
-- =========================================================
-- A) Revenue trends by month
SELECT
    dd.year_num,
    dd.month_num,
    dd.month_name,
    ROUND(SUM(fs.total_amount), 2) AS revenue
FROM fact_sales fs
JOIN dim_date dd ON dd.date_key = fs.date_key
GROUP BY dd.year_num, dd.month_num, dd.month_name
ORDER BY dd.year_num, dd.month_num;

-- B) Customer behavior: top 10 customers by spend
SELECT
    dc.user_id,
    dc.name,
    dc.city,
    dc.country,
    ROUND(SUM(fs.total_amount), 2) AS total_spent,
    COUNT(DISTINCT fs.order_id) AS total_orders
FROM fact_sales fs
JOIN dim_customer dc ON dc.customer_key = fs.customer_key
GROUP BY dc.user_id, dc.name, dc.city, dc.country
ORDER BY total_spent DESC
LIMIT 10;

-- C) Product performance: top 10 products
SELECT
    dp.product_id,
    dp.product_name,
    dp.category,
    SUM(fs.quantity) AS units_sold,
    ROUND(SUM(fs.total_amount), 2) AS sales_amount
FROM fact_sales fs
JOIN dim_product dp ON dp.product_key = fs.product_key
GROUP BY dp.product_id, dp.product_name, dp.category
ORDER BY units_sold DESC, sales_amount DESC
LIMIT 10;

-- D) Time-based insights: weekday vs weekend revenue
SELECT
    CASE WHEN dd.is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    ROUND(SUM(fs.total_amount), 2) AS revenue
FROM fact_sales fs
JOIN dim_date dd ON dd.date_key = fs.date_key
GROUP BY dd.is_weekend;

-- E) Time-based insights: quarterly trend
SELECT
    dd.year_num,
    dd.quarter_num,
    ROUND(SUM(fs.total_amount), 2) AS revenue
FROM fact_sales fs
JOIN dim_date dd ON dd.date_key = fs.date_key
GROUP BY dd.year_num, dd.quarter_num
ORDER BY dd.year_num, dd.quarter_num;

-- =========================================================
-- 11) VALIDATION QUERIES (OPTIONAL)
-- =========================================================
SELECT 'users' AS table_name, COUNT(*) AS row_count FROM users
UNION ALL SELECT 'products', COUNT(*) FROM products
UNION ALL SELECT 'orders', COUNT(*) FROM orders
UNION ALL SELECT 'order_items', COUNT(*) FROM order_items
UNION ALL SELECT 'payments', COUNT(*) FROM payments
UNION ALL SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL SELECT 'dim_date', COUNT(*) FROM dim_date
UNION ALL SELECT 'fact_sales', COUNT(*) FROM fact_sales;

SELECT * FROM etl_log ORDER BY etl_id DESC LIMIT 20;
SELECT * FROM etl_error_log ORDER BY error_id DESC LIMIT 20;
