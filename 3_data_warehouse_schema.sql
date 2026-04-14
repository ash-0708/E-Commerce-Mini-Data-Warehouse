-- 3_data_warehouse_schema.sql
USE ecommerce_dw_demo;

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
    date_key INT PRIMARY KEY,
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
