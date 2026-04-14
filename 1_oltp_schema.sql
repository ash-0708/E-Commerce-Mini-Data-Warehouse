-- 1_oltp_schema.sql
DROP DATABASE IF EXISTS ecommerce_dw_demo;
CREATE DATABASE ecommerce_dw_demo;
USE ecommerce_dw_demo;

SET sql_safe_updates = 0;

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
