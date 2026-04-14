-- 2_staging_tables.sql
USE ecommerce_dw_demo;

CREATE TABLE staging_users LIKE users;
CREATE TABLE staging_products LIKE products;
CREATE TABLE staging_orders LIKE orders;
CREATE TABLE staging_order_items LIKE order_items;
CREATE TABLE staging_payments LIKE payments;
