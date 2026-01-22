-- =====================================================
-- Project: Enterprise Data Cleaning & ETL Orchestration
-- Database: etl_project
-- Author: Sanjay
-- Description: Table creation scripts for RAW, STAGING,
--              and CURATED layers
-- =====================================================

USE etl_project;

-- =====================================================
-- RAW LAYER (Source data as-is from CSV)
-- =====================================================
CREATE TABLE IF NOT EXISTS raw_amazon (
    order_id        VARCHAR(50),
    order_date      VARCHAR(50),
    customer_id     VARCHAR(50),
    customer_name   TEXT,
    product_id      VARCHAR(50),
    product_name    TEXT,
    category        TEXT,
    brand           TEXT,
    quantity        VARCHAR(50),
    unit_price      VARCHAR(50),
    discount        VARCHAR(50),
    tax             VARCHAR(50),
    shipping_cost   VARCHAR(50),
    total_amount    VARCHAR(50),
    payment_method  TEXT,
    order_status    TEXT,
    city            TEXT,
    state           TEXT,
    country         TEXT,
    seller_id       VARCHAR(50)
);

-- =====================================================
-- STAGING LAYER (Cleaned & standardized data)
-- =====================================================
CREATE TABLE IF NOT EXISTS stg_amazon (
    order_id        VARCHAR(50),
    order_date      DATE,
    customer_id     VARCHAR(50),
    customer_name   TEXT,
    product_id      VARCHAR(50),
    product_name    TEXT,
    category        TEXT,
    brand           TEXT,
    quantity        INT,
    unit_price      DECIMAL(10,2),
    discount        DECIMAL(10,2),
    tax             DECIMAL(10,2),
    shipping_cost   DECIMAL(10,2),
    total_amount    DECIMAL(12,2),
    payment_method  TEXT,
    order_status    TEXT,
    city            TEXT,
    state           TEXT,
    country         TEXT,
    seller_id       VARCHAR(50),
    processed_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- CURATED LAYER (Analytics-ready / business layer)
-- =====================================================
CREATE TABLE IF NOT EXISTS cur_amazon (
    amazon_sk      INT AUTO_INCREMENT PRIMARY KEY,

    -- Business keys (alphanumeric)
    order_id       VARCHAR(50),
    customer_id    VARCHAR(50),
    product_id     VARCHAR(50),
    seller_id      VARCHAR(50),

    -- Dates
    order_date     DATE,
    load_date      DATE,

    -- Measures
    quantity       INT,
    unit_price     DECIMAL(10,2),
    discount       DECIMAL(10,2),
    tax            DECIMAL(10,2),
    shipping_cost  DECIMAL(10,2),
    total_amount   DECIMAL(12,2),

    -- Dimensions
    payment_method TEXT,
    order_status   TEXT,
    city           TEXT,
    state          TEXT,
    country        TEXT,

    -- SCD ready
    is_current     BOOLEAN
);

-- =====================================================
-- INDEXING (ETL performance optimization)
-- =====================================================

-- RAW
CREATE INDEX idx_raw_order_id ON raw_amazon(order_id);

-- STAGING
CREATE INDEX idx_stg_order_id ON stg_amazon(order_id);
CREATE INDEX idx_stg_customer_id ON stg_amazon(customer_id);

-- CURATED
CREATE INDEX idx_cur_order_id ON cur_amazon(order_id);
CREATE INDEX idx_cur_order_date ON cur_amazon(order_date);
