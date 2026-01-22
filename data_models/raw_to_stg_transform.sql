-- RAW → STAGING TRANSFORMATION
-- Clears staging and reloads clean data

USE etl_project;

TRUNCATE TABLE stg_amazon;

INSERT INTO stg_amazon (
    order_id,
    order_date,
    customer_id,
    customer_name,
    product_id,
    product_name,
    category,
    brand,
    quantity,
    unit_price,
    discount,
    tax,
    shipping_cost,
    total_amount,
    payment_method,
    order_status,
    city,
    state,
    country,
    seller_id
)
SELECT DISTINCT
    order_id,
    order_date,
    customer_id,
    customer_name,
    product_id,
    product_name,
    category,
    brand,
    CAST(quantity AS UNSIGNED),
    CAST(unit_price AS DECIMAL(10,2)),
    CAST(discount AS DECIMAL(10,2)),
    CAST(tax AS DECIMAL(10,2)),
    CAST(shipping_cost AS DECIMAL(10,2)),
    CAST(total_amount AS DECIMAL(12,2)),
    payment_method,
    order_status,
    city,
    state,
    country,
    seller_id
FROM raw_amazon;
