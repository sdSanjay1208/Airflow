-- STAGING → CURATED TRANSFORMATION

USE etl_project;

TRUNCATE TABLE cur_amazon;

INSERT INTO cur_amazon (
    order_id,
    customer_id,
    product_id,
    category,
    seller_id,
    order_date,
    load_date,
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
    is_current
)
SELECT
    order_id,
    customer_id,
    product_id,
    CONCAT(
        UPPER(LEFT(category, 1)),
        LOWER(SUBSTRING(category, 2))
    ) AS category,
    seller_id,
    order_date,
    CURRENT_DATE,
    quantity,
    unit_price,
    discount,
    tax,
    shipping_cost,
    (quantity * unit_price) - discount + tax + shipping_cost AS total_amount,
    payment_method,
    order_status,
    city,
    state,
    country,
    1
FROM stg_amazon;
