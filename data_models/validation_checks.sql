-- VALIDATION CHECKS

USE etl_project;

-- Row count checks
SELECT COUNT(*) AS raw_count FROM raw_amazon;
SELECT COUNT(*) AS staging_count FROM stg_amazon;
SELECT COUNT(*) AS curated_count FROM cur_amazon;

-- Duplicate check (should return 0 rows)
SELECT order_id, product_id, COUNT(*) AS cnt
FROM cur_amazon
GROUP BY order_id, product_id
HAVING COUNT(*) > 1;

-- Null checks
SELECT COUNT(*) AS null_orders
FROM cur_amazon
WHERE order_id IS NULL;

-- Category standardization check
SELECT DISTINCT category
FROM cur_amazon
ORDER BY category;
