CREATE TABLE IF NOT EXISTS dim_category (
    category_key INT AUTO_INCREMENT PRIMARY KEY,
    category_name VARCHAR(100),
    start_date DATE,
    end_date DATE,
    is_current BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
