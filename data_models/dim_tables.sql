-- ===============================
-- DIMENSION TABLE: CATEGORY (SCD TYPE-2)
-- ===============================

CREATE TABLE IF NOT EXISTS dim_category (
    category_key INT AUTO_INCREMENT PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL,

    start_date DATE NOT NULL,
    end_date DATE DEFAULT NULL,

    is_current BOOLEAN DEFAULT TRUE,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
