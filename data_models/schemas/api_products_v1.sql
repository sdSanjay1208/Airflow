CREATE TABLE IF NOT EXISTS api_products_raw (
    id INT PRIMARY KEY,
    title TEXT,
    category VARCHAR(100),
    price DECIMAL(10,2),
    description TEXT,
    image_url TEXT
);
