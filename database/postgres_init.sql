CREATE TABLE IF NOT EXISTS public.transactions (
    id SERIAL PRIMARY KEY,
    order_id INT,
    user_id INT,
    product_id INT,
    quantity INT,
    total_price DECIMAL(10,2),
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
