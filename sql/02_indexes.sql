CREATE INDEX IF NOT EXISTS idx_sales_order_date
    ON sales (order_date) INCLUDE (total_amount, product_id);

CREATE INDEX IF NOT EXISTS idx_sales_customer_date
    ON sales (customer_id, order_date DESC);

CREATE INDEX IF NOT EXISTS idx_sales_product_date
    ON sales (product_id, order_date) INCLUDE (total_amount);

CREATE INDEX IF NOT EXISTS idx_sales_rated
    ON sales (customer_id) INCLUDE (rating)
    WHERE rating IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_products_category ON products (category);
CREATE INDEX IF NOT EXISTS idx_customers_country ON customers (country);

CREATE INDEX IF NOT EXISTS idx_rejected_run_id ON etl_rejected_records (run_id);
CREATE INDEX IF NOT EXISTS idx_etl_runs_started ON etl_runs (started_at DESC);

ANALYZE customers;
ANALYZE products;
ANALYZE sales;
