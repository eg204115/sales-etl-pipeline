\echo '=== 1. Customer order history WITH idx_sales_customer_date ==='
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT transaction_id, order_date, total_amount
FROM   sales
WHERE  customer_id = 'CUST-00042'
ORDER BY order_date DESC;

\echo '=== 1. Same query WITHOUT the index (dropped inside a transaction) ==='
BEGIN;
DROP INDEX idx_sales_customer_date;
DROP INDEX idx_sales_rated;
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT transaction_id, order_date, total_amount
FROM   sales
WHERE  customer_id = 'CUST-00042'
ORDER BY order_date DESC;
ROLLBACK;

\echo '=== 2. One week of revenue WITH idx_sales_order_date (covering) ==='
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT SUM(total_amount)
FROM   sales
WHERE  order_date BETWEEN DATE '2025-06-01' AND DATE '2025-06-07';

\echo '=== 2. Same query WITHOUT the index ==='
BEGIN;
DROP INDEX idx_sales_order_date;
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT SUM(total_amount)
FROM   sales
WHERE  order_date BETWEEN DATE '2025-06-01' AND DATE '2025-06-07';
ROLLBACK;

\echo '=== 3. Monthly revenue (whole table) - the planner may still prefer a Seq Scan ==='
\echo '    Reading 100% of a small table sequentially is cheaper than via an index.'
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT date_trunc('month', order_date) AS month, SUM(total_amount)
FROM   sales
GROUP BY 1
ORDER BY 1;
