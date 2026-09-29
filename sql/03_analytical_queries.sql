-- name: top_10_categories_by_revenue
SELECT  p.category,
        COUNT(*)                           AS orders,
        SUM(s.quantity)                    AS units_sold,
        SUM(s.total_amount)                AS revenue,
        ROUND(AVG(s.total_amount), 2)      AS avg_order_value,
        ROUND(100.0 * SUM(s.total_amount)
              / SUM(SUM(s.total_amount)) OVER (), 2) AS revenue_share_pct
FROM    sales s
JOIN    products p ON p.product_id = s.product_id
GROUP BY p.category
ORDER BY revenue DESC
LIMIT 10;

-- name: monthly_revenue_growth
WITH monthly AS (
    SELECT  date_trunc('month', order_date)::date AS month,
            COUNT(*)                              AS orders,
            SUM(total_amount)                     AS revenue
    FROM    sales
    GROUP BY 1
)
SELECT  month,
        orders,
        revenue,
        LAG(revenue) OVER (ORDER BY month)                         AS prev_month_revenue,
        ROUND(100.0 * (revenue - LAG(revenue) OVER (ORDER BY month))
              / NULLIF(LAG(revenue) OVER (ORDER BY month), 0), 2)  AS mom_growth_pct,
        ROUND(100.0 * (revenue - LAG(revenue, 12) OVER (ORDER BY month))
              / NULLIF(LAG(revenue, 12) OVER (ORDER BY month), 0), 2) AS yoy_growth_pct
FROM    monthly
ORDER BY month;

-- name: average_rating_by_country
SELECT  c.country,
        COUNT(*)                         AS rated_orders,
        ROUND(AVG(s.rating), 2)          AS avg_rating,
        ROUND(100.0 * COUNT(*) FILTER (WHERE s.rating >= 4) / COUNT(*), 1) AS pct_positive
FROM    sales s
JOIN    customers c ON c.customer_id = s.customer_id
WHERE   s.rating IS NOT NULL
GROUP BY c.country
HAVING  COUNT(*) >= 30
ORDER BY avg_rating DESC;

-- name: top_customers_lifetime_value
SELECT  c.customer_id,
        c.customer_name,
        c.country,
        COUNT(*)                         AS orders,
        SUM(s.total_amount)              AS lifetime_value,
        MIN(s.order_date)                AS first_order,
        MAX(s.order_date)                AS last_order
FROM    sales s
JOIN    customers c ON c.customer_id = s.customer_id
GROUP BY c.customer_id, c.customer_name, c.country
ORDER BY lifetime_value DESC
LIMIT 10;

-- name: payment_mix_last_quarter
SELECT  payment_method,
        COUNT(*)                                            AS orders,
        SUM(total_amount)                                   AS revenue,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)  AS order_share_pct
FROM    sales
WHERE   order_date >= (SELECT MAX(order_date) FROM sales) - INTERVAL '3 months'
GROUP BY payment_method
ORDER BY revenue DESC;

-- name: data_quality_summary
SELECT  reason,
        COUNT(*) AS rejected_rows
FROM    etl_rejected_records r
CROSS JOIN LATERAL unnest(string_to_array(r.reject_reasons, ';')) AS reason
WHERE   r.run_id = (SELECT run_id FROM etl_runs
                    WHERE status = 'success' ORDER BY started_at DESC LIMIT 1)
GROUP BY reason
ORDER BY rejected_rows DESC;
