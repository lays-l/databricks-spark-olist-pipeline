-- Receita diária por estado
SELECT
    order_purchase_date,
    customer_state,
    SUM(payment_total_value)    AS total_revenue,
    COUNT(DISTINCT order_id)    AS total_orders
FROM workspace.gold.fact_order_revenue
GROUP BY order_purchase_date, customer_state
ORDER BY order_purchase_date, total_revenue DESC;

-- Taxa de atraso por estado
-- is_late = null para pedidos não entregues → filtro WHERE is_delivered = true garante
-- que apenas pedidos concluídos entram no cálculo
SELECT
    customer_state,
    COUNT(*)                                              AS total_orders,
    SUM(CASE WHEN is_late THEN 1 ELSE 0 END)             AS late_orders,
    SUM(CASE WHEN is_late THEN 1 ELSE 0 END) / COUNT(*)  AS late_rate
FROM workspace.gold.fact_order_revenue
WHERE is_delivered = true
GROUP BY customer_state
ORDER BY late_rate DESC;

-- Receita por método de pagamento
SELECT
    main_payment_type,
    COUNT(DISTINCT order_id)    AS total_orders,
    SUM(payment_total_value)    AS total_revenue,
    AVG(payment_total_value)    AS avg_order_value
FROM workspace.gold.fact_order_revenue
GROUP BY main_payment_type
ORDER BY total_revenue DESC;

-- Top 10 sellers por receita
SELECT
    seller_id,
    seller_state,
    total_orders,
    total_revenue,
    late_rate
FROM workspace.gold.seller_performance
ORDER BY total_revenue DESC
LIMIT 10;

-- Receita por estado
SELECT
    customer_state,
    total_orders,
    total_revenue,
    avg_order_value,
    late_rate
FROM workspace.gold.customer_state_revenue
ORDER BY total_revenue DESC;
