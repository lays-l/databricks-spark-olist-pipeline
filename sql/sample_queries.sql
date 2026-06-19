-- =============================================================================
-- Olist E-Commerce — Queries analíticas de exemplo
-- Tabelas fonte: workspace.gold.*
-- Rodar no Databricks SQL Editor (catálogo: workspace)
-- =============================================================================

-- =============================================================================
-- 1. Receita diária por estado
-- Responde: "Qual foi a receita ao longo do tempo e por região?"
-- =============================================================================
SELECT
    order_purchase_date,
    customer_state,
    COUNT(DISTINCT order_id)        AS total_orders,
    SUM(payment_total_value)        AS total_revenue,
    AVG(payment_total_value)        AS avg_order_value
FROM workspace.gold.fact_order_revenue
GROUP BY order_purchase_date, customer_state
ORDER BY order_purchase_date, total_revenue DESC;


-- =============================================================================
-- 2. Receita total por estado (ranking)
-- Responde: "Quais estados geram mais receita?"
-- =============================================================================
SELECT
    customer_state,
    total_orders,
    delivered_orders,
    total_revenue,
    avg_order_value,
    avg_delivery_days,
    late_rate
FROM workspace.gold.customer_state_revenue
ORDER BY total_revenue DESC;


-- =============================================================================
-- 3. Taxa de atraso por estado
-- Responde: "Onde estão os maiores problemas de entrega?"
-- Nota: is_late = null para pedidos não entregues — filtro WHERE is_delivered = true
-- garante que apenas pedidos concluídos entram no cálculo da taxa
-- =============================================================================
SELECT
    customer_state,
    delivered_orders,
    late_orders,
    late_rate,
    avg_delivery_days
FROM workspace.gold.customer_state_revenue
ORDER BY late_rate DESC;


-- =============================================================================
-- 4. Tempo médio de entrega por estado
-- Responde: "Qual o tempo médio de entrega por região?"
-- =============================================================================
SELECT
    customer_state,
    AVG(delivery_days)              AS avg_delivery_days,
    MIN(delivery_days)              AS min_delivery_days,
    MAX(delivery_days)              AS max_delivery_days,
    COUNT(order_id)                 AS delivered_orders
FROM workspace.gold.fact_order_revenue
WHERE is_delivered = true
GROUP BY customer_state
ORDER BY avg_delivery_days DESC;


-- =============================================================================
-- 5. Pedidos entregues com e sem atraso
-- Responde: "Qual proporção dos pedidos foi entregue no prazo?"
-- =============================================================================
SELECT
    is_late,
    COUNT(order_id)                 AS total_orders,
    ROUND(AVG(delivery_days), 1)    AS avg_delivery_days
FROM workspace.gold.fact_order_revenue
WHERE is_delivered = true
GROUP BY is_late
ORDER BY is_late;


-- =============================================================================
-- 6. Top 10 categorias por receita
-- Responde: "Quais categorias vendem mais?"
-- =============================================================================
SELECT
    product_category_name_english,
    total_orders,
    total_items,
    total_revenue,
    avg_item_price
FROM workspace.gold.product_category_revenue
ORDER BY total_revenue DESC
LIMIT 10;


-- =============================================================================
-- 7. Receita e volume por método de pagamento
-- Responde: "Qual método de pagamento é mais utilizado?"
-- =============================================================================
SELECT
    main_payment_type,
    total_orders,
    total_revenue,
    avg_order_value,
    avg_installments
FROM workspace.gold.payment_method_summary
ORDER BY total_orders DESC;


-- =============================================================================
-- 8. Pedidos parcelados vs à vista — ticket médio
-- Responde: "Pedidos parcelados têm ticket médio maior?"
-- Compara credit_card (parcelado) com boleto e voucher (geralmente à vista)
-- =============================================================================
SELECT
    main_payment_type,
    avg_order_value,
    avg_installments,
    CASE
        WHEN avg_installments > 1.5 THEN 'parcelado'
        ELSE 'à vista'
    END AS modalidade
FROM workspace.gold.payment_method_summary
ORDER BY avg_order_value DESC;


-- =============================================================================
-- 9. Top 10 sellers por receita
-- Responde: "Quais sellers têm maior volume de vendas?"
-- =============================================================================
SELECT
    seller_id,
    seller_state,
    total_orders,
    total_items,
    total_revenue,
    avg_item_price,
    avg_delivery_days,
    late_rate
FROM workspace.gold.seller_performance
ORDER BY total_revenue DESC
LIMIT 10;


-- =============================================================================
-- 10. Sellers com maior taxa de atraso (mínimo 50 pedidos)
-- Responde: "Quais sellers têm piores índices de entrega?"
-- Filtro de mínimo 50 pedidos evita distorção por sellers com poucos pedidos
-- =============================================================================
SELECT
    seller_id,
    seller_state,
    total_orders,
    late_order_count,
    late_rate,
    avg_delivery_days
FROM workspace.gold.seller_performance
WHERE total_orders >= 50
ORDER BY late_rate DESC
LIMIT 10;


-- =============================================================================
-- 11. Evolução mensal de receita
-- Responde: "Como a receita evoluiu ao longo dos meses?"
-- =============================================================================
SELECT
    DATE_TRUNC('month', order_purchase_date)    AS month,
    COUNT(DISTINCT order_id)                    AS total_orders,
    SUM(payment_total_value)                    AS total_revenue,
    AVG(payment_total_value)                    AS avg_order_value
FROM workspace.gold.fact_order_revenue
GROUP BY DATE_TRUNC('month', order_purchase_date)
ORDER BY month;


-- =============================================================================
-- 12. Distribuição de pedidos por status
-- Visão geral da base: quantos pedidos em cada etapa do ciclo de vida
-- =============================================================================
SELECT
    order_status,
    COUNT(order_id)                 AS total_orders,
    ROUND(COUNT(order_id) * 100.0 / SUM(COUNT(order_id)) OVER (), 2) AS pct
FROM workspace.gold.fact_order_revenue
GROUP BY order_status
ORDER BY total_orders DESC;


-- =============================================================================
-- 13. Resultado das verificações de qualidade (04_data_quality_checks.py)
-- Visão geral do pipeline de auditoria: quais regras passaram e quais falharam
-- =============================================================================
SELECT
    table_name,
    rule_name,
    total_records,
    invalid_records,
    invalid_pct,
    status,
    checked_at
FROM workspace.gold.data_quality_summary
ORDER BY table_name, rule_name;


-- =============================================================================
-- 14. Regras com falha — detalhamento para investigação
-- =============================================================================
SELECT
    table_name,
    rule_name,
    invalid_records,
    invalid_pct
FROM workspace.gold.data_quality_summary
WHERE status = 'FAIL'
ORDER BY invalid_records DESC;
