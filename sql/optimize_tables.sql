-- Compacta arquivos pequenos gerados por cargas incrementais (MERGE)
OPTIMIZE workspace.gold.fact_order_revenue;
OPTIMIZE workspace.gold.daily_revenue;
OPTIMIZE workspace.gold.product_category_revenue;
OPTIMIZE workspace.gold.seller_performance;
OPTIMIZE workspace.gold.customer_state_revenue;

-- ZORDER reorganiza os dados fisicamente para melhorar data skipping
-- Escolha justificada: queries analíticas filtram frequentemente por customer_state e order_status
-- Não usar ZORDER em colunas de alta cardinalidade como order_id (raramente filtrado)
OPTIMIZE workspace.gold.fact_order_revenue
ZORDER BY (customer_state, order_status);
