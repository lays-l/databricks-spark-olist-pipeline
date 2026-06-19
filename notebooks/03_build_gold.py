# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Construção da camada Gold
# MAGIC
# MAGIC Cria tabelas analíticas prontas para consumo, eliminando a necessidade de joins repetidos
# MAGIC em ferramentas de BI ou análises ad-hoc.
# MAGIC
# MAGIC **Tabelas criadas:**
# MAGIC - `workspace.gold.fact_order_revenue` — tabela fato, 1 linha por pedido, particionada por data
# MAGIC - `workspace.gold.daily_revenue` — receita agregada por dia e estado
# MAGIC - `workspace.gold.customer_state_revenue` — receita, volume e taxa de atraso por estado
# MAGIC - `workspace.gold.product_category_revenue` — receita e volume por categoria
# MAGIC - `workspace.gold.seller_performance` — desempenho de vendas e entrega por seller
# MAGIC - `workspace.gold.payment_method_summary` — resumo por método de pagamento

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, countDistinct, sum, avg, round, when,
    row_number, collect_set, max as spark_max
)
from pyspark.sql.window import Window

from src.config import (
    SILVER_ORDERS, SILVER_ITEMS, SILVER_PAYMENTS,
    SILVER_CUSTOMERS, SILVER_PRODUCTS, SILVER_SELLERS,
    GOLD_FACT, GOLD_DAILY, GOLD_STATE,
    GOLD_CATEGORY, GOLD_SELLER, GOLD_PAYMENT
)

# COMMAND ----------
# MAGIC %md ## Leitura das tabelas Silver

# COMMAND ----------

orders   = spark.table(SILVER_ORDERS)
items    = spark.table(SILVER_ITEMS)
payments = spark.table(SILVER_PAYMENTS)
customers = spark.table(SILVER_CUSTOMERS)
products = spark.table(SILVER_PRODUCTS)
sellers  = spark.table(SILVER_SELLERS)

# COMMAND ----------
# MAGIC %md ## Agregação de pagamentos por pedido
# MAGIC
# MAGIC Um pedido pode ter múltiplas formas de pagamento (ex: cartão + voucher).
# MAGIC Agrupamos em uma linha por pedido e identificamos o método principal via window function
# MAGIC ordenada pelo maior valor pago — sem precisar de subquery aninhada.

# COMMAND ----------

# Window para identificar o pagamento de maior valor por pedido
w_payment = Window.partitionBy("order_id").orderBy(col("payment_value").desc())

main_payment = (
    payments
    .withColumn("rn", row_number().over(w_payment))
    .filter(col("rn") == 1)
    .select("order_id", col("payment_type").alias("main_payment_type"))
)

payments_agg = (
    payments
    .groupBy("order_id")
    .agg(
        round(sum("payment_value"), 2).alias("payment_total_value"),
        spark_max("payment_installments").alias("payment_installments_max"),
        collect_set("payment_type").alias("payment_types"),
        count("*").alias("payment_count")
    )
    .join(main_payment, "order_id", "left")
)

# COMMAND ----------
# MAGIC %md ## Agregação de itens por pedido
# MAGIC
# MAGIC Um pedido pode ter múltiplos itens de diferentes produtos e sellers.
# MAGIC Agrupamos para ter totais financeiros e contagens por pedido.

# COMMAND ----------

items_agg = (
    items
    .groupBy("order_id")
    .agg(
        count("*").alias("item_count"),
        countDistinct("product_id").alias("product_count"),
        countDistinct("seller_id").alias("seller_count"),
        round(sum("price"), 2).alias("items_total_value"),
        round(sum("freight_value"), 2).alias("freight_total_value"),
        round(sum("item_total_value"), 2).alias("order_total_value")
    )
)

# COMMAND ----------
# MAGIC %md ## Tabela fato: `gold.fact_order_revenue`
# MAGIC
# MAGIC Consolida em uma única linha por pedido todos os atributos relevantes para análise:
# MAGIC dados do cliente, status, datas de entrega, valores financeiros e métricas calculadas.
# MAGIC Particionada por `order_purchase_date` para otimizar queries por período.

# COMMAND ----------

fact = (
    orders
    .join(customers.select("customer_id", "customer_city", "customer_state"), "customer_id", "left")
    .join(payments_agg, "order_id", "left")
    .join(items_agg, "order_id", "left")
    .select(
        "order_id",
        "customer_id",
        "customer_city",
        "customer_state",
        "order_status",
        "order_purchase_timestamp",
        "order_purchase_date",
        "order_approved_at",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "delivery_days",
        "estimated_delivery_days",
        "is_delivered",
        "is_late",
        "payment_total_value",
        "payment_installments_max",
        "main_payment_type",
        "payment_count",
        "item_count",
        "product_count",
        "seller_count",
        "items_total_value",
        "freight_total_value",
        "order_total_value"
    )
)

(
    fact.write.format("delta")
    .mode("overwrite")
    .partitionBy("order_purchase_date")
    .saveAsTable(GOLD_FACT)
)
print(f"  ✓ {GOLD_FACT} — {fact.count()} linhas")

# COMMAND ----------
# MAGIC %md ## Tabela: `gold.daily_revenue`
# MAGIC
# MAGIC Agrega receita e métricas de entrega por dia e estado.
# MAGIC Responde perguntas como "qual foi a receita de SP em dezembro de 2017?".

# COMMAND ----------

daily = (
    fact
    .groupBy("order_purchase_date", "customer_state")
    .agg(
        count("order_id").alias("total_orders"),
        count(when(col("is_delivered") == True, 1)).alias("delivered_orders"),
        count(when(col("is_late") == True, 1)).alias("late_orders"),
        round(sum("payment_total_value"), 2).alias("total_revenue"),
        round(avg("payment_total_value"), 2).alias("avg_order_value"),
        round(avg("delivery_days"), 1).alias("avg_delivery_days")
    )
    .orderBy("order_purchase_date", "customer_state")
)

daily.write.format("delta").mode("overwrite").saveAsTable(GOLD_DAILY)
print(f"  ✓ {GOLD_DAILY} — {daily.count()} linhas")

# COMMAND ----------
# MAGIC %md ## Tabela: `gold.customer_state_revenue`
# MAGIC
# MAGIC Agrega receita por estado do cliente.
# MAGIC Responde: "Quais estados geram mais receita?" e "Onde estão os maiores atrasos?".
# MAGIC `late_rate` = proporção de pedidos entregues com atraso sobre o total entregue.

# COMMAND ----------

state_revenue = (
    fact
    .groupBy("customer_state")
    .agg(
        count("order_id").alias("total_orders"),
        count(when(col("is_delivered") == True, 1)).alias("delivered_orders"),
        count(when(col("is_late") == True, 1)).alias("late_orders"),
        round(sum("payment_total_value"), 2).alias("total_revenue"),
        round(avg("payment_total_value"), 2).alias("avg_order_value"),
        round(avg("delivery_days"), 1).alias("avg_delivery_days")
    )
    .withColumn(
        "late_rate",
        round(col("late_orders") / col("delivered_orders"), 4)
    )
    .orderBy(col("total_revenue").desc())
)

state_revenue.write.format("delta").mode("overwrite").saveAsTable(GOLD_STATE)
print(f"  ✓ {GOLD_STATE} — {state_revenue.count()} linhas")

# COMMAND ----------
# MAGIC %md ## Tabela: `gold.product_category_revenue`
# MAGIC
# MAGIC Agrega receita por categoria de produto (em inglês).
# MAGIC Responde: "Quais categorias vendem mais e geram mais receita?".

# COMMAND ----------

# Join entre itens, orders (para filtrar entregues) e produtos (para categoria)
category_revenue = (
    items
    .join(orders.select("order_id", "order_status"), "order_id", "left")
    .join(products.select("product_id", "product_category_name_english"), "product_id", "left")
    .groupBy("product_category_name_english")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        count("*").alias("total_items"),
        round(sum("price"), 2).alias("total_revenue"),
        round(avg("price"), 2).alias("avg_item_price")
    )
    .orderBy(col("total_revenue").desc())
)

category_revenue.write.format("delta").mode("overwrite").saveAsTable(GOLD_CATEGORY)
print(f"  ✓ {GOLD_CATEGORY} — {category_revenue.count()} linhas")

# COMMAND ----------
# MAGIC %md ## Tabela: `gold.seller_performance`
# MAGIC
# MAGIC Agrega métricas de vendas e entrega por seller.
# MAGIC Responde: "Quais sellers têm maior volume e menor taxa de atraso?".
# MAGIC Fonte: join entre `silver.order_items`, `silver.orders` e `silver.sellers`.

# COMMAND ----------

seller_orders = (
    items
    .join(
        orders.select("order_id", "delivery_days", "is_late", "is_delivered"),
        "order_id", "left"
    )
    .groupBy("seller_id")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        count("order_item_id").alias("total_items"),
        round(sum("price"), 2).alias("total_revenue"),
        round(avg("price"), 2).alias("avg_item_price"),
        round(avg("delivery_days"), 1).alias("avg_delivery_days"),
        count(when(col("is_late") == True, 1)).alias("late_order_count")
    )
    .withColumn(
        "late_rate",
        round(col("late_order_count") / col("total_orders"), 4)
    )
)

seller_perf = (
    seller_orders
    .join(sellers.select("seller_id", "seller_city", "seller_state"), "seller_id", "left")
    .select(
        "seller_id", "seller_city", "seller_state",
        "total_orders", "total_items", "total_revenue",
        "avg_item_price", "avg_delivery_days",
        "late_order_count", "late_rate"
    )
    .orderBy(col("total_revenue").desc())
)

seller_perf.write.format("delta").mode("overwrite").saveAsTable(GOLD_SELLER)
print(f"  ✓ {GOLD_SELLER} — {seller_perf.count()} linhas")

# COMMAND ----------
# MAGIC %md ## Tabela: `gold.payment_method_summary`
# MAGIC
# MAGIC Agrega totais por método de pagamento principal.
# MAGIC Responde: "Qual método é mais usado?" e "Pedidos parcelados têm ticket médio maior?".

# COMMAND ----------

payment_summary = (
    fact
    .groupBy("main_payment_type")
    .agg(
        count("order_id").alias("total_orders"),
        round(sum("payment_total_value"), 2).alias("total_revenue"),
        round(avg("payment_total_value"), 2).alias("avg_order_value"),
        round(avg("payment_installments_max"), 1).alias("avg_installments")
    )
    .orderBy(col("total_orders").desc())
)

payment_summary.write.format("delta").mode("overwrite").saveAsTable(GOLD_PAYMENT)
print(f"  ✓ {GOLD_PAYMENT} — {payment_summary.count()} linhas")

# COMMAND ----------
# MAGIC %md ## Resumo da camada Gold

# COMMAND ----------

gold_tables = [GOLD_FACT, GOLD_DAILY, GOLD_STATE, GOLD_CATEGORY, GOLD_SELLER, GOLD_PAYMENT]

print("\nResumo da camada Gold:\n")
for t in gold_tables:
    count_val = spark.table(t).count()
    print(f"  {t}: {count_val} registros")
