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
# MAGIC O dataset Olist permite que um mesmo pedido seja pago com mais de uma forma —
# MAGIC por exemplo, parte no cartão de crédito e parte com voucher. Na `silver.payments`,
# MAGIC cada forma de pagamento é uma linha separada. Sem agregação, um join direto com
# MAGIC `orders` multiplicaria as linhas, fazendo o pedido aparecer mais de uma vez na fato.
# MAGIC
# MAGIC **`main_payment` (window function):** identifica o método principal — definido como
# MAGIC o de maior `payment_value`. A window ordena os pagamentos do pedido por valor desc
# MAGIC e seleciona `row_number() == 1`, sem precisar de subquery aninhada.
# MAGIC
# MAGIC **`payments_agg` (groupBy):** colapsa todas as linhas do pedido em uma, calculando:
# MAGIC - `payment_total_value` — soma de todos os pagamentos
# MAGIC - `payment_installments_max` — máximo de parcelas entre os métodos usados
# MAGIC - `payment_types` — array com todos os métodos (`["credit_card", "voucher"]`)
# MAGIC - `payment_count` — quantidade de formas de pagamento utilizadas

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
# MAGIC Na `silver.order_items`, cada item de um pedido é uma linha separada — um pedido com
# MAGIC 3 produtos tem 3 linhas. Sem agregação, o join com `orders` triplicaria esse pedido na fato.
# MAGIC
# MAGIC O groupBy consolida tudo em uma linha por pedido, calculando:
# MAGIC - `item_count` — total de itens (linhas) do pedido
# MAGIC - `product_count` — produtos distintos (usando `countDistinct`)
# MAGIC - `seller_count` — sellers distintos envolvidos no pedido
# MAGIC - `items_total_value` — soma dos preços dos produtos
# MAGIC - `freight_total_value` — soma dos fretes
# MAGIC - `order_total_value` — total do pedido (produto + frete), campo já calculado na Silver

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
# MAGIC Tabela central da camada Gold: consolida em **uma linha por pedido** todos os atributos
# MAGIC relevantes para análise, eliminando a necessidade de joins repetidos em queries analíticas.
# MAGIC
# MAGIC **Fontes:** `silver.orders` + `silver.customers` + `payments_agg` + `items_agg`
# MAGIC (as duas últimas já pré-agregadas nas etapas anteriores para garantir granularidade 1:1).
# MAGIC
# MAGIC **Particionamento por `order_purchase_date`:** quando uma query filtra por período
# MAGIC (ex: `WHERE order_purchase_date = '2018-01-01'`), o Spark lê apenas os arquivos Delta
# MAGIC da partição correspondente, ignorando todos os outros — isso é o **partition pruning**.
# MAGIC
# MAGIC **Campos calculados herdados da Silver:** `delivery_days`, `estimated_delivery_days`,
# MAGIC `is_delivered` e `is_late` já chegam prontos — a Gold apenas os expõe na fato.

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
# MAGIC Agrega receita e métricas de entrega por **dia × estado**, a partir da fato já construída.
# MAGIC Responde perguntas como "qual foi a receita de SP em dezembro de 2017?".
# MAGIC
# MAGIC - `total_orders` — total de pedidos no dia/estado
# MAGIC - `delivered_orders` — pedidos efetivamente entregues (`is_delivered = true`)
# MAGIC - `late_orders` — pedidos entregues com atraso (`is_late = true`)
# MAGIC - `avg_delivery_days` — média de dias para entrega (apenas pedidos com data de entrega)
# MAGIC
# MAGIC O `count(when(...))` é o padrão Spark para contar condicionalmente sem precisar de
# MAGIC subquery — equivalente ao `COUNT(CASE WHEN ... END)` do SQL.

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
# MAGIC Agrega receita por **estado do cliente**, direto da fato.
# MAGIC Responde: "Quais estados geram mais receita?" e "Onde estão os maiores atrasos?".
# MAGIC
# MAGIC - `late_rate` — proporção de pedidos com atraso sobre o total **entregue** (não sobre todos
# MAGIC   os pedidos). Usar `delivered_orders` no denominador evita distorção: pedidos ainda em
# MAGIC   trânsito têm `is_late = null` e não devem entrar no cálculo de taxa de atraso.
# MAGIC - `withColumn` pós-agg calcula o `late_rate` como coluna derivada, usando os alias já
# MAGIC   definidos no `agg` — isso só é possível após o `groupBy` resolver os nomes.

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
# MAGIC Agrega receita por **categoria de produto** (nome em inglês, normalizado na Silver).
# MAGIC Responde: "Quais categorias vendem mais e geram mais receita?".
# MAGIC
# MAGIC **Fonte:** `silver.order_items` (granularidade: 1 linha por item), enriquecida com a
# MAGIC categoria via join com `silver.products`. O join com `orders` traz o `order_status`,
# MAGIC permitindo filtros futuros por status se necessário.
# MAGIC
# MAGIC - `total_orders` usa `countDistinct("order_id")` — um pedido com 3 itens da mesma
# MAGIC   categoria conta como 1 pedido, não como 3.
# MAGIC - `total_items` usa `count("*")` — conta cada linha de item individualmente.
# MAGIC - `avg_item_price` é a média por item (não por pedido), refletindo o ticket médio
# MAGIC   de um produto dentro da categoria.

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
# MAGIC Agrega métricas de vendas e entrega por **seller**.
# MAGIC Responde: "Quais sellers têm maior volume e menor taxa de atraso?".
# MAGIC
# MAGIC **Estratégia de join em duas etapas:**
# MAGIC 1. `items` + `orders` → agrega métricas de desempenho por `seller_id`
# MAGIC 2. Resultado + `sellers` → enriquece com cidade e estado do seller
# MAGIC
# MAGIC Separar em duas etapas evita que o join com `orders` (grande) ocorra antes da agregação,
# MAGIC reduzindo o volume de dados trafegado no Spark.
# MAGIC
# MAGIC - `total_orders` usa `countDistinct("order_id")` — um pedido com 3 itens do mesmo
# MAGIC   seller conta como 1 pedido.
# MAGIC - `late_rate` = `late_order_count / total_orders` — proporção de pedidos com atraso
# MAGIC   sobre o total de pedidos do seller (inclui não entregues no denominador, diferente
# MAGIC   de `customer_state_revenue` onde usamos apenas entregues).

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
# MAGIC Agrega totais por **método de pagamento principal** (campo `main_payment_type` da fato,
# MAGIC definido na etapa de agregação de pagamentos como o método de maior valor do pedido).
# MAGIC Responde: "Qual método é mais usado?" e "Pedidos parcelados têm ticket médio maior?".
# MAGIC
# MAGIC - `avg_installments` — média do máximo de parcelas por pedido. Valores próximos de 1
# MAGIC   indicam pagamento à vista; valores altos indicam parcelamento recorrente.
# MAGIC - A comparação entre `avg_order_value` de `credit_card` vs `boleto` revela se clientes
# MAGIC   que parcelam tendem a comprar itens de maior valor — padrão comum em e-commerce brasileiro.

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
