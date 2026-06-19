# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Transformação Silver
# MAGIC
# MAGIC **Objetivo:** Criar tabelas limpas, tipadas e padronizadas a partir da Bronze.
# MAGIC
# MAGIC A camada Silver é onde aplicamos regras de negócio e qualidade de dados.
# MAGIC Registros inválidos são isolados em tabelas dedicadas para auditoria — não descartados.
# MAGIC
# MAGIC **Tabelas criadas:**
# MAGIC - `workspace.silver.orders` + `workspace.silver.invalid_orders`
# MAGIC - `workspace.silver.order_items`
# MAGIC - `workspace.silver.payments` + `workspace.silver.invalid_payments`
# MAGIC - `workspace.silver.customers`
# MAGIC - `workspace.silver.products` (com broadcast join para tradução de categorias)
# MAGIC - `workspace.silver.sellers`
# MAGIC - `workspace.silver.reviews`

# COMMAND ----------

from pyspark.sql.functions import (
    col, to_timestamp, to_date, datediff,
    when, lower, trim, coalesce, lit,
    broadcast
)
from src.config import (
    BRONZE_ORDERS, BRONZE_ITEMS, BRONZE_PAYMENTS, BRONZE_CUSTOMERS,
    BRONZE_PRODUCTS, BRONZE_SELLERS, BRONZE_REVIEWS, BRONZE_CATEGORY,
    SILVER_ORDERS, SILVER_ITEMS, SILVER_PAYMENTS, SILVER_CUSTOMERS,
    SILVER_PRODUCTS, SILVER_SELLERS, SILVER_REVIEWS
)

SILVER_INVALID_ORDERS   = "workspace.silver.invalid_orders"
SILVER_INVALID_PAYMENTS = "workspace.silver.invalid_payments"

VALID_ORDER_STATUSES = [
    "created", "approved", "invoiced", "processing",
    "shipped", "delivered", "unavailable", "canceled"
]

# COMMAND ----------
# MAGIC %md ## silver.orders

# COMMAND ----------

# MAGIC %md
# MAGIC **Transformações aplicadas:**
# MAGIC - Conversão de datas de StringType para TimestampType
# MAGIC - Padronização de order_status para lowercase
# MAGIC - Criação de campos calculados: order_purchase_date, delivery_days, estimated_delivery_days
# MAGIC - is_delivered: True se a data de entrega está preenchida
# MAGIC - is_late: True/False apenas para pedidos entregues — null para não entregues
# MAGIC   (usar False para pedidos em trânsito mascararia a taxa de atraso real)

# COMMAND ----------

orders_raw = spark.table(BRONZE_ORDERS)

orders_clean = (
    orders_raw
    .withColumn("order_purchase_timestamp",       to_timestamp("order_purchase_timestamp"))
    .withColumn("order_approved_at",              to_timestamp("order_approved_at"))
    .withColumn("order_delivered_carrier_date",   to_timestamp("order_delivered_carrier_date"))
    .withColumn("order_delivered_customer_date",  to_timestamp("order_delivered_customer_date"))
    .withColumn("order_estimated_delivery_date",  to_timestamp("order_estimated_delivery_date"))
    .withColumn("order_status",                   lower(trim(col("order_status"))))
    .withColumn("order_purchase_date",            to_date("order_purchase_timestamp"))
    .withColumn(
        "delivery_days",
        datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))
    )
    .withColumn(
        "estimated_delivery_days",
        datediff(col("order_estimated_delivery_date"), col("order_purchase_timestamp"))
    )
    .withColumn(
        "is_delivered",
        when(col("order_delivered_customer_date").isNotNull(), True).otherwise(False)
    )
    .withColumn(
        "is_late",
        when(
            col("is_delivered") == True,
            when(
                col("order_delivered_customer_date") > col("order_estimated_delivery_date"),
                True
            ).otherwise(False)
        )
        # null para pedidos não entregues — is_late não é aplicável a pedidos em trânsito
    )
    .select(
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_purchase_date",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "delivery_days",
        "estimated_delivery_days",
        "is_delivered",
        "is_late",
        "ingestion_timestamp",
        "ingestion_date",
        "source_file",
    )
)

# Separar registros inválidos antes de salvar a Silver
invalid_orders = orders_clean.filter(
    col("order_id").isNull() |
    col("customer_id").isNull() |
    col("order_purchase_timestamp").isNull() |
    ~col("order_status").isin(VALID_ORDER_STATUSES)
)

valid_orders = orders_clean.filter(
    col("order_id").isNotNull() &
    col("customer_id").isNotNull() &
    col("order_purchase_timestamp").isNotNull() &
    col("order_status").isin(VALID_ORDER_STATUSES)
)

valid_orders.write.format("delta").mode("overwrite").saveAsTable(SILVER_ORDERS)
invalid_orders.write.format("delta").mode("overwrite").saveAsTable(SILVER_INVALID_ORDERS)

print(f"  ✓ {SILVER_ORDERS}: {valid_orders.count()} registros válidos")
print(f"  ⚠ {SILVER_INVALID_ORDERS}: {invalid_orders.count()} registros inválidos isolados")

# COMMAND ----------
# MAGIC %md ## silver.payments

# COMMAND ----------

payments_raw = spark.table(BRONZE_PAYMENTS)

payments_clean = (
    payments_raw
    .withColumn("payment_value",        col("payment_value").cast("double"))
    .withColumn("payment_installments", col("payment_installments").cast("integer"))
    .withColumn("payment_type",         lower(trim(col("payment_type"))))
    .withColumn("is_credit_card",       when(col("payment_type") == "credit_card", True).otherwise(False))
    .withColumn("is_installment",       when(col("payment_installments") > 1, True).otherwise(False))
    .select(
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
        "is_credit_card",
        "is_installment",
        "ingestion_timestamp",
        "ingestion_date",
        "source_file",
    )
)

invalid_payments = payments_clean.filter(
    col("order_id").isNull() |
    (col("payment_value") < 0) |
    col("payment_type").isNull()
)

valid_payments = payments_clean.filter(
    col("order_id").isNotNull() &
    (col("payment_value") >= 0) &
    col("payment_type").isNotNull()
)

valid_payments.write.format("delta").mode("overwrite").saveAsTable(SILVER_PAYMENTS)
invalid_payments.write.format("delta").mode("overwrite").saveAsTable(SILVER_INVALID_PAYMENTS)

print(f"  ✓ {SILVER_PAYMENTS}: {valid_payments.count()} registros válidos")
print(f"  ⚠ {SILVER_INVALID_PAYMENTS}: {invalid_payments.count()} registros inválidos isolados")

# COMMAND ----------
# MAGIC %md ## silver.order_items

# COMMAND ----------

items_raw = spark.table(BRONZE_ITEMS)

items_clean = (
    items_raw
    .withColumn("shipping_limit_date", to_timestamp("shipping_limit_date"))
    .withColumn("price",         col("price").cast("double"))
    .withColumn("freight_value", col("freight_value").cast("double"))
    # No dataset atual, freight_value nunca é nulo nem zero (verificado na exploração).
    # O coalesce garante robustez caso fontes futuras enviem null para frete grátis.
    # Bronze preserva o dado bruto — a decisão de tratar null como 0.0 é da Silver.
    .withColumn("freight_value",    coalesce(col("freight_value"), lit(0.0)))
    .withColumn("item_total_value", col("price") + col("freight_value"))
    .filter(
        col("order_id").isNotNull() &
        col("product_id").isNotNull() &
        col("seller_id").isNotNull() &
        (col("price") >= 0) &
        (col("freight_value") >= 0)
    )
    .select(
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
        "item_total_value",
        "ingestion_timestamp",
        "ingestion_date",
        "source_file",
    )
)

items_clean.write.format("delta").mode("overwrite").saveAsTable(SILVER_ITEMS)
print(f"  ✓ {SILVER_ITEMS}: {items_clean.count()} registros")

# COMMAND ----------
# MAGIC %md ## silver.customers

# COMMAND ----------

customers_raw = spark.table(BRONZE_CUSTOMERS)

customers_clean = (
    customers_raw
    .withColumn("customer_city",  lower(trim(col("customer_city"))))
    .withColumn("customer_state", trim(col("customer_state")))
    .filter(col("customer_id").isNotNull())
    .select(
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
        "ingestion_timestamp",
        "ingestion_date",
        "source_file",
    )
)

customers_clean.write.format("delta").mode("overwrite").saveAsTable(SILVER_CUSTOMERS)
print(f"  ✓ {SILVER_CUSTOMERS}: {customers_clean.count()} registros")

# COMMAND ----------
# MAGIC %md ## silver.products
# MAGIC
# MAGIC **Decisão técnica — Broadcast join:**
# MAGIC A tabela de tradução de categorias tem apenas 71 registros e cabe inteiramente em memória.
# MAGIC Ao usar `broadcast()`, o Spark envia uma cópia dessa tabela para cada executor,
# MAGIC evitando o shuffle da tabela grande de produtos. Sem broadcast, ambas as tabelas
# MAGIC precisariam ser redistribuídas pela rede — muito mais custoso.

# COMMAND ----------

products_raw = spark.table(BRONZE_PRODUCTS)

# Seleciona apenas as colunas de negócio da tabela de tradução antes do join.
# A tabela de tradução também possui colunas de metadados de ingestão
# (ingestion_date, ingestion_timestamp, source_file) herdadas da Bronze.
# Sem esse select, o join resultaria em colunas duplicadas e o Delta rejeitaria a escrita.
category_filtered_raw = spark.table(BRONZE_CATEGORY).select(
    "product_category_name",
    "product_category_name_english"
)

products_clean = (
    products_raw
    .join(broadcast(category_filtered_raw), "product_category_name", "left")
    .withColumn(
        "product_category_name_english",
        coalesce(col("product_category_name_english"), lit("unknown"))
    )
    .filter(col("product_id").isNotNull())
    .select(
        "product_id",
        "product_category_name",
        "product_category_name_english",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "ingestion_timestamp",
        "ingestion_date",
        "source_file",
    )
)

products_clean.write.format("delta").mode("overwrite").saveAsTable(SILVER_PRODUCTS)
print(f"  ✓ {SILVER_PRODUCTS}: {products_clean.count()} registros")

# COMMAND ----------
# MAGIC %md ## silver.sellers

# COMMAND ----------

sellers_raw = spark.table(BRONZE_SELLERS)

sellers_clean = (
    sellers_raw
    .withColumn("seller_city",  lower(trim(col("seller_city"))))
    .withColumn("seller_state", trim(col("seller_state")))
    .filter(col("seller_id").isNotNull())
    .select(
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
        "ingestion_timestamp",
        "ingestion_date",
        "source_file",
    )
)

sellers_clean.write.format("delta").mode("overwrite").saveAsTable(SILVER_SELLERS)
print(f"  ✓ {SILVER_SELLERS}: {sellers_clean.count()} registros")

# COMMAND ----------
# MAGIC %md ## silver.reviews

# COMMAND ----------

reviews_raw = spark.table(BRONZE_REVIEWS)

reviews_clean = (
    reviews_raw
    .withColumn("review_creation_date",    to_timestamp("review_creation_date"))
    .withColumn("review_answer_timestamp", to_timestamp("review_answer_timestamp"))
    .filter(
        col("review_id").isNotNull() &
        col("order_id").isNotNull()
    )
    .select(
        "review_id",
        "order_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_answer_timestamp",
        "ingestion_timestamp",
        "ingestion_date",
        "source_file",
    )
)

reviews_clean.write.format("delta").mode("overwrite").saveAsTable(SILVER_REVIEWS)
print(f"  ✓ {SILVER_REVIEWS}: {reviews_clean.count()} registros")

# COMMAND ----------
# MAGIC %md ## Validação final

# COMMAND ----------

print("\nResumo da camada Silver:\n")
silver_tables = [
    SILVER_ORDERS, SILVER_ITEMS, SILVER_PAYMENTS, SILVER_CUSTOMERS,
    SILVER_PRODUCTS, SILVER_SELLERS, SILVER_REVIEWS,
    SILVER_INVALID_ORDERS, SILVER_INVALID_PAYMENTS
]
for t in silver_tables:
    count = spark.table(t).count()
    print(f"  {t}: {count} registros")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pontos de discussão
# MAGIC
# MAGIC - **Select explícito em todas as tabelas:** define a ordem e o contrato de colunas de cada tabela.
# MAGIC   Torna o schema previsível, facilita revisão de código e evita que colunas indesejadas
# MAGIC   ou duplicadas viajem para a Silver sem intenção.
# MAGIC - **Separação de inválidos:** registros com order_id nulo ou status inválido não são descartados —
# MAGIC   ficam em `silver.invalid_orders` para auditoria e investigação da origem do problema.
# MAGIC - **is_late como null:** pedidos não entregues recebem `null` em `is_late`, não `False`.
# MAGIC   Isso evita que pedidos em trânsito distorçam análises de taxa de atraso.
# MAGIC - **Broadcast join:** tabela de tradução de categorias (71 registros) é enviada para todos
# MAGIC   os executores, evitando shuffle da tabela grande de produtos.
# MAGIC - **Datas como StringType na Bronze:** a conversão para TimestampType acontece aqui na Silver,
# MAGIC   garantindo que o dado bruto seja preservado exatamente como veio da fonte.
