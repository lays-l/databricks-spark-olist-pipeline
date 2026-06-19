# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Data Quality Checks
# MAGIC
# MAGIC Valida a integridade dos dados processados e registra os resultados em uma tabela de
# MAGIC auditoria. O objetivo não é apenas remover dados inválidos — é **registrar** os problemas
# MAGIC encontrados para permitir rastreabilidade, investigação e melhoria da origem dos dados.
# MAGIC
# MAGIC **Tabela de saída:** `workspace.gold.data_quality_summary`
# MAGIC
# MAGIC Cada linha representa o resultado de uma regra aplicada a uma tabela:
# MAGIC `table_name | rule_name | total_records | invalid_records | invalid_pct | status | checked_at`

# COMMAND ----------

from pyspark.sql.functions import col, count, when, current_timestamp, lit
# 'round' não é importado do Spark para não sobrescrever o built-in do Python,
# que é usado no cálculo do percentual de registros inválidos (operação sobre floats).
from datetime import datetime

from src.config import (
    SILVER_ORDERS, SILVER_ITEMS, SILVER_PAYMENTS,
    GOLD_FACT, GOLD_DQ_SUMMARY
)
from src.data_quality import (
    check_not_null, check_non_negative, check_valid_status,
    check_no_duplicates, VALID_ORDER_STATUSES
)

# COMMAND ----------
# MAGIC %md ## Função de verificação
# MAGIC
# MAGIC Centraliza a lógica de cada check: conta o total de registros, conta os inválidos
# MAGIC (registros que violam a regra) e calcula o percentual. O resultado é uma linha de auditoria.
# MAGIC
# MAGIC `invalid_expr` é uma coluna booleana que retorna `True` para registros que **violam** a regra.
# MAGIC Exemplo: `col("order_id").isNull()` marca como inválidos os pedidos sem ID.

# COMMAND ----------

results = []

def check(table_name: str, rule_name: str, df, invalid_df):
    """
    Executa uma regra de qualidade e armazena o resultado na lista de auditoria.

    table_name:  nome completo da tabela verificada
    rule_name:   descrição da regra (ex: "order_id not null")
    df:          DataFrame completo (para contar o total)
    invalid_df:  DataFrame filtrado retornado pelas funções de src.data_quality
                 (check_not_null, check_non_negative, check_valid_status, check_no_duplicates)
    """
    total   = df.count()
    invalid = invalid_df.count()
    pct     = round(invalid / total * 100, 4) if total > 0 else 0.0
    status  = "PASS" if invalid == 0 else "FAIL"

    results.append({
        "table_name":      table_name,
        "rule_name":       rule_name,
        "total_records":   total,
        "invalid_records": invalid,
        "invalid_pct":     pct,
        "status":          status,
        "checked_at":      datetime.now().isoformat()
    })
    print(f"  [{status}] {table_name} — {rule_name}: {invalid}/{total} inválidos ({pct}%)")

# COMMAND ----------
# MAGIC %md ## Verificações: `silver.orders`
# MAGIC
# MAGIC Regras mínimas de integridade para a tabela de pedidos:
# MAGIC - `order_id` não pode ser nulo — é a chave primária
# MAGIC - `customer_id` não pode ser nulo — pedido sem cliente não é rastreável
# MAGIC - `order_status` deve pertencer ao conjunto de valores válidos do Olist
# MAGIC - Grain: não deve haver `order_id` duplicado (1 pedido = 1 linha)

# COMMAND ----------

orders = spark.table(SILVER_ORDERS)

check(SILVER_ORDERS, "order_id not null",
      orders, check_not_null(orders, "order_id"))

check(SILVER_ORDERS, "customer_id not null",
      orders, check_not_null(orders, "customer_id"))

check(SILVER_ORDERS, "order_status valid",
      orders, check_valid_status(orders, "order_status", VALID_ORDER_STATUSES))

# check_no_duplicates retorna os grupos com mais de 1 ocorrência — cada linha = 1 order_id duplicado
check(SILVER_ORDERS, "order_id no duplicates",
      orders, check_no_duplicates(orders, "order_id"))

# COMMAND ----------
# MAGIC %md ## Verificações: `silver.order_payments`
# MAGIC
# MAGIC - `order_id` não pode ser nulo
# MAGIC - `payment_value` não pode ser negativo (frete grátis ou desconto total = 0, não negativo)
# MAGIC - `payment_installments` não pode ser negativo
# MAGIC - `payment_type` não pode ser nulo

# COMMAND ----------

payments = spark.table(SILVER_PAYMENTS)

check(SILVER_PAYMENTS, "order_id not null",
      payments, check_not_null(payments, "order_id"))

check(SILVER_PAYMENTS, "payment_value >= 0",
      payments, check_non_negative(payments, "payment_value"))

check(SILVER_PAYMENTS, "payment_installments >= 0",
      payments, check_non_negative(payments, "payment_installments"))

check(SILVER_PAYMENTS, "payment_type not null",
      payments, check_not_null(payments, "payment_type"))

# COMMAND ----------
# MAGIC %md ## Verificações: `silver.order_items`
# MAGIC
# MAGIC - `order_id`, `product_id` e `seller_id` não podem ser nulos
# MAGIC - `price` não pode ser negativo
# MAGIC - `freight_value` não pode ser negativo

# COMMAND ----------

items = spark.table(SILVER_ITEMS)

check(SILVER_ITEMS, "order_id not null",
      items, check_not_null(items, "order_id"))

check(SILVER_ITEMS, "product_id not null",
      items, check_not_null(items, "product_id"))

check(SILVER_ITEMS, "seller_id not null",
      items, check_not_null(items, "seller_id"))

check(SILVER_ITEMS, "price >= 0",
      items, check_non_negative(items, "price"))

check(SILVER_ITEMS, "freight_value >= 0",
      items, check_non_negative(items, "freight_value"))

# COMMAND ----------
# MAGIC %md ## Verificações: `gold.fact_order_revenue`
# MAGIC
# MAGIC Valida a consistência da tabela fato após os joins e agregações:
# MAGIC - O total de pedidos na fato deve bater com `silver.orders`
# MAGIC - Nenhum `order_id` deve ser nulo
# MAGIC - Nenhum pedido entregue deve ter `delivery_days` nulo (campo calculado na Silver)
# MAGIC - `payment_total_value` não deve ser negativo

# COMMAND ----------

fact = spark.table(GOLD_FACT)
orders_count = orders.count()
fact_count   = fact.count()

# Consistência de grain: fato deve ter o mesmo número de pedidos que a Silver
grain_status = "PASS" if fact_count == orders_count else "FAIL"
results.append({
    "table_name":      GOLD_FACT,
    "rule_name":       "grain matches silver.orders",
    "total_records":   fact_count,
    "invalid_records": abs(fact_count - orders_count),
    "invalid_pct":     0.0 if fact_count == orders_count else round(abs(fact_count - orders_count) / orders_count * 100, 4),
    "status":          grain_status,
    "checked_at":      datetime.now().isoformat()
})
print(f"  [{grain_status}] {GOLD_FACT} — grain matches silver.orders: fact={fact_count}, silver={orders_count}")

check(GOLD_FACT, "order_id not null",
      fact, check_not_null(fact, "order_id"))

check(GOLD_FACT, "delivered orders have delivery_days",
      fact, fact.filter((col("is_delivered") == True) & col("delivery_days").isNull()))

check(GOLD_FACT, "payment_total_value >= 0",
      fact, check_non_negative(fact, "payment_total_value"))

# COMMAND ----------
# MAGIC %md ## Salvar resultado na tabela de auditoria
# MAGIC
# MAGIC Converte a lista de resultados em DataFrame e salva em `gold.data_quality_summary`.
# MAGIC O modo `overwrite` reescreve o resumo a cada execução — em produção, `append` preservaria
# MAGIC o histórico de execuções anteriores para análise de tendências de qualidade ao longo do tempo.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

dq_schema = StructType([
    StructField("table_name",      StringType(), True),
    StructField("rule_name",       StringType(), True),
    StructField("total_records",   LongType(),   True),
    StructField("invalid_records", LongType(),   True),
    StructField("invalid_pct",     DoubleType(), True),
    StructField("status",          StringType(), True),
    StructField("checked_at",      StringType(), True),
])

dq_df = spark.createDataFrame(results, schema=dq_schema)
dq_df.write.format("delta").mode("overwrite").saveAsTable(GOLD_DQ_SUMMARY)

print(f"\n  ✓ {GOLD_DQ_SUMMARY} — {dq_df.count()} regras verificadas")

# COMMAND ----------
# MAGIC %md ## Resumo

# COMMAND ----------

print("\n--- Resultado das verificações de qualidade ---\n")
passes = sum(1 for r in results if r["status"] == "PASS")
fails  = sum(1 for r in results if r["status"] == "FAIL")
print(f"  PASS: {passes}")
print(f"  FAIL: {fails}")

if fails > 0:
    print("\n  Regras que falharam:")
    for r in results:
        if r["status"] == "FAIL":
            print(f"    - [{r['table_name']}] {r['rule_name']}: {r['invalid_records']} registros inválidos")
