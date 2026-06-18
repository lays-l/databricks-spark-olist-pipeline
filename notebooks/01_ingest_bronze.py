# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Ingestão Bronze
# MAGIC
# MAGIC **Objetivo:** Ler os CSVs brutos da Olist e salvar como Delta Tables na camada Bronze.
# MAGIC
# MAGIC A camada Bronze preserva os dados exatamente como vieram da fonte.
# MAGIC A única transformação aplicada é a adição de metadados de rastreabilidade:
# MAGIC - `ingestion_timestamp`: quando o dado foi ingerido
# MAGIC - `ingestion_date`: data da ingestão (usada para particionamento futuro)
# MAGIC - `source_file`: caminho do arquivo de origem
# MAGIC
# MAGIC **Decisão técnica — schemas explícitos vs inferSchema:**
# MAGIC `inferSchema=True` realiza dois scans do arquivo (um para inferir, outro para ler),
# MAGIC é mais lento e pode inferir tipos incorretos (ex: order_id como Integer).
# MAGIC Schemas explícitos definidos em `src/schemas.py` tornam o pipeline previsível.
# MAGIC Datas ficam como StringType na Bronze — a conversão é responsabilidade da Silver.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, current_date, col
from src.config import RAW_DATA_PATH, SOURCE_FILES
from src.config import (
    BRONZE_ORDERS, BRONZE_ITEMS, BRONZE_PAYMENTS, BRONZE_CUSTOMERS,
    BRONZE_PRODUCTS, BRONZE_SELLERS, BRONZE_REVIEWS, BRONZE_CATEGORY
)
from src.schemas import (
    ORDERS_SCHEMA, ORDER_ITEMS_SCHEMA, PAYMENTS_SCHEMA, CUSTOMERS_SCHEMA,
    PRODUCTS_SCHEMA, SELLERS_SCHEMA, REVIEWS_SCHEMA, CATEGORY_TRANSLATION_SCHEMA
)

# COMMAND ----------
# MAGIC %md ## Função de ingestão

# COMMAND ----------

def ingest_csv_to_bronze(file_name: str, schema, table_name: str):
    """
    Lê um CSV do Volume com schema explícito, adiciona metadados de ingestão
    e salva como Delta Table na camada Bronze.
    """
    path = f"{RAW_DATA_PATH}/{file_name}"

    df = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(path)
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("ingestion_date", current_date())
        # input_file_name() não é suportado no Unity Catalog
        # _metadata.file_path é a alternativa nativa para ambientes com UC habilitado
        .withColumn("source_file", col("_metadata.file_path"))
    )

    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    print(f"  ✓ {table_name} — {df.count()} linhas ingeridas")
    return df

# COMMAND ----------
# MAGIC %md ## Ingestão de todas as tabelas

# COMMAND ----------

print("Iniciando ingestão Bronze...\n")

ingest_csv_to_bronze(SOURCE_FILES["orders"],             ORDERS_SCHEMA,              BRONZE_ORDERS)
ingest_csv_to_bronze(SOURCE_FILES["order_items"],        ORDER_ITEMS_SCHEMA,         BRONZE_ITEMS)
ingest_csv_to_bronze(SOURCE_FILES["payments"],           PAYMENTS_SCHEMA,            BRONZE_PAYMENTS)
ingest_csv_to_bronze(SOURCE_FILES["customers"],          CUSTOMERS_SCHEMA,           BRONZE_CUSTOMERS)
ingest_csv_to_bronze(SOURCE_FILES["products"],           PRODUCTS_SCHEMA,            BRONZE_PRODUCTS)
ingest_csv_to_bronze(SOURCE_FILES["sellers"],            SELLERS_SCHEMA,             BRONZE_SELLERS)
ingest_csv_to_bronze(SOURCE_FILES["reviews"],            REVIEWS_SCHEMA,             BRONZE_REVIEWS)
ingest_csv_to_bronze(SOURCE_FILES["category_translation"], CATEGORY_TRANSLATION_SCHEMA, BRONZE_CATEGORY)

print("\nIngestão Bronze concluída.")

# COMMAND ----------
# MAGIC %md ## Validação rápida

# COMMAND ----------

spark.sql(f"SELECT COUNT(*) AS total FROM {BRONZE_ORDERS}").show()
spark.sql(f"DESCRIBE DETAIL {BRONZE_ORDERS}").select("format", "numFiles", "sizeInBytes").show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pontos de discussão
# MAGIC
# MAGIC - **Bronze preserva o dado bruto:** nenhuma regra de negócio é aplicada.
# MAGIC   Isso permite auditoria e reprocessamento quando regras mudam.
# MAGIC - **Schemas explícitos:** evitam inferência dupla e tipos incorretos.
# MAGIC   Datas como StringType são intencionais — o dado original é preservado.
# MAGIC - **Delta Table:** diferente de Parquet puro, o Delta mantém transaction log,
# MAGIC   suporta ACID, schema enforcement e time travel.
# MAGIC - **Lazy evaluation:** nenhum dado é lido até o `.write` ser chamado —
# MAGIC   o Spark constrói o plano de execução antes de executar.
