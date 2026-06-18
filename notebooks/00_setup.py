# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup do Ambiente
# MAGIC
# MAGIC **Objetivo:** Preparar o ambiente para execução do pipeline.
# MAGIC
# MAGIC Este notebook:
# MAGIC 1. Valida o ambiente (catalog, versão do Spark)
# MAGIC 2. Cria os schemas Bronze, Silver e Gold no Unity Catalog
# MAGIC 3. Cria o Volume para armazenar os CSVs brutos
# MAGIC 4. Baixa os CSVs da Olist via Kaggle API
# MAGIC 5. Verifica se todos os arquivos necessários estão disponíveis

# COMMAND ----------
# MAGIC %md ## Célula 1 — Validar ambiente

# COMMAND ----------

print(f"Spark version: {spark.version}")
spark.sql("SELECT current_catalog(), current_database()").show()

# COMMAND ----------
# MAGIC %md ## Célula 2 — Criar schemas e Volume no Unity Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 'workspace' é o catalog padrão confirmado via SELECT current_catalog() com Spark 4.1.0
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.gold;
# MAGIC
# MAGIC -- Volume para armazenar os CSVs brutos
# MAGIC -- Path: /Volumes/workspace/default/olist_raw/
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.olist_raw;

# COMMAND ----------
# MAGIC %md ## Célula 3 — Baixar CSVs da Olist via Kaggle API
# MAGIC
# MAGIC **Atenção:** informe seu token antes de executar. Não commite esse valor no Git.
# MAGIC Token obtido em: kaggle.com/settings > API > Create New API Token

# COMMAND ----------

import os
os.environ["KAGGLE_API_TOKEN"] = "seu_token_aqui"  # substituir — não commitar

# COMMAND ----------

# MAGIC %pip install kaggle --quiet

# COMMAND ----------

import subprocess

resultado = subprocess.run(
    [
        "kaggle", "datasets", "download",
        "--dataset", "olistbr/brazilian-ecommerce",
        "--unzip",
        "--path", "/Volumes/workspace/default/olist_raw/"
    ],
    capture_output=True, text=True
)

print(resultado.stdout)
if resultado.returncode != 0:
    print("Erro:", resultado.stderr)

# COMMAND ----------
# MAGIC %md ## Célula 4 — Verificar arquivos no Volume

# COMMAND ----------

from src.config import RAW_DATA_PATH, SOURCE_FILES

arquivos = dbutils.fs.ls(RAW_DATA_PATH)
arquivos_encontrados = {f.name for f in arquivos}

print(f"Arquivos em {RAW_DATA_PATH}:\n")
for nome in sorted(arquivos_encontrados):
    print(f"  ✓ {nome}")

faltando = [n for n in SOURCE_FILES.values() if n not in arquivos_encontrados]
if faltando:
    print("\nArquivos ausentes:")
    for nome in faltando:
        print(f"  ✗ {nome}")
else:
    print("\nTodos os arquivos disponíveis. Ambiente pronto para o 01_ingest_bronze.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pontos de discussão
# MAGIC
# MAGIC - A Medallion Architecture (Bronze/Silver/Gold) separa responsabilidades por camada,
# MAGIC   facilitando governança, rastreabilidade e reprocessamento controlado.
# MAGIC - No Databricks Free Edition, o catalog padrão é `workspace` (Unity Catalog).
# MAGIC - Volumes substituem o FileStore com governança integrada: permissões, auditoria e path padronizado.
# MAGIC - O Serverless compute elimina a necessidade de criar e gerenciar clusters —
# MAGIC   o ambiente sobe automaticamente ao executar a primeira célula.
