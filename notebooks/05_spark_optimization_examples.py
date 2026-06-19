# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Exemplos de Otimização Spark
# MAGIC
# MAGIC Notebook demonstrativo: executa e explica técnicas de otimização aplicadas sobre as
# MAGIC tabelas já criadas. O objetivo é evidenciar conhecimento prático das características
# MAGIC do Spark e do Delta Lake — não processa dados novos.
# MAGIC
# MAGIC **Técnicas demonstradas:**
# MAGIC 1. Seleção de colunas antes de joins
# MAGIC 2. Filtro antes de agregação (predicate pushdown)
# MAGIC 3. Broadcast join para tabelas pequenas
# MAGIC 4. Repartition com cuidado
# MAGIC 5. Cache com critério
# MAGIC 6. OPTIMIZE e ZORDER (Delta Lake)
# MAGIC 7. Delta time travel

# COMMAND ----------

from pyspark.sql.functions import col, broadcast, count, sum, avg, round

from src.config import (
    SILVER_ORDERS, SILVER_CUSTOMERS, SILVER_PRODUCTS,
    BRONZE_CATEGORY, GOLD_FACT
)

orders    = spark.table(SILVER_ORDERS)
customers = spark.table(SILVER_CUSTOMERS)
products  = spark.table(SILVER_PRODUCTS)
fact      = spark.table(GOLD_FACT)

# COMMAND ----------
# MAGIC %md ## 1. Seleção de colunas antes de joins
# MAGIC
# MAGIC **Por que importa:** o Spark serializa e shuffle todas as colunas do DataFrame durante
# MAGIC um join. Carregar a tabela inteira e só depois filtrar desperdiça memória, I/O e tempo
# MAGIC de rede entre executores.
# MAGIC
# MAGIC **Regra:** selecione apenas as colunas necessárias *antes* do join — nunca depois.

# COMMAND ----------

# ❌ Versão ineficiente: join com todas as colunas, select depois
inefficient = (
    orders.join(customers, "customer_id", "left")
    .select("order_id", "customer_state", "order_purchase_date")
)

# ✅ Versão otimizada: select antes do join reduz dados em memória e shuffle
orders_slim    = orders.select("order_id", "customer_id", "order_purchase_date")
customers_slim = customers.select("customer_id", "customer_state")

efficient = orders_slim.join(customers_slim, "customer_id", "left")

# Ambos produzem o mesmo resultado, mas o segundo é mais eficiente
print("Registros (ineficiente):", inefficient.count())
print("Registros (otimizado): ", efficient.count())

# COMMAND ----------
# MAGIC %md ## 2. Filtro antes de agregação (predicate pushdown)
# MAGIC
# MAGIC **Por que importa:** o Spark lê os dados do armazenamento e os processa em memória.
# MAGIC Aplicar filtros o mais cedo possível reduz o volume de dados que percorre todas as etapas
# MAGIC do plano de execução. Esse padrão é chamado de **predicate pushdown** — o filtro é
# MAGIC "empurrado" para a leitura, não aplicado no final.
# MAGIC
# MAGIC **Delta Lake** otimiza ainda mais: com `ZORDER` e estatísticas de min/max por bloco,
# MAGIC o Spark pode pular blocos inteiros de dados sem nem abri-los (**data skipping**).

# COMMAND ----------

# ❌ Versão ineficiente: agrega tudo, filtra depois
late_rate_bad = (
    fact
    .groupBy("customer_state")
    .agg(
        count("order_id").alias("total"),
        count(col("is_late")).alias("late")
    )
    .filter(col("total") > 100)
)

# ✅ Versão otimizada: filtra antes de agregar, reduzindo o volume do groupBy
late_rate_good = (
    fact
    .filter(col("is_delivered") == True)   # elimina pedidos em trânsito antes do groupBy
    .groupBy("customer_state")
    .agg(
        count("order_id").alias("delivered_orders"),
        count(col("is_late")).alias("late_orders")
    )
)

print("Estados com pedidos entregues:", late_rate_good.count())

# COMMAND ----------
# MAGIC %md ## 3. Broadcast join para tabelas pequenas
# MAGIC
# MAGIC **Por que importa:** em um join padrão (sort-merge join), o Spark embaralha (*shuffle*)
# MAGIC ambas as tabelas pela rede para colocar registros com a mesma chave no mesmo executor.
# MAGIC Isso é caro para tabelas grandes.
# MAGIC
# MAGIC **Broadcast join:** quando uma das tabelas cabe inteiramente em memória (tipicamente
# MAGIC < 10 MB), o Spark envia uma cópia dela para todos os executores. O join acontece
# MAGIC localmente em cada executor, sem shuffle da tabela grande.
# MAGIC
# MAGIC **Critério de uso:** tabelas de lookup, dimensões pequenas, listas de referência.
# MAGIC No projeto, a tabela de tradução de categorias tem apenas 71 linhas — candidata ideal.

# COMMAND ----------

category_translation = spark.table(BRONZE_CATEGORY)
print(f"Linhas na tabela de categorias: {category_translation.count()}")  # ~71 linhas

# ❌ Sem broadcast: Spark pode escolher shuffle de ambos os lados
products_translated_bad = products.join(
    category_translation.select("product_category_name", "product_category_name_english"),
    "product_category_name",
    "left"
)

# ✅ Com broadcast: envia a tabela pequena para todos os executores, zero shuffle na grande
products_translated = products.join(
    broadcast(category_translation.select("product_category_name", "product_category_name_english")),
    "product_category_name",
    "left"
)

print("Produtos com categoria traduzida:", products_translated.count())

# Para confirmar que o Spark usou broadcast, inspecione o plano:
# products_translated.explain()  # procure por "BroadcastHashJoin" no plano

# COMMAND ----------
# MAGIC %md ## 4. Repartition com cuidado
# MAGIC
# MAGIC **Por que importa:** o Spark divide os dados em partições que são processadas em paralelo.
# MAGIC Partições muito pequenas geram overhead de scheduling; partições muito grandes causam
# MAGIC `OutOfMemoryError`. O `repartition` força um shuffle completo para redistribuir os dados —
# MAGIC use apenas quando o ganho de paralelismo compensar o custo do shuffle.
# MAGIC
# MAGIC **`repartition` vs `coalesce`:**
# MAGIC - `repartition(n)` → shuffle completo, útil para aumentar ou redistribuir partições
# MAGIC - `coalesce(n)` → sem shuffle, só une partições — útil para reduzir antes de writes

# COMMAND ----------

# Nota: .rdd.getNumPartitions() não é suportado no Databricks Serverless (RDD API bloqueada).
# Em ambientes com cluster clássico, seria possível inspecionar o número de partições assim:
#   print(f"Partições atuais: {fact.rdd.getNumPartitions()}")
# No Serverless, o número de partições é gerenciado automaticamente pelo runtime.

# ✅ Repartition por coluna de agrupamento antes de múltiplas agregações
# Útil quando a mesma coluna será usada em vários groupBy seguidos
fact_repartitioned = fact.repartition("customer_state")

# ❌ Não usar repartition sem necessidade — gera shuffle desnecessário
# fact_repartitioned = fact.repartition(200)  # evitar número arbitrário sem análise

print(f"Repartition por customer_state aplicado: {fact_repartitioned.count()} linhas")

# COMMAND ----------
# MAGIC %md ## 5. Cache com critério
# MAGIC
# MAGIC **Por que importa:** o Spark reavalia o plano de execução completo a cada *action*
# MAGIC (`count`, `show`, `write`). Se o mesmo DataFrame for usado em múltiplas actions,
# MAGIC ele será relido do disco e recalculado todas as vezes.
# MAGIC
# MAGIC **`cache()`** armazena o DataFrame em memória após a primeira materialização, evitando
# MAGIC releituras e recalculos nas actions seguintes.
# MAGIC
# MAGIC **Quando usar:** apenas quando o mesmo DataFrame for reutilizado 2+ vezes na mesma
# MAGIC sessão. Cache desnecessário consome memória que poderia ser usada para shuffle.

# COMMAND ----------

# Nota: .cache() usa PERSIST TABLE internamente, que não é suportado no Databricks Serverless.
# Em ambientes com cluster clássico, o padrão seria:
#
#   fact_cached = fact.cache()
#
#   # Primeira action: materializa e armazena em memória
#   agg1 = fact_cached.filter(col("customer_state") == "SP").count()
#
#   # Segunda action: lê do cache, não relê o Delta
#   agg2 = fact_cached.filter(col("is_late") == True).count()
#
#   print(f"Pedidos em SP: {agg1}")
#   print(f"Pedidos com atraso: {agg2}")
#
#   # Liberar memória quando não precisar mais
#   fact_cached.unpersist()
#
# No Serverless, o runtime gerencia automaticamente o cache de DataFrames — o .cache()
# explícito não é necessário nem suportado.

print("Exemplo de cache comentado — não suportado no Serverless (PERSIST TABLE bloqueado).")

# COMMAND ----------
# MAGIC %md ## 6. OPTIMIZE e ZORDER (Delta Lake)
# MAGIC
# MAGIC **Problema sem OPTIMIZE:** cada execução de `write` ou `MERGE` gera novos arquivos
# MAGIC Parquet pequenos no Delta. Com o tempo, uma tabela pode ter centenas de arquivos tiny
# MAGIC que o Spark precisa abrir e fechar individualmente — overhead significativo.
# MAGIC
# MAGIC **`OPTIMIZE`** compacta esses arquivos em arquivos maiores (target ~1 GB cada),
# MAGIC reduzindo o número de operações de I/O.
# MAGIC
# MAGIC **`ZORDER BY`** vai além: reorganiza fisicamente os dados dentro dos arquivos para que
# MAGIC registros com valores próximos na coluna escolhida fiquem armazenados juntos.
# MAGIC O Delta armazena estatísticas de `min/max` por bloco — quando uma query filtra por
# MAGIC `customer_state = 'SP'`, o Spark verifica as estatísticas e **pula** blocos onde
# MAGIC `min > 'SP'` ou `max < 'SP'`. Isso é o **data skipping**.
# MAGIC
# MAGIC **Escolha da coluna ZORDER:** deve ser a coluna mais usada em filtros analíticos.
# MAGIC Evitar colunas de alta cardinalidade como `order_id` — raramente aparecem em filtros
# MAGIC analíticos e o benefício seria mínimo.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compacta arquivos pequenos nas tabelas Gold
# MAGIC OPTIMIZE workspace.gold.fact_order_revenue;
# MAGIC OPTIMIZE workspace.gold.daily_revenue;
# MAGIC OPTIMIZE workspace.gold.product_category_revenue;
# MAGIC OPTIMIZE workspace.gold.seller_performance;
# MAGIC OPTIMIZE workspace.gold.customer_state_revenue;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Reorganiza fisicamente a fato pelas colunas mais usadas em filtros analíticos:
# MAGIC -- customer_state: filtros regionais (quais estados geram mais receita?)
# MAGIC -- order_status:   filtros por status (apenas pedidos entregues)
# MAGIC OPTIMIZE workspace.gold.fact_order_revenue
# MAGIC ZORDER BY (customer_state, order_status);

# COMMAND ----------
# MAGIC %md ## 7. Delta time travel
# MAGIC
# MAGIC **O que é:** o Delta Lake mantém um transaction log com todas as versões da tabela
# MAGIC desde sua criação. Cada `write`, `MERGE` ou `OPTIMIZE` cria uma nova versão numerada.
# MAGIC
# MAGIC **Por que é exclusivo do Delta:** tabelas Parquet puras não têm transaction log —
# MAGIC cada escrita sobrescreve os arquivos. O Delta preserva os arquivos antigos até o
# MAGIC `VACUUM` ser executado, habilitando consultas históricas.
# MAGIC
# MAGIC **Casos de uso:**
# MAGIC - "Como era a receita calculada antes da última carga?"
# MAGIC - "Quais registros existiam antes do MERGE de ontem?"
# MAGIC - Reproducibilidade de análises: garantir que o mesmo resultado seja obtido mesmo
# MAGIC   após atualizações nos dados

# COMMAND ----------

# Consultar a versão inicial da tabela (estado após a primeira escrita)
df_v0 = (
    spark.read
    .format("delta")
    .option("versionAsOf", 0)
    .table("workspace.gold.fact_order_revenue")
)
print(f"Registros na versão 0: {df_v0.count()}")

# Ver o histórico completo de operações na tabela
print("\nHistórico de versões:")
spark.sql("DESCRIBE HISTORY workspace.gold.fact_order_revenue").select(
    "version", "timestamp", "operation", "operationParameters"
).show(truncate=False)

# COMMAND ----------
# MAGIC %md ## Resumo: quando usar cada técnica
# MAGIC
# MAGIC | Técnica | Quando usar | Quando evitar |
# MAGIC |---|---|---|
# MAGIC | **Select antes do join** | Sempre | — |
# MAGIC | **Filtro cedo** | Sempre que possível | — |
# MAGIC | **Broadcast join** | Tabela < 10 MB | Tabelas grandes (OutOfMemory) |
# MAGIC | **Repartition** | Antes de múltiplos groupBy pela mesma coluna | Sem análise do volume |
# MAGIC | **Cache** | DataFrame reutilizado 2+ vezes na sessão | Uso único ou DataFrames muito grandes |
# MAGIC | **OPTIMIZE** | Após cargas incrementais com muitos writes | — (sem custo em dataset estático) |
# MAGIC | **ZORDER** | Colunas de média cardinalidade usadas em filtros | Colunas como `order_id` (alta cardinalidade) |
# MAGIC | **Time travel** | Auditoria, depuração, reproducibilidade | — |
