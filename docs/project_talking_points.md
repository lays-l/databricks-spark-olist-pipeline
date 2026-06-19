# Pontos de discussão do projeto

Referência para apresentação ou defesa do projeto de pós-graduação.

---

## Por que Databricks e não Spark local?

O Databricks oferece Serverless compute gerenciado — não é necessário configurar cluster,
instalar dependências ou gerenciar recursos. O Unity Catalog centraliza governança, lineage
e controle de acesso. Em produção, pipelines de dados rodam em plataformas como Databricks,
AWS EMR ou Google Dataproc — não em máquinas locais.

---

## Por que arquitetura Medallion (Bronze/Silver/Gold)?

- **Bronze** preserva os dados originais — qualquer reprocessamento pode partir do zero
- **Silver** centraliza regras de limpeza — mudanças de negócio são aplicadas em um lugar
- **Gold** evita joins repetidos — ferramentas de BI consomem dados prontos

Alternativa sem Medallion: transformar direto do CSV para tabela analítica. O problema é que
qualquer erro de lógica força reingerer os dados originais. A separação em camadas cria
pontos de reprocessamento independentes.

---

## Por que Delta Lake e não Parquet puro?

| Feature | Parquet puro | Delta Lake |
|---|---|---|
| ACID transactions | Não | Sim |
| Schema enforcement | Não | Sim |
| Time travel | Não | Sim |
| MERGE (upsert) | Não | Sim |
| Transaction log | Não | Sim |
| OPTIMIZE/ZORDER | Não | Sim |

Em produção, sem ACID, uma falha no meio de uma escrita deixa a tabela em estado inconsistente.

---

## Por que `is_late = null` para pedidos não entregues?

Usar `False` para pedidos ainda em trânsito mascararia a taxa de atraso real.
Se 10% dos pedidos estão em trânsito e são contados como "não atrasados", a taxa calculada
seria artificialmente baixa. Com `null`, o filtro `WHERE is_delivered = true` garante que
apenas pedidos concluídos entram no denominador da taxa.

---

## Por que overwrite e não carga incremental?

O dataset Olist é um snapshot histórico estático — não há novos dados chegando. Fazer MERGE
em dados que nunca mudam não agrega valor prático. Em produção com dados dinâmicos:
- Bronze: `mode("append")` com controle por data de ingestão
- Silver: `MERGE INTO` para aplicar atualizações de status (ex: `shipped` → `delivered`)

  Sem controle de janela, o MERGE faria join de toda a Silver contra toda a fonte a cada
  execução — caro em escala. A abordagem correta usa uma **marca d'água (watermark)**:
  filtra apenas os registros modificados desde a última execução antes de chegar no MERGE.

  ```python
  from delta.tables import DeltaTable
  from pyspark.sql.functions import (
      col, to_timestamp, to_date, datediff, when, lower, trim, max as spark_max, expr
  )

  # Watermark: busca o timestamp da última ingestão na Bronze.
  last_ingestion = spark.sql("""
      SELECT MAX(ingestion_timestamp) AS last_run
      FROM workspace.bronze.orders
      WHERE ingestion_date = current_date() - INTERVAL 1 DAY
  """).collect()[0]["last_run"]

  # Lê apenas os registros novos ou modificados desde o último run
  # e aplica as mesmas transformações do 02_transform_silver.py
  new_orders = (
      spark.table("workspace.bronze.orders")
      .filter(col("ingestion_timestamp") > last_ingestion)
      .withColumn("order_purchase_timestamp",      to_timestamp("order_purchase_timestamp"))
      .withColumn("order_approved_at",             to_timestamp("order_approved_at"))
      .withColumn("order_delivered_carrier_date",  to_timestamp("order_delivered_carrier_date"))
      .withColumn("order_delivered_customer_date", to_timestamp("order_delivered_customer_date"))
      .withColumn("order_estimated_delivery_date", to_timestamp("order_estimated_delivery_date"))
      .withColumn("order_status",                  lower(trim(col("order_status"))))
      .withColumn("order_purchase_date",           to_date("order_purchase_timestamp"))
      .withColumn("delivery_days",
          datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp")))
      .withColumn("estimated_delivery_days",
          datediff(col("order_estimated_delivery_date"), col("order_purchase_timestamp")))
      .withColumn("is_delivered",
          when(col("order_delivered_customer_date").isNotNull(), True).otherwise(False))
      .withColumn("is_late",
          when(col("is_delivered") == True,
              when(col("order_delivered_customer_date") > col("order_estimated_delivery_date"),
                  True).otherwise(False)))
  )

  # MERGE processa apenas o delta — não a tabela histórica inteira
  silver_table = DeltaTable.forName(spark, "workspace.silver.orders")

  (
      silver_table.alias("target").merge(
          new_orders.alias("source"),
          "target.order_id = source.order_id"
      ).whenMatchedUpdate(
          # atualiza apenas se o status mudou — evita rewrites desnecessários
          condition="target.order_status != source.order_status",
          set={
              "order_status":                  "source.order_status",
              "order_delivered_customer_date": "source.order_delivered_customer_date",
              "delivery_days":                 "source.delivery_days",
              "is_delivered":                  "source.is_delivered",
              "is_late":                       "source.is_late",
          }
      ).whenNotMatchedInsertAll()  # insere se o order_id não existe ainda
      .execute()
  )
  ```
- Gold: recalculo incremental por partição afetada

---

## Decisões de modelagem na Gold

**Por que pré-agregar pagamentos e itens antes do join da fato?**

Um pedido pode ter múltiplos pagamentos e múltiplos itens. Sem pré-agregação, o join
multiplicaria as linhas: um pedido com 3 itens e 2 pagamentos geraria 6 linhas na fato.
A pré-agregação garante 1 linha por pedido antes do join final.

**Por que particionar a fato por `order_purchase_date`?**

Queries analíticas frequentemente filtram por período. Com particionamento, o Spark lê
apenas os arquivos da data filtrada (partition pruning), ignorando todo o resto.

**Por que `countDistinct` para `total_orders` em `seller_performance`?**

Um seller pode ter múltiplos itens no mesmo pedido. `count("order_id")` contaria cada item
como um pedido. `countDistinct("order_id")` conta pedidos únicos — métrica correta.

---

## O que demonstra conhecimento de engenharia de dados

- Schema explícito em vez de `inferSchema` — previsibilidade e performance
- Separação de responsabilidades por camada — manutenibilidade
- Isolamento de registros inválidos em vez de descarte — rastreabilidade
- Window function para pagamento principal — evita subquery aninhada
- Broadcast join para tabela de lookup pequena — evita shuffle desnecessário
- Particionamento da tabela fato — otimização de leitura por período
- ZORDER nas colunas mais filtradas — data skipping eficiente
- `to_timestamp` na Silver para conversão de datas — mantidas como `StringType` na Bronze intencionalmente
- `multiLine=True` + `escape='"'` para CSV com texto livre — robustez de ingestão
- Data quality com auditoria — observabilidade do pipeline
