# Conceitos Spark demonstrados no projeto

## Lazy evaluation

O Spark não executa transformações imediatamente. Cada `.withColumn`, `.filter`, `.join` apenas
adiciona uma etapa ao **plano de execução lógico**. A execução real só ocorre quando uma *action*
é chamada: `.count()`, `.write`, `.show()`, `.collect()`.

Isso permite ao Spark otimizar o plano antes de executar — por exemplo, aplicar filtros antes
de joins (predicate pushdown) mesmo que o código os defina depois.

```python
df = spark.table("workspace.silver.orders")    # sem execução
df = df.filter(col("order_status") == "delivered")  # sem execução
df = df.select("order_id", "customer_id")           # sem execução
count = df.count()                                   # execução acontece aqui
```

---

## Schemas explícitos (StructType)

`inferSchema=True` faz dois scans do arquivo e pode inferir tipos incorretos (ex: `review_score`
inferido como `Long` quando é `Integer`). O projeto define todos os schemas explicitamente em
`src/schemas.py`:

```python
ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_status", StringType(), True),
    ...
])
```

Datas são mantidas como `StringType` na Bronze intencionalmente — a conversão é responsabilidade
da Silver, que também valida o formato.

---

## Broadcast join

Em um join padrão (sort-merge join), o Spark embaralha ambas as tabelas pela rede para colocar
registros com a mesma chave no mesmo executor. Quando uma das tabelas é pequena (< ~10 MB), o
**broadcast join** envia uma cópia dela para todos os executores — o join acontece localmente,
sem shuffle da tabela grande.

Usado no projeto para a tabela de tradução de categorias (71 linhas) com `silver.products`:

```python
products.join(
    broadcast(category_translation.select("product_category_name", "product_category_name_english")),
    "product_category_name", "left"
)
```

---

## Window functions

Permitem calcular valores que dependem de outras linhas do mesmo grupo sem precisar de subquery.
Usadas no projeto para identificar o método de pagamento principal (maior valor) por pedido:

```python
w = Window.partitionBy("order_id").orderBy(col("payment_value").desc())

main_payment = (
    payments
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .select("order_id", col("payment_type").alias("main_payment_type"))
)
```

`row_number()` numera as linhas dentro de cada `order_id`, ordenadas por valor desc. Pegar
`rn == 1` seleciona o pagamento de maior valor por pedido.

---

## Particionamento

A tabela `gold.fact_order_revenue` é particionada por `order_purchase_date`:

```python
fact.write.format("delta")
    .partitionBy("order_purchase_date")
    .saveAsTable("workspace.gold.fact_order_revenue")
```

Queries que filtram por data (`WHERE order_purchase_date = '2018-01-01'`) leem apenas os
arquivos da partição correspondente — **partition pruning**. O Spark não abre os arquivos de
outras datas.

---

## OPTIMIZE e ZORDER

O Delta Lake acumula arquivos Parquet pequenos a cada write. `OPTIMIZE` os compacta.
`ZORDER BY` reorganiza fisicamente os dados para que valores similares fiquem no mesmo bloco.

O Delta armazena estatísticas de `min/max` por coluna em cada bloco **automaticamente** em
todos os writes. O ZORDER não cria as estatísticas — ele reorganiza os dados para que as
estatísticas se tornem úteis: quando os blocos têm intervalos min/max que não se sobrepõem,
o Spark pode pular a maioria deles ao filtrar (**data skipping**).

```sql
OPTIMIZE workspace.gold.fact_order_revenue
ZORDER BY (customer_state, order_status);
```

---

## Delta time travel

O Delta mantém um transaction log com todas as versões da tabela. Cada `write` ou `MERGE`
cria uma versão numerada. É possível consultar versões anteriores:

```python
# Por número de versão
spark.read.format("delta").option("versionAsOf", 0).table("workspace.gold.fact_order_revenue")

# Por timestamp
spark.read.format("delta").option("timestampAsOf", "2024-01-01").table(...)

# Histórico de operações
spark.sql("DESCRIBE HISTORY workspace.gold.fact_order_revenue").show()
```

Os arquivos antigos são preservados até o `VACUUM` ser executado (mínimo recomendado: 7 dias).

---

## Multiline CSV e RFC 4180

O arquivo `olist_order_reviews_dataset.csv` contém dois problemas de parsing:

1. **Quebras de linha dentro de campos** — `review_comment_message` pode conter `\n`.
   Corrigido com `.option("multiLine", True)`.

2. **Aspas escapadas com `""`** — padrão RFC 4180 (`""bolinhas""` = `"bolinhas"`).
   O Spark usa `\` como escape por padrão — para entender `""`, é necessário
   `.option("escape", '"')`.

Sem essas opções, os campos subsequentes à quebra ou às aspas ficam desalinhados,
fazendo texto do comentário aparecer na coluna `review_creation_date`.
