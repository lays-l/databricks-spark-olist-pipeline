# Arquitetura do Pipeline

## Visão geral

```
CSVs da Olist (Kaggle)
        ↓
/Volumes/workspace/default/olist_raw/   ← Unity Catalog Volume
        ↓  [01_ingest_bronze.py]
workspace.bronze.*    Delta Tables — dados brutos, schema explícito, metadados de ingestão
        ↓  [02_transform_silver.py]
workspace.silver.*    Delta Tables — tipagem, limpeza, campos calculados, inválidos isolados
        ↓  [03_build_gold.py]
workspace.gold.*      Delta Tables — tabelas analíticas prontas para consumo
        ↓
Databricks SQL Editor / sample_queries.sql
```

---

## Camadas

### Bronze — ingestão raw

Responsabilidade: persistir os dados originais com o mínimo de transformação.

- Schema explícito via `StructType` em `src/schemas.py` — sem `inferSchema=True`
- Datas mantidas como `StringType` — a conversão é responsabilidade da Silver
- Colunas de metadados adicionadas: `ingestion_timestamp`, `ingestion_date`, `source_file`
- Opções de leitura CSV: `multiLine=True` e `escape='"'` para reviews (campo com texto livre)
- Modo de escrita: `overwrite`

### Silver — limpeza e tipagem

Responsabilidade: dados confiáveis, tipados e padronizados para consumo interno.

- Conversão de `StringType` para `TimestampType` com `to_timestamp`
- Campos calculados: `order_purchase_date`, `delivery_days`, `estimated_delivery_days`, `is_delivered`, `is_late`
- `is_late = null` para pedidos não entregues — evita distorção na taxa de atraso
- Registros inválidos isolados em `silver.invalid_orders` e `silver.invalid_payments`
- Broadcast join para tabela de categorias (71 linhas) com `silver.products`
- Select explícito em todas as tabelas — define o contrato de schema da camada
- Modo de escrita: `overwrite`

### Gold — consumo analítico

Responsabilidade: tabelas modeladas para responder perguntas de negócio sem joins repetidos.

- `fact_order_revenue` — tabela fato, 1 linha por pedido, particionada por `order_purchase_date`
- `daily_revenue` — receita por dia × estado
- `customer_state_revenue` — receita e taxa de atraso por estado
- `product_category_revenue` — receita por categoria
- `seller_performance` — desempenho por seller
- `payment_method_summary` — resumo por método de pagamento
- Modo de escrita: `overwrite` (dataset estático — ver `docs/project_talking_points.md` para padrão incremental com watermark e MERGE)

---

## Unity Catalog

Todas as tabelas seguem o padrão de três partes: `catalog.schema.table`.

| Catalog | Schema | Uso |
|---|---|---|
| `workspace` | `bronze` | Dados brutos ingeridos |
| `workspace` | `silver` | Dados limpos e tipados |
| `workspace` | `gold` | Tabelas analíticas |
| `workspace` | `default` | Volume `olist_raw` com os CSVs |

O catálogo `workspace` é o padrão do Databricks Free Edition (confirmado via `SELECT current_catalog()`).

---

## Delta Lake

Todas as tabelas são Delta — não Parquet puro. Isso habilita:

- **ACID transactions** — escritas atômicas sem dados corrompidos em falhas parciais.
  Sem ACID, uma falha no meio de um `overwrite` pode deixar a tabela com arquivos parciais.
  O Delta escreve em staging e só confirma no transaction log após a conclusão.

- **Schema enforcement** — rejeita escritas que violam o schema definido.
  Se um novo campo chegar com tipo errado (ex: `price` como `StringType`), o Delta recusa
  o write em vez de corromper silenciosamente os dados.

- **Time travel** — consulta versões anteriores via número de versão ou timestamp.

  ```python
  # Estado da tabela antes do último overwrite
  spark.read.format("delta").option("versionAsOf", 0).table("workspace.gold.fact_order_revenue")

  # Histórico de todas as operações
  spark.sql("DESCRIBE HISTORY workspace.gold.fact_order_revenue").show(truncate=False)
  ```

- **Transaction log** — cada operação (`write`, `MERGE`, `OPTIMIZE`) gera uma entrada em
  `_delta_log/`. O Spark lê esse log para reconstruir o estado atual da tabela sem precisar
  escanear todos os arquivos físicos.

- **OPTIMIZE + ZORDER** — compactação de arquivos pequenos e reorganização física para
  data skipping. Ver detalhes em `docs/spark_concepts.md`.
