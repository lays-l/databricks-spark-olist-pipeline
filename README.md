# Databricks + PySpark + Delta Lake — Olist E-Commerce Pipeline

Pipeline de engenharia de dados desenvolvido como projeto de pós-graduação, utilizando
**Databricks Free Edition**, **PySpark**, **Spark SQL** e **Delta Lake** com a base pública
[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

---

## Stack

| Componente | Detalhe |
|---|---|
| Plataforma | Databricks Free Edition |
| Compute | Serverless (Spark 4.1.0, Python 3.11) |
| Catalog | Unity Catalog — catalog padrão `workspace` |
| Storage | Unity Catalog Volumes (`/Volumes/workspace/default/olist_raw/`) |
| Formato | Delta Lake (ACID, time travel, MERGE incremental) |
| Linguagem | Python / PySpark / Spark SQL |

---

## Arquitetura

```
CSVs da Olist
(Kaggle → Unity Catalog Volume)
        ↓
/Volumes/workspace/default/olist_raw/
        ↓  [01_ingest_bronze.py]
workspace.bronze.*   — dados brutos com schema explícito + metadados de ingestão
        ↓  [02_transform_silver.py]
workspace.silver.*   — tipagem, limpeza, validação, registros inválidos isolados
        ↓  [03_build_gold.py]
workspace.gold.*     — tabelas analíticas: fato, agregações, MERGE incremental
        ↓
Databricks SQL Editor / Dashboard
```

---

## Perguntas de negócio respondidas

- Qual foi a receita diária?
- Quais estados geram mais receita?
- Quais categorias vendem mais?
- Qual o tempo médio de entrega?
- Quais pedidos foram entregues com atraso?
- Quais métodos de pagamento são mais utilizados?
- Pedidos parcelados têm ticket médio maior?
- Quais sellers têm maior volume de vendas?

---

## Estrutura do repositório

```
databricks-spark-olist-pipeline/
│
├── README.md
├── requirements.txt
├── .gitignore
│
├── data/
│   ├── raw/          # placeholder local — dados reais ficam no Volume
│   └── sample/       # dados sintéticos gerados por scripts/generate_sample_data.py
│
├── notebooks/
│   ├── 00_setup.py               # setup do ambiente, download dos CSVs
│   ├── 01_ingest_bronze.py       # ingestão CSV → Delta Bronze
│   ├── 02_transform_silver.py    # limpeza, tipagem, validação
│   ├── 03_build_gold.py          # tabelas analíticas + MERGE incremental
│   ├── 04_data_quality_checks.py # validações com registro de auditoria
│   └── 05_spark_optimization_examples.py  # broadcast, ZORDER, time travel
│
├── src/
│   ├── config.py          # constantes: paths, nomes de tabelas, catalog
│   ├── schemas.py         # StructType explícitos para cada tabela Bronze
│   ├── transformations.py # funções de transformação reutilizáveis
│   └── data_quality.py    # funções de validação de qualidade
│
├── sql/
│   └── sample_queries.sql         # 12 queries analíticas prontas para o SQL Editor
│
└── docs/
    ├── architecture.md
    ├── spark_concepts.md
    └── project_talking_points.md
```

---

## Pré-requisitos

- Conta no [Databricks Free Edition](https://www.databricks.com/try-databricks)
- Conta no [Kaggle](https://www.kaggle.com) com API Token gerado
- Repositório conectado ao Databricks via **Workspace → Repos**

---

## Como executar

### 1. Conectar o repositório ao Databricks

1. No Databricks, vá em **Workspace → Repos → Add Repo**
2. Cole a URL deste repositório
3. Clique em **Create Repo**

### 2. Configurar o Kaggle API Token

> **Restrição do Databricks Free Edition:** o Serverless não possui suporte a variáveis
> de ambiente pela interface de configuração (a seção "Environment variables" não existe
> nessa versão). O token deve ser informado diretamente na célula do notebook antes de
> executar o download, e **não deve ser commitado no repositório**.

**Como obter o token:**
1. Acesse [kaggle.com/settings](https://www.kaggle.com/settings)
2. Seção **API** → **Create New API Token**
3. Copie o token gerado (formato `KGAT_...`)

**Como usar no notebook:**

No `00_setup.py`, localize a célula de configuração e substitua o valor antes de executar:

```python
os.environ["KAGGLE_API_TOKEN"] = "KGAT_seu_token_aqui"  # substituir antes de executar
```

Após o download ser concluído, os CSVs ficam persistidos no Volume do Unity Catalog.
O token não é necessário para execuções seguintes — apenas para o download inicial.
**Nunca commite o notebook com o token real preenchido.**

### 3. Executar os notebooks em ordem

Abra cada notebook dentro do repo no Databricks e execute com **Serverless compute**:

| Notebook | O que faz |
|---|---|
| `00_setup.py` | Cria schemas, Volume e baixa os CSVs via Kaggle API |
| `01_ingest_bronze.py` | Lê CSVs com schema explícito → salva como Delta em `workspace.bronze.*` |
| `02_transform_silver.py` | Tipagem, limpeza, campos calculados → `workspace.silver.*` |
| `03_build_gold.py` | Tabelas analíticas + MERGE incremental → `workspace.gold.*` |
| `04_data_quality_checks.py` | Validações com auditoria → `workspace.gold.data_quality_summary` |
| `05_spark_optimization_examples.py` | Exemplos comentados de otimização Spark |

---

## Tabelas criadas

### Bronze — dados brutos

| Tabela | Fonte |
|---|---|
| `workspace.bronze.orders` | olist_orders_dataset.csv |
| `workspace.bronze.order_items` | olist_order_items_dataset.csv |
| `workspace.bronze.payments` | olist_order_payments_dataset.csv |
| `workspace.bronze.customers` | olist_customers_dataset.csv |
| `workspace.bronze.products` | olist_products_dataset.csv |
| `workspace.bronze.sellers` | olist_sellers_dataset.csv |
| `workspace.bronze.reviews` | olist_order_reviews_dataset.csv |
| `workspace.bronze.category_translation` | product_category_name_translation.csv |

### Silver — dados limpos

| Tabela | Descrição |
|---|---|
| `workspace.silver.orders` | Pedidos com datas convertidas, flags `is_late` e `is_delivered` |
| `workspace.silver.order_items` | Itens com `item_total_value` calculado |
| `workspace.silver.payments` | Pagamentos validados com flags de tipo |
| `workspace.silver.customers` | Clientes padronizados |
| `workspace.silver.products` | Produtos com categoria em inglês (broadcast join) |
| `workspace.silver.sellers` | Vendedores padronizados |
| `workspace.silver.reviews` | Avaliações com tipagem correta |
| `workspace.silver.invalid_orders` | Pedidos inválidos isolados para auditoria |
| `workspace.silver.invalid_payments` | Pagamentos inválidos isolados para auditoria |

### Gold — tabelas analíticas

| Tabela | Descrição |
|---|---|
| `workspace.gold.fact_order_revenue` | Tabela fato com 1 linha por pedido, particionada por data |
| `workspace.gold.daily_revenue` | Receita agregada por dia e estado |
| `workspace.gold.customer_state_revenue` | Receita, volume e taxa de atraso por estado |
| `workspace.gold.product_category_revenue` | Receita e volume por categoria de produto |
| `workspace.gold.seller_performance` | Desempenho de vendas e entrega por seller |
| `workspace.gold.payment_method_summary` | Resumo por método de pagamento |
| `workspace.gold.data_quality_summary` | Auditoria de qualidade de dados |

---

## Conceitos Spark demonstrados

- **Schemas explícitos (StructType)** — sem `inferSchema`, tipagem previsível e performática
- **Medallion Architecture** — separação Bronze / Silver / Gold com responsabilidades distintas
- **Delta Lake** — ACID, schema enforcement, MERGE incremental, time travel
- **Broadcast join** — tabela de tradução de categorias enviada para todos os executores
- **Window functions** — identificação do pagamento principal por pedido
- **Lazy evaluation** — plano de execução construído antes da action
- **Particionamento** — tabela fato particionada por `order_purchase_date`
- **OPTIMIZE + ZORDER** — compactação e reorganização física para data skipping
- **Data quality** — registros inválidos isolados com registro de auditoria

---

## Decisões técnicas

**Por que schemas explícitos na Bronze?**
`inferSchema=True` faz scan duplo do arquivo e pode inferir tipos incorretos. Schemas
definidos em `src/schemas.py` tornam o pipeline previsível. Datas ficam como `StringType`
na Bronze intencionalmente — a conversão é responsabilidade da Silver.

**Por que `is_late` é `null` para pedidos não entregues?**
Usar `False` para pedidos ainda em trânsito mascararia a análise de taxa de atraso.
`null` deixa explícito que o campo não é aplicável, e o filtro `WHERE is_delivered = true`
garante que apenas pedidos concluídos entram no cálculo.

**Por que `overwrite` e não carga incremental?**

Este projeto usa `mode("overwrite")` na Bronze e Silver intencionalmente, por duas razões:

1. **Dataset estático** — os CSVs da Olist são um snapshot histórico fixo. Não há novos
   arquivos chegando, portanto não existe "delta" real a processar. Fazer MERGE num dataset
   que nunca muda não acrescentaria valor prático.

2. **Foco educacional** — o overwrite simplifica o ciclo de desenvolvimento: limpa a tabela
   e reescreve do zero a cada execução, eliminando efeitos colaterais de reprocessamentos
   parciais durante a construção e depuração do pipeline.

**Como seria o incremental em produção:**

Em um pipeline real com dados chegando continuamente, a abordagem correta seria:

- **Bronze (append):** ao invés de `overwrite`, usar `mode("append")` com filtro por data de
  ingestão, garantindo que apenas arquivos novos sejam processados:

  ```python
  df.write.format("delta").mode("append").saveAsTable(table_name)
  ```

- **Silver (MERGE/upsert):** para refletir atualizações em pedidos já existentes (ex: status
  mudou de `shipped` para `delivered`), usar `MERGE INTO` que aplica insert para novos
  registros e update para os existentes, sem reescrever toda a tabela:

  ```python
  silver_table.alias("target").merge(
      new_data.alias("source"),
      "target.order_id = source.order_id"
  ).whenMatchedUpdateAll()   # atualiza se já existe
   .whenNotMatchedInsertAll() # insere se é novo
   .execute()
  ```

- **Gold (MERGE ou recalculo incremental):** tabelas de agregação podem ser recalculadas
  apenas para o período afetado (ex: última semana) usando filtro de partição, evitando
  reprocessar todo o histórico.

O Delta Lake suporta as duas estratégias nativamente e mantém o transaction log em ambos os
casos, o que habilita `time travel` independentemente do modo usado.

**OPTIMIZE e ZORDER — quando cada um importa**

O Delta Lake acumula arquivos Parquet pequenos a cada write — especialmente em tabelas
particionadas ou com cargas incrementais frequentes. O `OPTIMIZE` compacta esses arquivos
em arquivos maiores (~1 GB), reduzindo o overhead de I/O.

> **Impacto real:** em pipelines com carga incremental diária (append ou MERGE), cada
> execução gera novos arquivos nas partições existentes. Sem `OPTIMIZE`, uma partição de
> um dia pode ter dezenas de arquivos tiny após semanas de execução. Neste projeto com
> dataset estático e `overwrite`, o impacto do OPTIMIZE é baixo — o valor aparece em
> produção com cargas incrementais contínuas.

O `OPTIMIZE` não exclui fisicamente os arquivos antigos: apenas os marca como removidos
no transaction log. A exclusão física ocorre ao rodar `VACUUM` (mínimo recomendado:
`RETAIN 168 HOURS` para preservar 7 dias de time travel).

**ZORDER e data skipping**

O Delta Lake armazena estatísticas de `min` e `max` por coluna para cada bloco de dados
automaticamente em **todos os writes** — independente do ZORDER. O que o ZORDER muda
não é a existência das estatísticas, mas a **utilidade** delas.

Sem ZORDER, os dados são distribuídos aleatoriamente: cada bloco tem SP misturado com
AM, RJ, MG... então o intervalo `min='AM' / max='SP'` se repete em praticamente todos
os blocos. O Spark não consegue eliminar nenhum bloco ao filtrar por `customer_state = 'SP'`.

Com `ZORDER BY (customer_state)`, registros com valores próximos ficam no mesmo bloco.
Os intervalos min/max deixam de se sobrepor — o Spark pula a maioria dos blocos sem
abri-los. Isso é o **data skipping**.

A coluna escolhida deve ser de **média cardinalidade** e usada frequentemente em filtros
analíticos. `customer_state` (27 valores) e `order_status` (7 valores) são candidatos
ideais. `order_id` (99 mil valores únicos) não beneficia o ZORDER — cada bloco teria um
intervalo min/max único e nenhum bloco seria pulado.
