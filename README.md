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
CSVs da Olist (Kaggle → Unity Catalog Volume)
        ↓  [01_ingest_bronze.py]
workspace.bronze.*   — dados brutos com schema explícito + metadados de ingestão
        ↓  [02_transform_silver.py]
workspace.silver.*   — tipagem, limpeza, validação, registros inválidos isolados
        ↓  [03_build_gold.py]
workspace.gold.*     — tabelas analíticas prontas para consumo
        ↓
Databricks SQL Editor / sql/sample_queries.sql
```

Detalhes de cada camada: [`docs/architecture.md`](docs/architecture.md)

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
├── notebooks/
│   ├── 00_setup.py               # setup do ambiente, download dos CSVs
│   ├── 01_ingest_bronze.py       # ingestão CSV → Delta Bronze
│   ├── 02_transform_silver.py    # limpeza, tipagem, validação
│   ├── 03_build_gold.py          # tabelas analíticas
│   ├── 04_data_quality_checks.py # validações com registro de auditoria
│   └── 05_spark_optimization_examples.py  # broadcast, ZORDER, time travel
│
├── src/
│   ├── config.py       # constantes: paths, nomes de tabelas, catalog
│   ├── schemas.py      # StructType explícitos para cada tabela Bronze
│   └── data_quality.py # funções de validação reutilizáveis
│
├── sql/
│   └── sample_queries.sql  # 12 queries analíticas prontas para o SQL Editor
│
└── docs/
    ├── architecture.md            # diagrama e descrição de cada camada
    ├── spark_concepts.md          # lazy eval, broadcast, ZORDER, time travel
    └── project_talking_points.md  # decisões técnicas e pontos de defesa
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
> de ambiente pela interface de configuração. O token deve ser informado diretamente na
> célula do notebook antes de executar o download, e **não deve ser commitado**.

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
| `01_ingest_bronze.py` | Lê CSVs com schema explícito → `workspace.bronze.*` |
| `02_transform_silver.py` | Tipagem, limpeza, campos calculados → `workspace.silver.*` |
| `03_build_gold.py` | Tabelas analíticas → `workspace.gold.*` |
| `04_data_quality_checks.py` | Validações com auditoria → `workspace.gold.data_quality_summary` |
| `05_spark_optimization_examples.py` | Exemplos de OPTIMIZE, ZORDER e time travel |

---

## Tabelas criadas

### Bronze

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

### Silver

| Tabela | Descrição |
|---|---|
| `workspace.silver.orders` | Pedidos com datas convertidas, flags `is_late` e `is_delivered` |
| `workspace.silver.order_items` | Itens com `item_total_value` calculado |
| `workspace.silver.payments` | Pagamentos validados |
| `workspace.silver.customers` | Clientes padronizados |
| `workspace.silver.products` | Produtos com categoria em inglês (broadcast join) |
| `workspace.silver.sellers` | Vendedores padronizados |
| `workspace.silver.reviews` | Avaliações com tipagem correta |
| `workspace.silver.invalid_orders` | Pedidos inválidos isolados para auditoria |
| `workspace.silver.invalid_payments` | Pagamentos inválidos isolados para auditoria |

### Gold

| Tabela | Descrição |
|---|---|
| `workspace.gold.fact_order_revenue` | Tabela fato, 1 linha por pedido, particionada por data |
| `workspace.gold.daily_revenue` | Receita por dia e estado |
| `workspace.gold.customer_state_revenue` | Receita e taxa de atraso por estado |
| `workspace.gold.product_category_revenue` | Receita por categoria |
| `workspace.gold.seller_performance` | Desempenho por seller |
| `workspace.gold.payment_method_summary` | Resumo por método de pagamento |
| `workspace.gold.data_quality_summary` | Auditoria de qualidade de dados |

---

## Documentação

| Arquivo | Conteúdo |
|---|---|
| [`docs/architecture.md`](docs/architecture.md) | Diagrama, descrição das camadas, Unity Catalog, Delta Lake |
| [`docs/spark_concepts.md`](docs/spark_concepts.md) | Lazy eval, broadcast join, window functions, ZORDER, time travel, multiLine CSV |
| [`docs/project_talking_points.md`](docs/project_talking_points.md) | Decisões técnicas e pontos de defesa do projeto |
