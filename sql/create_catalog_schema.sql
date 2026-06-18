-- Unity Catalog: schemas criados com prefixo de catalog explícito
-- 'workspace' é o catalog padrão do Databricks Free Edition
-- Confirmado via SELECT current_catalog() com Spark 4.1.0
-- Padrão: catalog.schema → workspace.bronze, workspace.silver, workspace.gold

CREATE SCHEMA IF NOT EXISTS workspace.bronze;
CREATE SCHEMA IF NOT EXISTS workspace.silver;
CREATE SCHEMA IF NOT EXISTS workspace.gold;

-- Volume para armazenar os CSVs brutos
-- Equivalente ao /FileStore/ nas versões sem Unity Catalog
-- Path: /Volumes/workspace/default/olist_raw/
CREATE VOLUME IF NOT EXISTS workspace.default.olist_raw;
