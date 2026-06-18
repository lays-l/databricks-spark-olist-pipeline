# data/

Esta pasta existe apenas para documentação local. Os dados reais ficam no Databricks.

## raw/
Placeholder para CSVs locais durante desenvolvimento. Ignorados pelo Git (ver `.gitignore`).

## sample/
CSVs sintéticos gerados pelo script `scripts/generate_sample_data.py`.
Usados como fallback quando os dados reais da Olist não estão disponíveis.

## Dados em produção
Os CSVs da Olist ficam no Unity Catalog Volume:
`/Volumes/workspace/default/olist_raw/`
