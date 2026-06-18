from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, count, when, current_timestamp, lit


VALID_ORDER_STATUSES = [
    "created", "approved", "invoiced", "processing",
    "shipped", "delivered", "unavailable", "canceled"
]


def check_not_null(df: DataFrame, column: str) -> DataFrame:
    """Retorna registros onde a coluna é nula."""
    return df.filter(col(column).isNull())


def check_non_negative(df: DataFrame, column: str) -> DataFrame:
    """Retorna registros onde o valor numérico é negativo."""
    return df.filter(col(column) < 0)


def check_valid_status(df: DataFrame, column: str, valid_values: list) -> DataFrame:
    """Retorna registros onde o valor não está na lista permitida."""
    return df.filter(~col(column).isin(valid_values))


def build_quality_summary(
    spark: SparkSession,
    table_name: str,
    rule_name: str,
    total: int,
    invalid: int
) -> DataFrame:
    """Cria uma linha de resumo de qualidade para salvar em gold.data_quality_summary."""
    invalid_pct = round(invalid / total * 100, 2) if total > 0 else 0.0
    return spark.createDataFrame([{
        "table_name": table_name,
        "rule_name": rule_name,
        "total_records": total,
        "invalid_records": invalid,
        "invalid_percentage": invalid_pct,
        "checked_at": str(current_timestamp()),
    }])
