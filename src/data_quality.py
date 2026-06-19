from pyspark.sql import DataFrame
from pyspark.sql.functions import col


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
    """Retorna registros onde o valor não está na lista de valores permitidos."""
    return df.filter(~col(column).isin(valid_values))


def check_no_duplicates(df: DataFrame, key_column: str) -> DataFrame:
    """
    Retorna os grupos de key_column que aparecem mais de uma vez.
    Usado para validar a unicidade da chave primária (grain) de uma tabela.
    """
    from pyspark.sql.functions import count
    return (
        df.groupBy(key_column)
        .agg(count("*").alias("occurrences"))
        .filter(col("occurrences") > 1)
    )
