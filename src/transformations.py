from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, to_date, datediff, when, lower


def parse_order_dates(df: DataFrame) -> DataFrame:
    """Converte colunas de data de StringType para TimestampType na camada Silver."""
    return (
        df
        .withColumn("order_purchase_timestamp", to_timestamp("order_purchase_timestamp"))
        .withColumn("order_approved_at", to_timestamp("order_approved_at"))
        .withColumn("order_delivered_carrier_date", to_timestamp("order_delivered_carrier_date"))
        .withColumn("order_delivered_customer_date", to_timestamp("order_delivered_customer_date"))
        .withColumn("order_estimated_delivery_date", to_timestamp("order_estimated_delivery_date"))
    )


def add_order_derived_fields(df: DataFrame) -> DataFrame:
    """Adiciona campos calculados: data de compra, dias de entrega, flags de entrega e atraso."""
    return (
        df
        .withColumn("order_purchase_date", to_date("order_purchase_timestamp"))
        .withColumn(
            "delivery_days",
            datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))
        )
        .withColumn(
            "estimated_delivery_days",
            datediff(col("order_estimated_delivery_date"), col("order_purchase_timestamp"))
        )
        .withColumn(
            "is_delivered",
            when(col("order_delivered_customer_date").isNotNull(), True).otherwise(False)
        )
        .withColumn(
            # is_late é null para pedidos não entregues — não False.
            # Isso evita que pedidos em trânsito apareçam como "não atrasados" nas análises.
            "is_late",
            when(
                col("is_delivered") == True,
                when(
                    col("order_delivered_customer_date") > col("order_estimated_delivery_date"),
                    True
                ).otherwise(False)
            )
        )
    )


def standardize_order_status(df: DataFrame) -> DataFrame:
    """Padroniza order_status para lowercase."""
    return df.withColumn("order_status", lower(col("order_status")))
