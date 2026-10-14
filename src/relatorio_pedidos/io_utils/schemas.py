from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class PedidosSchema:
    """Schema explícito do dataset de pedidos (CSV)."""

    @staticmethod
    def get() -> StructType:
        return StructType(
            [
                StructField("id_pedido", StringType(), True),
                StructField("produto", StringType(), True),
                StructField("valor_unitario", DoubleType(), True),
                StructField("quantidade", LongType(), True),
                StructField("data_criacao", TimestampType(), True),
                StructField("uf", StringType(), True),
                StructField("id_cliente", LongType(), True),
            ]
        )


class PagamentosSchema:
    """Schema explícito do dataset de pagamentos (JSON), incluindo o objeto aninhado."""

    @staticmethod
    def avaliacao_fraude() -> StructType:
        return StructType(
            [
                StructField("fraude", BooleanType(), True),
                StructField("score", DoubleType(), True),
            ]
        )

    @staticmethod
    def get() -> StructType:
        return StructType(
            [
                StructField("id_pedido", StringType(), True),
                StructField("forma_pagamento", StringType(), True),
                StructField("valor_pagamento", DoubleType(), True),
                StructField("status", BooleanType(), True),
                StructField("data_processamento", TimestampType(), True),
                StructField("avaliacao_fraude", PagamentosSchema.avaliacao_fraude(), True),
            ]
        )
