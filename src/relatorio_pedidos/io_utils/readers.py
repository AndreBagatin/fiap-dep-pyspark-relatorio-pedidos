from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from relatorio_pedidos.config.settings import Settings


class DataReader:
    """Lê os datasets de entrada sempre com schema explícito (sem inferência)."""

    def __init__(
        self,
        spark: SparkSession,
        settings: Settings,
        pedidos_schema: StructType,
        pagamentos_schema: StructType,
    ):
        self._spark = spark
        self._settings = settings
        self._pedidos_schema = pedidos_schema
        self._pagamentos_schema = pagamentos_schema

    def read_pedidos(self) -> DataFrame:
        """Lê os pedidos (CSV com separador e cabeçalho definidos na configuração)."""
        return (
            self._spark.read.schema(self._pedidos_schema)
            .option("compression", self._settings.pedidos_compression)
            .option("mode", self._settings.read_mode)
            .csv(
                self._settings.pedidos_path,
                header=self._settings.pedidos_header,
                sep=self._settings.pedidos_sep,
            )
        )

    def read_pagamentos(self) -> DataFrame:
        """Lê os pagamentos (JSON, um objeto por linha, com avaliacao_fraude aninhada)."""
        return (
            self._spark.read.schema(self._pagamentos_schema)
            .option("compression", self._settings.pagamentos_compression)
            .option("mode", self._settings.read_mode)
            .json(self._settings.pagamentos_path)
        )
