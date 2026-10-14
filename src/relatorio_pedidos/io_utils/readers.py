from pyspark.sql import DataFrame, SparkSession

from relatorio_pedidos.config.settings import Settings
from relatorio_pedidos.io_utils.schemas import PagamentosSchema, PedidosSchema


class DataReader:
    """Lê os datasets de entrada sempre com schema explícito (sem inferência)."""

    def __init__(self, spark: SparkSession, settings: Settings):
        self._spark = spark
        self._settings = settings

    def read_pedidos(self) -> DataFrame:
        """Lê os pedidos (CSV com separador e cabeçalho definidos na configuração)."""
        return (
            self._spark.read.schema(PedidosSchema.get())
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
            self._spark.read.schema(PagamentosSchema.get())
            .option("compression", self._settings.pagamentos_compression)
            .option("mode", self._settings.read_mode)
            .json(self._settings.pagamentos_path)
        )
