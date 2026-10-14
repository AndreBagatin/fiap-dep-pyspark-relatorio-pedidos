from typing import Protocol

from pyspark.sql import DataFrame


class DataSource(Protocol):
    """Contrato de quem fornece os DataFrames de entrada (ex.: DataReader)."""

    def read_pedidos(self) -> DataFrame: ...

    def read_pagamentos(self) -> DataFrame: ...


class ReportBuilder(Protocol):
    """Contrato da lógica de negócio que produz o relatório (ex.: RelatorioPedidos)."""

    def gerar_relatorio(self, pedidos: DataFrame, pagamentos: DataFrame) -> DataFrame: ...


class DataSink(Protocol):
    """Contrato de quem grava o resultado (ex.: ParquetWriter)."""

    def write(self, df: DataFrame) -> str: ...
