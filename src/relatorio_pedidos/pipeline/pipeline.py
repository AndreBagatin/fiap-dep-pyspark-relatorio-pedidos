import logging

from relatorio_pedidos.business.relatorio_pedidos import RelatorioPedidos
from relatorio_pedidos.io_utils.readers import DataReader
from relatorio_pedidos.io_utils.writers import ParquetWriter

logger = logging.getLogger(__name__)


class Pipeline:
    """Orquestra leitura -> transformação -> escrita.

    Não cria suas dependências: todas chegam prontas pelo construtor,
    montadas no Aggregation Root (main.py).
    """

    def __init__(self, reader: DataReader, relatorio: RelatorioPedidos, writer: ParquetWriter):
        self._reader = reader
        self._relatorio = relatorio
        self._writer = writer

    def run(self) -> str:
        """Executa o pipeline completo e devolve o caminho do parquet gerado."""
        logger.info("Pipeline iniciado")
        pedidos = self._reader.read_pedidos()
        pagamentos = self._reader.read_pagamentos()
        df = self._relatorio.gerar_relatorio(pedidos, pagamentos)
        caminho = self._writer.write(df)
        logger.info("Pipeline concluído. Parquet gravado em: %s", caminho)
        return caminho
