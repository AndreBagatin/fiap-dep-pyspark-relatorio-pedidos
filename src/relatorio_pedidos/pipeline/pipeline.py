import logging

from relatorio_pedidos.pipeline.contracts import DataSink, DataSource, ReportBuilder


class Pipeline:
    """Orquestra leitura -> transformação -> escrita.

    Depende apenas de contratos (Protocol) e não cria suas dependências:
    todas chegam prontas pelo construtor, montadas no Aggregation Root (main.py).
    """

    _logger = logging.getLogger("relatorio_pedidos.pipeline")

    def __init__(self, reader: DataSource, relatorio: ReportBuilder, writer: DataSink):
        self._reader = reader
        self._relatorio = relatorio
        self._writer = writer

    def run(self) -> str:
        """Executa o pipeline completo e devolve o caminho do parquet gerado."""
        self._logger.info("Pipeline iniciado")
        pedidos = self._reader.read_pedidos()
        pagamentos = self._reader.read_pagamentos()
        df = self._relatorio.gerar_relatorio(pedidos, pagamentos)
        caminho = self._writer.write(df)
        self._logger.info("Pipeline concluído. Parquet gravado em: %s", caminho)
        return caminho
