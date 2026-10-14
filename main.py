"""Aggregation Root: único lugar que instancia as dependências e as injeta."""

import sys

from relatorio_pedidos.business.relatorio_pedidos import RelatorioPedidos
from relatorio_pedidos.config.settings import Settings
from relatorio_pedidos.io_utils.readers import DataReader
from relatorio_pedidos.io_utils.schemas import PagamentosSchema, PedidosSchema
from relatorio_pedidos.io_utils.writers import ParquetWriter
from relatorio_pedidos.pipeline.pipeline import Pipeline
from relatorio_pedidos.session.spark_session_manager import SparkSessionManager


class Application:
    """Monta o grafo de dependências e executa o pipeline."""

    def __init__(self, config_path: str | None = None):
        self._config_path = config_path

    def run(self) -> int:
        """Compõe as dependências e executa o pipeline.

        Falhas são registradas pela lógica de negócio e propagam (o Python termina com código 1);
        a sessão Spark é sempre encerrada.
        """
        settings = Settings.load(self._config_path)
        session_manager = SparkSessionManager(settings)
        try:
            spark = session_manager.get_session()
            Pipeline(
                reader=DataReader(spark, settings, PedidosSchema.get(), PagamentosSchema.get()),
                relatorio=RelatorioPedidos(settings.ano_relatorio, settings.log_contagens),
                writer=ParquetWriter(settings),
            ).run()
            return 0
        finally:
            session_manager.stop()

    @classmethod
    def cli(cls, argv: list[str] | None = None) -> None:
        """Ponto de entrada de linha de comando: [caminho_do_yaml] opcional."""
        args = sys.argv[1:] if argv is None else argv
        sys.exit(cls(config_path=args[0] if args else None).run())


if __name__ == "__main__":
    Application.cli()
