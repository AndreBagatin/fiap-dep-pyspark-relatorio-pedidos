"""Aggregation Root: único lugar que instancia as dependências e as injeta."""

import logging
import sys

from relatorio_pedidos.business.relatorio_pedidos import RelatorioPedidos
from relatorio_pedidos.config.settings import Settings
from relatorio_pedidos.io_utils.readers import DataReader
from relatorio_pedidos.io_utils.writers import ParquetWriter
from relatorio_pedidos.pipeline.pipeline import Pipeline
from relatorio_pedidos.session.spark_session_manager import SparkSessionManager

logger = logging.getLogger(__name__)


class Application:
    """Monta o grafo de dependências e executa o pipeline."""

    def __init__(self, config_path: str | None = None):
        self._config_path = config_path

    def run(self) -> int:
        settings = Settings.load(self._config_path)
        session_manager = SparkSessionManager(settings)
        try:
            spark = session_manager.get_session()
            pipeline = Pipeline(
                reader=DataReader(spark, settings),
                relatorio=RelatorioPedidos(settings.ano_relatorio, settings.log_contagens),
                writer=ParquetWriter(settings),
            )
            pipeline.run()
            return 0
        except Exception:
            logger.exception("Execução interrompida por erro")
            return 1
        finally:
            session_manager.stop()


if __name__ == "__main__":
    sys.exit(Application(config_path=sys.argv[1] if len(sys.argv) > 1 else None).run())
