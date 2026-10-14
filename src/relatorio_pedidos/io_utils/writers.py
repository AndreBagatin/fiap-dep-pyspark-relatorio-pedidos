from pyspark.sql import DataFrame

from relatorio_pedidos.config.settings import Settings


class ParquetWriter:
    """Grava o relatório em formato parquet."""

    def __init__(self, settings: Settings):
        self._settings = settings

    def write(self, df: DataFrame) -> str:
        """Grava o DataFrame e devolve o caminho de saída.

        O ``coalesce`` é aplicado depois da ordenação e não a desfaz: ele apenas
        junta partições vizinhas, preservando a ordem global do relatório.
        """
        (
            df.coalesce(self._settings.output_partitions)
            .write.mode(self._settings.output_mode)
            .parquet(self._settings.output_path)
        )
        return self._settings.output_path
