from pyspark.sql import SparkSession

from relatorio_pedidos.config.settings import Settings


class SparkSessionManager:
    """Gerencia o ciclo de vida (criar, obter e encerrar) da SparkSession."""

    def __init__(self, settings: Settings):
        self._settings = settings
        self._spark: SparkSession | None = None

    def get_session(self) -> SparkSession:
        """Cria a sessão na primeira chamada e a reutiliza nas seguintes."""
        if self._spark is None:
            self._spark = (
                SparkSession.builder.appName(self._settings.app_name)
                .master(self._settings.master)
                .config("spark.sql.shuffle.partitions", self._settings.shuffle_partitions)
                .config("spark.sql.session.timeZone", self._settings.timezone)
                .config("spark.ui.showConsoleProgress", "false")
                .getOrCreate()
            )
            self._spark.sparkContext.setLogLevel("WARN")
        return self._spark

    def stop(self) -> None:
        """Encerra a sessão, se estiver ativa."""
        if self._spark is not None:
            self._spark.stop()
            self._spark = None
