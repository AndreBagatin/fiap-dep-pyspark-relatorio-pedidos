import logging
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from relatorio_pedidos.business.exceptions import RelatorioPedidosError


class RelatorioPedidos:
    """Regras de negócio do relatório de pedidos com pagamento recusado e fraude=false.

    Etapas: filtro do ano -> valor total -> filtro de pagamentos -> join ->
    seleção de colunas -> ordenação.
    """

    _logger = logging.getLogger(__name__)

    COLUNAS_RELATORIO = ["id_pedido", "uf", "forma_pagamento", "valor_total", "data_pedido"]

    def __init__(self, ano: int, log_contagens: bool = True):
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
        self._ano = ano
        self._log_contagens = log_contagens

    def filtrar_pagamentos_recusados_legitimos(self, pagamentos: DataFrame) -> DataFrame:
        """Mantém pagamentos recusados (status=false) avaliados como legítimos (fraude=false)."""
        return pagamentos.filter(
            (F.col("status") == F.lit(False)) & (F.col("avaliacao_fraude.fraude") == F.lit(False))
        )

    def filtrar_ano(self, pedidos: DataFrame) -> DataFrame:
        """Mantém apenas pedidos criados no ano do relatório."""
        return pedidos.filter(F.year(F.col("data_criacao")) == self._ano)

    def calcular_valor_total(self, pedidos: DataFrame) -> DataFrame:
        """Valor total do pedido = valor_unitario * quantidade."""
        return pedidos.withColumn("valor_total", F.col("valor_unitario") * F.col("quantidade"))

    def juntar(self, pedidos: DataFrame, pagamentos: DataFrame) -> DataFrame:
        """Junção interna pedido x pagamento por id_pedido (relação 1:1 nos datasets)."""
        return pedidos.join(pagamentos, on="id_pedido", how="inner")

    def selecionar_colunas(self, df: DataFrame) -> DataFrame:
        """Seleciona as 5 colunas do relatório."""
        return df.select(
            "id_pedido",
            "uf",
            "forma_pagamento",
            "valor_total",
            F.col("data_criacao").alias("data_pedido"),
        )

    def ordenar(self, df: DataFrame) -> DataFrame:
        """Ordena por UF, forma de pagamento e data do pedido (id_pedido desempata)."""
        return df.orderBy("uf", "forma_pagamento", "data_pedido", "id_pedido")

    def gerar_relatorio(self, pedidos: DataFrame, pagamentos: DataFrame) -> DataFrame:
        """Executa todas as etapas e devolve o DataFrame final do relatório."""
        self._logger.info("Iniciando a geração do relatório (ano %s)", self._ano)
        pedidos = self._executar("Filtro do ano", self.filtrar_ano, pedidos)
        pedidos = self._executar("Cálculo do valor total", self.calcular_valor_total, pedidos)
        pagamentos = self._executar(
            "Filtro de pagamentos recusados e legítimos",
            self.filtrar_pagamentos_recusados_legitimos,
            pagamentos,
        )
        df = self._executar("Join pedidos x pagamentos", self.juntar, pedidos, pagamentos)
        df = self._executar("Seleção de colunas", self.selecionar_colunas, df)
        df = self._executar("Ordenação", self.ordenar, df)
        self._logger.info("Relatório gerado com sucesso")
        return df

    def _executar(self, etapa: str, funcao: Callable[..., DataFrame], *args: DataFrame) -> DataFrame:
        """Roda uma etapa registrando a contagem de registros; falhas viram RelatorioPedidosError."""
        try:
            resultado = funcao(*args)
            if self._log_contagens:
                self._logger.info("Etapa '%s' concluída: %d registros", etapa, resultado.count())
            else:
                self._logger.info("Etapa '%s' concluída", etapa)
            return resultado
        except Exception as erro:
            self._logger.error("Falha na etapa '%s': %s", etapa, erro)
            raise RelatorioPedidosError(f"Falha na etapa '{etapa}'") from erro
