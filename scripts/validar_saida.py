"""Confere o parquet gerado contra os datasets de origem.

Uso:  python scripts/validar_saida.py
"""

import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ / "src"))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from relatorio_pedidos.business.relatorio_pedidos import RelatorioPedidos  # noqa: E402
from relatorio_pedidos.config.settings import Settings  # noqa: E402
from relatorio_pedidos.io_utils.readers import DataReader  # noqa: E402
from relatorio_pedidos.io_utils.schemas import PagamentosSchema, PedidosSchema  # noqa: E402


class ValidadorSaida:
    """Verificações independentes da lógica de negócio sobre o parquet de saída."""

    def __init__(self, spark: SparkSession, settings: Settings):
        self._spark = spark
        self._settings = settings

    def run(self) -> bool:
        saida = self._spark.read.parquet(self._settings.output_path)
        reader = DataReader(self._spark, self._settings, PedidosSchema.get(), PagamentosSchema.get())
        pedidos, pagamentos = reader.read_pedidos(), reader.read_pagamentos()
        bruto = saida.join(pedidos, "id_pedido").join(pagamentos, "id_pedido")
        linhas = saida.collect()
        chaves = [(r.uf, r.forma_pagamento, r.data_pedido) for r in linhas]

        esperado = (
            pedidos.join(pagamentos, "id_pedido")
            .filter(
                (F.year("data_criacao") == self._settings.ano_relatorio)
                & (F.col("status") == F.lit(False))
                & (F.col("avaliacao_fraude.fraude") == F.lit(False))
            )
            .count()
        )
        verificacoes = {
            "colunas == " + str(RelatorioPedidos.COLUNAS_RELATORIO): (
                saida.columns == RelatorioPedidos.COLUNAS_RELATORIO
            ),
            f"contagem == {esperado} (cálculo independente)": len(linhas) == esperado,
            "id_pedido sem duplicidade": saida.select("id_pedido").distinct().count() == len(linhas),
            "todos os pedidos de " + str(self._settings.ano_relatorio): (
                saida.filter(F.year("data_pedido") != self._settings.ano_relatorio).count() == 0
            ),
            "todos com status=false e fraude=false": (
                bruto.filter(
                    (F.col("status") != F.lit(False)) | (F.col("avaliacao_fraude.fraude") != F.lit(False))
                ).count()
                == 0
            ),
            "valor_total == valor_unitario * quantidade": (
                bruto.filter(F.col("valor_total") != F.col("valor_unitario") * F.col("quantidade")).count()
                == 0
            ),
            "ordenado por uf, forma_pagamento, data_pedido": chaves == sorted(chaves),
        }
        for descricao, ok in verificacoes.items():
            print(f"[{'OK' if ok else 'FALHOU'}] {descricao}")
        print(f"\nTotal de linhas: {len(linhas)}")
        saida.printSchema()
        saida.show(8, truncate=False)
        return all(verificacoes.values())


if __name__ == "__main__":
    configuracao = Settings.load()
    sessao = (
        SparkSession.builder.appName("validar-saida")
        .master(configuracao.master)
        .config("spark.sql.shuffle.partitions", configuracao.shuffle_partitions)
        .config("spark.sql.session.timeZone", configuracao.timezone)
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    sessao.sparkContext.setLogLevel("ERROR")
    try:
        sys.exit(0 if ValidadorSaida(sessao, configuracao).run() else 1)
    finally:
        sessao.stop()
