import os
import sys
from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from relatorio_pedidos.io_utils.schemas import PagamentosSchema, PedidosSchema

# Garante que os workers Python do Spark usem o mesmo interpretador dos testes.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    """Uma única SparkSession local compartilhada por toda a suíte de testes."""
    sessao = (
        SparkSession.builder.appName("testes-relatorio-pedidos")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    sessao.sparkContext.setLogLevel("ERROR")
    return sessao


@pytest.fixture(autouse=True)
def fuso_da_sessao_preservado(spark):
    """Restaura o fuso da sessão compartilhada (a Application o altera ao reaproveitá-la)."""
    original = spark.conf.get("spark.sql.session.timeZone")
    yield
    spark.conf.set("spark.sql.session.timeZone", original)


def pytest_sessionfinish(session, exitstatus):
    ativa = SparkSession.getActiveSession()
    if ativa is not None:
        ativa.stop()


@pytest.fixture
def make_pedidos(spark):
    """Cria DataFrame de pedidos com o schema explícito do projeto.

    Cada linha: (id_pedido, valor_unitario, quantidade, data_criacao, uf).
    """

    def _make(linhas):
        dados = [(i, "PRODUTO", float(v), int(q), d, uf, 1) for i, v, q, d, uf in linhas]
        return spark.createDataFrame(dados, schema=PedidosSchema.get())

    return _make


@pytest.fixture
def make_pagamentos(spark):
    """Cria DataFrame de pagamentos com o schema explícito do projeto.

    Cada linha: (id_pedido, forma_pagamento, status, fraude).
    """

    def _make(linhas):
        dados = [
            (i, forma, 100.0, status, datetime(2025, 1, 1), (fraude, 0.5))
            for i, forma, status, fraude in linhas
        ]
        return spark.createDataFrame(dados, schema=PagamentosSchema.get())

    return _make
