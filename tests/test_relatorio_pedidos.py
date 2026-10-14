import logging
from datetime import datetime

import pytest
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from relatorio_pedidos.business.relatorio_pedidos import RelatorioPedidos, RelatorioPedidosError


@pytest.fixture
def relatorio():
    return RelatorioPedidos(ano=2025)


def test_filtra_apenas_pagamentos_recusados_e_legitimos(relatorio, make_pagamentos):
    pagamentos = make_pagamentos(
        [
            ("recusado-legitimo", "PIX", False, False),
            ("recusado-fraude", "PIX", False, True),
            ("aprovado-legitimo", "PIX", True, False),
            ("aprovado-fraude", "PIX", True, True),
            ("status-nulo", "PIX", None, False),
            ("fraude-nula", "PIX", False, None),
        ]
    )

    ids = [r.id_pedido for r in relatorio.filtrar_pagamentos_recusados_legitimos(pagamentos).collect()]

    assert ids == ["recusado-legitimo"]


def test_filtra_apenas_pedidos_de_2025(relatorio, make_pedidos):
    pedidos = make_pedidos(
        [
            ("fim-2024", 10, 1, datetime(2024, 12, 31, 23, 59, 59), "SP"),
            ("inicio-2025", 10, 1, datetime(2025, 1, 1, 0, 0, 0), "SP"),
            ("fim-2025", 10, 1, datetime(2025, 12, 31, 23, 59, 59), "SP"),
            ("inicio-2026", 10, 1, datetime(2026, 1, 1, 0, 0, 0), "SP"),
        ]
    )

    ids = sorted(r.id_pedido for r in relatorio.filtrar_ano(pedidos).collect())

    assert ids == ["fim-2025", "inicio-2025"]


def test_ano_do_filtro_vem_da_injecao(make_pedidos):
    pedidos = make_pedidos(
        [
            ("a", 10, 1, datetime(2024, 5, 1), "SP"),
            ("b", 10, 1, datetime(2025, 5, 1), "SP"),
        ]
    )

    ids = [r.id_pedido for r in RelatorioPedidos(ano=2024).filtrar_ano(pedidos).collect()]

    assert ids == ["a"]


def test_calcula_valor_total_como_valor_unitario_vezes_quantidade(relatorio, make_pedidos):
    pedidos = make_pedidos([("p1", 150.5, 3, datetime(2025, 3, 1), "RJ")])

    linha = relatorio.calcular_valor_total(pedidos).collect()[0]

    assert linha.valor_total == pytest.approx(451.5)


def test_join_descarta_pedidos_sem_pagamento_filtrado(relatorio, make_pedidos, make_pagamentos):
    pedidos = make_pedidos(
        [
            ("p1", 10, 1, datetime(2025, 3, 1), "SP"),
            ("p2", 10, 1, datetime(2025, 3, 2), "SP"),
        ]
    )
    pagamentos = make_pagamentos([("p1", "PIX", False, False)])

    ids = [r.id_pedido for r in relatorio.juntar(pedidos, pagamentos).collect()]

    assert ids == ["p1"]


def test_colunas_finais_do_relatorio(relatorio, make_pedidos, make_pagamentos):
    pedidos = relatorio.calcular_valor_total(make_pedidos([("p1", 10, 2, datetime(2025, 3, 1), "SP")]))
    pagamentos = make_pagamentos([("p1", "BOLETO", False, False)])

    df = relatorio.selecionar_colunas(relatorio.juntar(pedidos, pagamentos))

    assert df.columns == ["id_pedido", "uf", "forma_pagamento", "valor_total", "data_pedido"]
    assert df.columns == RelatorioPedidos.COLUNAS_RELATORIO


def test_ordena_por_uf_forma_pagamento_e_data(relatorio, spark):
    schema = StructType(
        [
            StructField("id_pedido", StringType()),
            StructField("uf", StringType()),
            StructField("forma_pagamento", StringType()),
            StructField("valor_total", DoubleType()),
            StructField("data_pedido", TimestampType()),
        ]
    )
    df = spark.createDataFrame(
        [
            ("d", "SP", "PIX", 1.0, datetime(2025, 1, 1)),
            ("c", "SP", "BOLETO", 1.0, datetime(2025, 2, 1)),
            ("b", "AC", "PIX", 1.0, datetime(2025, 3, 1)),
            ("a", "AC", "PIX", 1.0, datetime(2025, 1, 15)),
        ],
        schema=schema,
    )

    ids = [r.id_pedido for r in relatorio.ordenar(df).collect()]

    assert ids == ["a", "b", "c", "d"]


def test_gerar_relatorio_de_ponta_a_ponta(relatorio, make_pedidos, make_pagamentos):
    pedidos = make_pedidos(
        [
            ("ok-sp", 100, 2, datetime(2025, 6, 1, 10), "SP"),
            ("ok-ac", 50, 3, datetime(2025, 7, 1, 10), "AC"),
            ("aprovado", 100, 1, datetime(2025, 6, 1), "SP"),
            ("fraude", 100, 1, datetime(2025, 6, 1), "SP"),
            ("ano-2024", 100, 1, datetime(2024, 6, 1), "SP"),
        ]
    )
    pagamentos = make_pagamentos(
        [
            ("ok-sp", "PIX", False, False),
            ("ok-ac", "BOLETO", False, False),
            ("aprovado", "PIX", True, False),
            ("fraude", "PIX", False, True),
            ("ano-2024", "PIX", False, False),
        ]
    )

    linhas = relatorio.gerar_relatorio(pedidos, pagamentos).collect()

    assert [(r.id_pedido, r.uf, r.forma_pagamento, r.valor_total) for r in linhas] == [
        ("ok-ac", "AC", "BOLETO", 150.0),
        ("ok-sp", "SP", "PIX", 200.0),
    ]
    assert linhas[0].data_pedido == datetime(2025, 7, 1, 10)


def test_erro_em_etapa_e_logado_e_relancado_de_forma_controlada(relatorio, make_pagamentos, spark, caplog):
    pedidos_sem_data = spark.createDataFrame(
        [("p1", "SP")], StructType([StructField("id_pedido", StringType()), StructField("uf", StringType())])
    )
    pagamentos = make_pagamentos([("p1", "PIX", False, False)])

    with caplog.at_level(logging.ERROR):
        with pytest.raises(RelatorioPedidosError, match="Filtro do ano"):
            relatorio.gerar_relatorio(pedidos_sem_data, pagamentos)

    assert any("Falha na etapa 'Filtro do ano'" in m for m in caplog.messages)


def test_contagens_podem_ser_desligadas(make_pedidos, make_pagamentos, caplog):
    pedidos = make_pedidos([("p1", 10, 1, datetime(2025, 3, 1), "SP")])
    pagamentos = make_pagamentos([("p1", "PIX", False, False)])

    with caplog.at_level(logging.INFO):
        RelatorioPedidos(ano=2025, log_contagens=False).gerar_relatorio(pedidos, pagamentos)

    assert not any("registros" in m for m in caplog.messages)
    assert any(m == "Etapa 'Ordenação' concluída" for m in caplog.messages)


def test_registra_contagem_de_cada_etapa(relatorio, make_pedidos, make_pagamentos, caplog):
    pedidos = make_pedidos([("p1", 10, 1, datetime(2025, 3, 1), "SP")])
    pagamentos = make_pagamentos([("p1", "PIX", False, False)])

    with caplog.at_level(logging.INFO):
        relatorio.gerar_relatorio(pedidos, pagamentos)

    etapas = [m for m in caplog.messages if m.startswith("Etapa '")]
    assert len(etapas) == 6
    assert all("1 registros" in m for m in etapas)
