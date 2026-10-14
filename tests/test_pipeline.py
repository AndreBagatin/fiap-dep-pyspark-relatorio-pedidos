from datetime import datetime
from unittest.mock import MagicMock

import pytest

from relatorio_pedidos.business.exceptions import RelatorioPedidosError
from relatorio_pedidos.business.relatorio_pedidos import RelatorioPedidos
from relatorio_pedidos.pipeline.pipeline import Pipeline


def test_pipeline_orquestra_leitura_transformacao_e_escrita(make_pedidos, make_pagamentos):
    reader, writer = MagicMock(), MagicMock()
    reader.read_pedidos.return_value = make_pedidos([("p1", 10, 1, datetime(2025, 3, 1), "SP")])
    reader.read_pagamentos.return_value = make_pagamentos([("p1", "PIX", False, False)])
    writer.write.return_value = "saida"

    caminho = Pipeline(reader, RelatorioPedidos(ano=2025), writer).run()

    assert caminho == "saida"
    reader.read_pedidos.assert_called_once()
    reader.read_pagamentos.assert_called_once()
    df_gravado = writer.write.call_args.args[0]
    assert [r.id_pedido for r in df_gravado.collect()] == ["p1"]


def test_pipeline_nao_grava_quando_a_regra_de_negocio_falha():
    reader, writer, relatorio = MagicMock(), MagicMock(), MagicMock()
    relatorio.gerar_relatorio.side_effect = RelatorioPedidosError("falhou")

    with pytest.raises(RelatorioPedidosError):
        Pipeline(reader, relatorio, writer).run()

    writer.write.assert_not_called()
