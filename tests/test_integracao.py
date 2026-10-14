import gzip
import json
from dataclasses import replace

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from main import Application
from relatorio_pedidos.config.settings import Settings
from relatorio_pedidos.io_utils.readers import DataReader
from relatorio_pedidos.io_utils.schemas import PagamentosSchema, PedidosSchema
from relatorio_pedidos.io_utils.writers import ParquetWriter

CABECALHO = "ID_PEDIDO;PRODUTO;VALOR_UNITARIO;QUANTIDADE;DATA_CRIACAO;UF;ID_CLIENTE\n"


@pytest.fixture
def dados(tmp_path):
    """Cria mini datasets (CSV e JSON gzip) e devolve as Settings apontando para eles."""
    pedidos_dir, pagamentos_dir = tmp_path / "pedidos", tmp_path / "pagamentos"
    pedidos_dir.mkdir()
    pagamentos_dir.mkdir()
    csv = CABECALHO + (
        "p1;NOTEBOOK;1500.0;2;2025-03-10T10:00:00;SP;1\n"
        "p2;MONITOR;600.0;1;2025-03-11T10:00:00;AC;2\n"
        "p3;MONITOR;600.0;1;2024-03-11T10:00:00;AC;3\n"
    )
    with gzip.open(pedidos_dir / "pedidos-2025-03.csv.gz", "wt", encoding="utf-8") as f:
        f.write(csv)
    pagamentos = [
        {
            "id_pedido": "p1",
            "forma_pagamento": "PIX",
            "valor_pagamento": 2850.0,
            "status": False,
            "data_processamento": "2025-03-10T23:31:59.888910",
            "avaliacao_fraude": {"fraude": False, "score": 0.2},
        },
        {
            "id_pedido": "p2",
            "forma_pagamento": "BOLETO",
            "valor_pagamento": 600.0,
            "status": True,
            "data_processamento": "2025-03-11T23:31:59.888910",
            "avaliacao_fraude": {"fraude": False, "score": 0.1},
        },
        {
            "id_pedido": "p3",
            "forma_pagamento": "PIX",
            "valor_pagamento": 600.0,
            "status": False,
            "data_processamento": "2024-03-11T23:31:59.888910",
            "avaliacao_fraude": {"fraude": False, "score": 0.1},
        },
    ]
    with gzip.open(pagamentos_dir / "pagamentos-2025-03.json.gz", "wt", encoding="utf-8") as f:
        f.write("\n".join(json.dumps(p) for p in pagamentos) + "\n")
    return replace(
        Settings(),
        pedidos_path=str(pedidos_dir),
        pagamentos_path=str(pagamentos_dir),
        output_path=str(tmp_path / "saida"),
        shuffle_partitions=1,
    )


def test_schemas_sao_explicitos_e_incluem_o_objeto_aninhado(spark, dados):
    reader = DataReader(spark, dados)

    pedidos, pagamentos = reader.read_pedidos(), reader.read_pagamentos()

    assert pedidos.schema == PedidosSchema.get()
    assert pagamentos.schema == PagamentosSchema.get()
    assert isinstance(pagamentos.schema["avaliacao_fraude"].dataType, StructType)
    assert pedidos.count() == 3  # cabeçalho não vira registro


def test_escrita_em_parquet_preserva_os_dados(spark, dados):
    reader = DataReader(spark, dados)
    caminho = ParquetWriter(dados).write(reader.read_pedidos())

    lido = spark.read.parquet(caminho)

    assert lido.count() == 3
    assert lido.columns == PedidosSchema.get().fieldNames()


def test_aplicacao_executa_o_pipeline_completo(dados, tmp_path):
    arquivo = tmp_path / "settings.yaml"
    arquivo.write_text(
        f"spark:\n  master: 'local[1]'\n  shuffle_partitions: 1\n"
        f"paths:\n  pedidos: '{dados.pedidos_path.replace(chr(92), '/')}'\n"
        f"  pagamentos: '{dados.pagamentos_path.replace(chr(92), '/')}'\n"
        f"  output: '{dados.output_path.replace(chr(92), '/')}'\n",
        encoding="utf-8",
    )

    codigo = Application(config_path=str(arquivo)).run()

    assert codigo == 0
    # A Application encerra a sessão ao terminar (a fixture ficou obsoleta); abre outra para conferir.
    sessao = SparkSession.builder.master("local[1]").getOrCreate()
    linhas = sessao.read.parquet(dados.output_path).collect()
    assert [(r.id_pedido, r.uf, r.forma_pagamento, r.valor_total) for r in linhas] == [
        ("p1", "SP", "PIX", 3000.0)
    ]


def test_cli_encerra_com_codigo_1_quando_a_entrada_nao_existe(tmp_path):
    arquivo = tmp_path / "settings.yaml"
    arquivo.write_text(
        f"spark:\n  master: 'local[1]'\npaths:\n  pedidos: '{(tmp_path / 'nao-existe').as_posix()}'\n",
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as saida:
        Application.cli([str(arquivo)])

    assert saida.value.code == 1


def test_aplicacao_retorna_erro_quando_a_entrada_nao_existe(tmp_path):
    arquivo = tmp_path / "settings.yaml"
    arquivo.write_text(
        f"spark:\n  master: 'local[1]'\npaths:\n  pedidos: '{(tmp_path / 'nao-existe').as_posix()}'\n"
        f"  output: '{(tmp_path / 'out').as_posix()}'\n",
        encoding="utf-8",
    )

    assert Application(config_path=str(arquivo)).run() == 1
