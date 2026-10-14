from relatorio_pedidos.config.settings import Settings


def test_padroes_quando_nao_ha_yaml(tmp_path, monkeypatch):
    monkeypatch.delenv("RELATORIO_CONFIG", raising=False)
    monkeypatch.chdir(tmp_path)

    settings = Settings.load()

    assert settings.ano_relatorio == 2025
    assert settings.output_path == "data/output/relatorio_pedidos"


def test_yaml_sobrescreve_padroes(tmp_path):
    arquivo = tmp_path / "s.yaml"
    arquivo.write_text(
        "paths:\n  output: /tmp/saida\nnegocio:\n  ano_relatorio: 2024\n"
        "file_options:\n  pedidos_csv:\n    sep: ','\n",
        encoding="utf-8",
    )

    settings = Settings.load(str(arquivo))

    assert settings.output_path == "/tmp/saida"
    assert settings.ano_relatorio == 2024
    assert settings.pedidos_sep == ","
    assert settings.master == "local[*]"


def test_variaveis_de_ambiente_tem_precedencia_sobre_o_yaml(tmp_path, monkeypatch):
    arquivo = tmp_path / "s.yaml"
    arquivo.write_text("paths:\n  output: do-yaml\n", encoding="utf-8")
    monkeypatch.setenv("OUTPUT_PATH", "do-ambiente")
    monkeypatch.setenv("ANO_RELATORIO", "2023")

    settings = Settings.load(str(arquivo))

    assert settings.output_path == "do-ambiente"
    assert settings.ano_relatorio == 2023
