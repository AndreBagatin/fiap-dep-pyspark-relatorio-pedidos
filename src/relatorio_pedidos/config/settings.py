"""Configurações centralizadas do projeto.

Ordem de precedência (da menor para a maior):
1. valores padrão definidos na classe ``Settings``;
2. arquivo YAML (``config/settings.yaml`` ou o indicado em ``RELATORIO_CONFIG``);
3. variáveis de ambiente (``PEDIDOS_PATH``, ``PAGAMENTOS_PATH``, ``OUTPUT_PATH``,
   ``ANO_RELATORIO``).
"""

import os
from dataclasses import dataclass, replace
from pathlib import Path

import yaml


@dataclass(frozen=True)
class Settings:
    """Parâmetros de execução do pipeline."""

    # Spark
    app_name: str = "relatorio-pedidos-recusados-legitimos"
    master: str = "local[*]"
    shuffle_partitions: int = 8
    timezone: str = "UTC"

    # Entrada
    pedidos_path: str = "data/input/datasets-csv-pedidos/data/pedidos/"
    pagamentos_path: str = "data/input/dataset-json-pagamentos/data/pagamentos/"

    # Opções de leitura
    pedidos_sep: str = ";"
    pedidos_header: bool = True
    pedidos_compression: str = "gzip"
    pagamentos_compression: str = "gzip"
    read_mode: str = "FAILFAST"

    # Saída
    output_path: str = "data/output/relatorio_pedidos"
    output_mode: str = "overwrite"
    output_partitions: int = 1

    # Regras de negócio
    ano_relatorio: int = 2025

    ENV_CONFIG = "RELATORIO_CONFIG"
    DEFAULT_CONFIG_FILE = "config/settings.yaml"

    @classmethod
    def load(cls, path: str | None = None) -> "Settings":
        """Carrega as configurações combinando padrões, YAML e variáveis de ambiente."""
        settings = cls()
        yaml_path = Path(path or os.environ.get(cls.ENV_CONFIG) or cls.DEFAULT_CONFIG_FILE)
        if yaml_path.is_file():
            with open(yaml_path, "r", encoding="utf-8") as file:
                settings = cls._from_dict(settings, yaml.safe_load(file) or {})
        return cls._from_env(settings)

    @staticmethod
    def _from_dict(base: "Settings", raw: dict) -> "Settings":
        spark = raw.get("spark", {})
        paths = raw.get("paths", {})
        pedidos_csv = raw.get("file_options", {}).get("pedidos_csv", {})
        pagamentos_json = raw.get("file_options", {}).get("pagamentos_json", {})
        output = raw.get("output", {})
        negocio = raw.get("negocio", {})
        valores = {
            "app_name": spark.get("app_name"),
            "master": spark.get("master"),
            "shuffle_partitions": spark.get("shuffle_partitions"),
            "timezone": spark.get("timezone"),
            "pedidos_path": paths.get("pedidos"),
            "pagamentos_path": paths.get("pagamentos"),
            "pedidos_sep": pedidos_csv.get("sep"),
            "pedidos_header": pedidos_csv.get("header"),
            "pedidos_compression": pedidos_csv.get("compression"),
            "pagamentos_compression": pagamentos_json.get("compression"),
            "read_mode": raw.get("file_options", {}).get("mode"),
            "output_path": paths.get("output"),
            "output_mode": output.get("mode"),
            "output_partitions": output.get("partitions"),
            "ano_relatorio": negocio.get("ano_relatorio"),
        }
        return replace(base, **{k: v for k, v in valores.items() if v is not None})

    @staticmethod
    def _from_env(base: "Settings") -> "Settings":
        env = os.environ
        valores = {
            "pedidos_path": env.get("PEDIDOS_PATH"),
            "pagamentos_path": env.get("PAGAMENTOS_PATH"),
            "output_path": env.get("OUTPUT_PATH"),
            "ano_relatorio": int(env["ANO_RELATORIO"]) if env.get("ANO_RELATORIO") else None,
        }
        return replace(base, **{k: v for k, v in valores.items() if v is not None})
