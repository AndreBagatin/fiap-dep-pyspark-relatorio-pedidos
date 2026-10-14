"""Baixa os datasets do professor para data/input/ (sem precisar de git).

Uso:  python scripts/download_datasets.py
"""

import io
import sys
import urllib.request
import zipfile
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
DESTINO = RAIZ / "data" / "input"
REPOSITORIOS = {
    "dataset-json-pagamentos": "data/pagamentos/",
    "datasets-csv-pedidos": "data/pedidos/",
}
URL = "https://codeload.github.com/infobarbosa/{repo}/zip/refs/heads/main"


class DatasetDownloader:
    """Baixa o zip de cada repositório e extrai apenas a pasta de dados."""

    def __init__(self, destino: Path, repositorios: dict[str, str]):
        self._destino = destino
        self._repositorios = repositorios

    def run(self) -> None:
        for repo, subpasta in self._repositorios.items():
            self._baixar(repo, subpasta)

    def _baixar(self, repo: str, subpasta: str) -> None:
        print(f"Baixando {repo} ...")
        with urllib.request.urlopen(URL.format(repo=repo), timeout=120) as resposta:
            conteudo = resposta.read()
        extraidos = 0
        with zipfile.ZipFile(io.BytesIO(conteudo)) as zf:
            for nome in zf.namelist():
                relativo = nome.split("/", 1)[1] if "/" in nome else ""
                if relativo.startswith(subpasta) and not nome.endswith("/"):
                    alvo = self._destino / repo / relativo
                    alvo.parent.mkdir(parents=True, exist_ok=True)
                    alvo.write_bytes(zf.read(nome))
                    extraidos += 1
        print(f"  {extraidos} arquivos em {self._destino / repo / subpasta}")


if __name__ == "__main__":
    try:
        DatasetDownloader(DESTINO, REPOSITORIOS).run()
    except OSError as erro:
        print(f"Falha no download: {erro}", file=sys.stderr)
        sys.exit(1)
