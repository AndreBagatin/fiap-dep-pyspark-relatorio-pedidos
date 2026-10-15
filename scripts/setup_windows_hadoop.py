"""Windows nativo: baixa winutils.exe e hadoop.dll para tools/hadoop/bin.

Uso:  python scripts/setup_windows_hadoop.py
Depois, em cada sessão do PowerShell, rode o comando impresso ao final
(define HADOOP_HOME e o PATH). Linux, macOS, WSL e Codespaces não precisam disto.
"""

import hashlib
import sys
import urllib.request
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
DESTINO = RAIZ / "tools" / "hadoop" / "bin"
URL = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/{arquivo}"
ARQUIVOS = ("winutils.exe", "hadoop.dll")
SHA256 = {
    "winutils.exe": "496a591eb1e67df2a620f710d529ba6ddfe1c19149e6647cc4e320bb0efd8553",
    "hadoop.dll": "d7ab36a68518748cef142be2da5069b4c763c2cd764c1d2e6ac48c7200405be3",
}


class HadoopWindowsInstaller:
    """Baixa os binários nativos do Hadoop exigidos pelo Spark no Windows."""

    def __init__(self, destino: Path, arquivos: tuple[str, ...]):
        self._destino = destino
        self._arquivos = arquivos

    def run(self) -> None:
        self._destino.mkdir(parents=True, exist_ok=True)
        for arquivo in self._arquivos:
            alvo = self._destino / arquivo
            if alvo.exists():
                print(f"{arquivo} já existe, mantido.")
                continue
            print(f"Baixando {arquivo} ...")
            with urllib.request.urlopen(URL.format(arquivo=arquivo), timeout=120) as resposta:
                conteudo = resposta.read()
            if hashlib.sha256(conteudo).hexdigest() != SHA256[arquivo]:
                raise OSError(f"checksum SHA-256 de {arquivo} diferente do esperado; arquivo descartado")
            alvo.write_bytes(conteudo)
        home = self._destino.parent
        print("\nEm cada sessão do PowerShell, antes de rodar o pipeline ou os testes:")
        print(f'  $env:HADOOP_HOME = "{home}"')
        print(r'  $env:Path = "$env:HADOOP_HOME\bin;$env:Path"')


if __name__ == "__main__":
    try:
        HadoopWindowsInstaller(DESTINO, ARQUIVOS).run()
    except OSError as erro:
        print(f"Falha no download: {erro}", file=sys.stderr)
        sys.exit(1)
