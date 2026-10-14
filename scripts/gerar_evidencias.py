"""Gera as evidências (imagens com no máximo 20 linhas) e o PDF de entrega.

Executa o pipeline, o validador e o pytest, captura as saídas e monta
entrega/Trabalho_Final_DEP.pdf. Requer as dependências de requirements.txt
e os datasets em data/input/ (scripts/download_datasets.py).

Uso:  python scripts/gerar_evidencias.py
"""

import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from pygments import highlight
from pygments.formatters import ImageFormatter
from pygments.lexers import get_lexer_for_filename
from pygments.lexers.special import TextLexer
from pygments.util import ClassNotFound
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (
    Image,
    KeepTogether,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

RAIZ = Path(__file__).resolve().parent.parent
IMAGENS = RAIZ / "entrega" / "imagens"
PDF = RAIZ / "entrega" / "Trabalho_Final_DEP.pdf"
MAX_LINHAS = 20
FONTE_PX = 22
LARGURA_UTIL_PT = landscape(A4)[0] - 2 * 1.5 * cm
REPO_URL = "https://github.com/AndreBagatin/fiap-dep-pyspark-relatorio-pedidos"
INTEGRANTES = [
    ("Priscila Cristina Mendes Cardoso", "373153"),
    ("Andre Victor Bagatin", "374773"),
    ("Felipe Nogueira da Silva", "373111"),
    ("Heloisa Cordeiro Silva", "373370"),
    ("Leonardo Machado de Andrade", "373150"),
    ("Nicolas Galhardo Lhen", "374205"),
]
ARQUIVOS_CODIGO = [
    ("main.py", "Aggregation Root: instancia e injeta todas as dependências"),
    ("config/settings.yaml", "Configurações externas (caminhos, opções de leitura, ano)"),
    ("src/relatorio_pedidos/config/settings.py", "Pacote config: classe Settings"),
    ("src/relatorio_pedidos/session/spark_session_manager.py", "Pacote session: SparkSessionManager"),
    ("src/relatorio_pedidos/io_utils/schemas.py", "Schemas explícitos (StructType), inclusive aninhado"),
    ("src/relatorio_pedidos/io_utils/readers.py", "Pacote io_utils: leitura de CSV e JSON com schema"),
    ("src/relatorio_pedidos/io_utils/writers.py", "Pacote io_utils: escrita em parquet"),
    ("src/relatorio_pedidos/business/relatorio_pedidos.py", "Pacote business: regras, logging e try/except"),
    ("src/relatorio_pedidos/pipeline/pipeline.py", "Pacote pipeline: orquestração"),
    ("pyproject.toml", "Empacotamento: pyproject.toml"),
    ("requirements.txt", "Empacotamento: requirements.txt"),
    ("MANIFEST.in", "Empacotamento: MANIFEST.in"),
    ("tests/conftest.py", "Testes: fixtures com schema explícito"),
    ("tests/test_relatorio_pedidos.py", "Testes unitários da lógica de negócio"),
    ("tests/test_pipeline.py", "Testes da orquestração com dependências injetadas (mocks)"),
]
RUIDO = (
    "WARN",
    "Using Spark's default log4j",
    "To adjust logging level",
    "Closing down clientserver",
    "XITO: o processo",
    "Setting default log level",
)


@dataclass
class Evidencia:
    titulo: str
    legenda: str
    imagem: Path


class ImageMaker:
    """Cria imagens de código/saída de terminal com no máximo MAX_LINHAS linhas."""

    def __init__(self, destino: Path):
        self._destino = destino
        self._destino.mkdir(parents=True, exist_ok=True)
        self._contador = 0

    def codigo(self, caminho: str, descricao: str) -> list[Evidencia]:
        linhas = (RAIZ / caminho).read_text(encoding="utf-8").splitlines()
        try:
            lexer = get_lexer_for_filename(caminho)
        except ClassNotFound:
            lexer = TextLexer()
        blocos = self._dividir(linhas)
        return [
            self._gerar(
                "\n".join(linhas[ini - 1 : fim]),
                lexer,
                ini,
                f"{descricao}",
                f"{caminho} - linhas {ini} a {fim}",
                "default",
            )
            for ini, fim in blocos
        ]

    def terminal(self, titulo: str, comando: str, texto: str) -> list[Evidencia]:
        linhas = [linha.rstrip() for linha in texto.splitlines() if linha.strip()]
        partes = -(-len(linhas) // MAX_LINHAS)  # divisão em partes de tamanho parecido
        tamanho = -(-len(linhas) // partes)
        blocos = [(i + 1, min(i + tamanho, len(linhas))) for i in range(0, len(linhas), tamanho)]
        return [
            self._gerar(
                "\n".join(linhas[ini - 1 : fim]),
                TextLexer(),
                ini,
                titulo,
                f"{comando} - linhas {ini} a {fim}",
                "monokai",
                numeros=False,
            )
            for ini, fim in blocos
        ]

    @staticmethod
    def _dividir(linhas: list[str]) -> list[tuple[int, int]]:
        """Divide em blocos de até MAX_LINHAS linhas, preferindo cortar em linhas em branco."""
        blocos, inicio, total = [], 0, len(linhas)
        while inicio < total:
            fim = min(inicio + MAX_LINHAS, total)
            if fim < total:
                for corte in range(fim, inicio + MAX_LINHAS - 13, -1):
                    if not linhas[corte - 1].strip():
                        fim = corte
                        break
            trecho = linhas[inicio:fim]
            while trecho and not trecho[0].strip():
                inicio += 1
                trecho = trecho[1:]
            while trecho and not trecho[-1].strip():
                trecho = trecho[:-1]
            if trecho:
                blocos.append((inicio + 1, inicio + len(trecho)))
            inicio = fim
        return blocos

    def _gerar(self, texto, lexer, inicio, titulo, legenda, estilo, numeros=True) -> Evidencia:
        assert texto.count("\n") + 1 <= MAX_LINHAS
        self._contador += 1
        caminho = self._destino / f"evidencia_{self._contador:03d}.png"
        formatador = ImageFormatter(
            style=estilo,
            font_size=FONTE_PX,
            line_numbers=numeros,
            line_number_start=inicio,
            line_pad=4,
            image_pad=12,
        )
        caminho.write_bytes(highlight(texto + "\n", lexer, formatador))
        return Evidencia(titulo, legenda, caminho)


class Executor:
    """Executa comandos do projeto e captura a saída sem o ruído do Spark/JVM."""

    def run(self, args: list[str]) -> str:
        proc = subprocess.run(
            args, cwd=RAIZ, capture_output=True, text=True, encoding="utf-8", errors="replace"
        )
        saida = proc.stdout + "\n" + proc.stderr
        linhas = [ln for ln in saida.splitlines() if not any(r in ln for r in RUIDO)]
        if proc.returncode != 0:
            print("\n".join(linhas[-40:]))
            raise SystemExit(f"Comando falhou: {' '.join(args)}")
        return "\n".join(linhas)


class PdfBuilder:
    """Monta o PDF de entrega (paisagem) com capa, resumo e evidências."""

    def __init__(self, evidencias: list[Evidencia]):
        estilos = getSampleStyleSheet()
        self._h1 = ParagraphStyle("h1", parent=estilos["Title"], fontSize=26, leading=32)
        self._h2 = ParagraphStyle("h2", parent=estilos["Heading2"], fontSize=16, spaceBefore=8)
        self._txt = ParagraphStyle("txt", parent=estilos["BodyText"], fontSize=11.5, leading=16)
        self._leg = ParagraphStyle("leg", parent=estilos["BodyText"], fontSize=10, textColor=colors.grey)
        self._evidencias = evidencias

    def build(self) -> None:
        doc = SimpleDocTemplate(
            str(PDF),
            pagesize=landscape(A4),
            leftMargin=1.5 * cm,
            rightMargin=1.5 * cm,
            topMargin=1.3 * cm,
            bottomMargin=1.3 * cm,
            title="Data Engineering Programming - Trabalho Final",
            author="Grupo FIAP",
        )
        doc.build(self._capa() + self._resumo() + self._evidencias_flow())

    def _capa(self) -> list:
        tabela = Table([["Integrante", "RM"], *INTEGRANTES], colWidths=[11 * cm, 4 * cm], hAlign="LEFT")
        tabela.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f3a5f")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("GRID", (0, 0), (-1, -1), 0.4, colors.grey),
                    ("FONTSIZE", (0, 0), (-1, -1), 12),
                    ("TOPPADDING", (0, 0), (-1, -1), 5),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ]
            )
        )
        return [
            Spacer(1, 1.2 * cm),
            Paragraph("Data Engineering Programming", self._h1),
            Paragraph("Trabalho Final - FIAP MBA - Prof. Marcelo Barbosa Pinto", self._h2),
            Paragraph(
                "Relatório de pedidos de 2025 com pagamento recusado (status=false) "
                "e fraude=false, em PySpark, orientado a objetos e com injeção de dependências.",
                self._txt,
            ),
            Spacer(1, 0.6 * cm),
            tabela,
            Spacer(1, 0.8 * cm),
            Paragraph("<b>Repositório público no GitHub:</b>", self._txt),
            Paragraph(REPO_URL, ParagraphStyle("url", parent=self._txt, fontName="Courier", fontSize=14)),
            PageBreak(),
        ]

    def _resumo(self) -> list:
        itens = [
            (
                "Arquitetura e injeção de dependências",
                [
                    "<b>main.py</b> é o Aggregation Root: a classe Application instancia Settings, "
                    "SparkSessionManager, DataReader, RelatorioPedidos, ParquetWriter e Pipeline, "
                    "injetando-os via construtor. Nenhuma outra classe cria suas dependências.",
                    "Pacotes: <b>config</b> (Settings), <b>session</b> (SparkSessionManager), "
                    "<b>io_utils</b> (schemas, DataReader, ParquetWriter), <b>business</b> "
                    "(RelatorioPedidos) e <b>pipeline</b> (Pipeline: leitura, negócio e escrita).",
                    "Todos os DataFrames usam StructType explícito (inclusive o struct aninhado "
                    "avaliacao_fraude); não há inferência de schema nem notebooks.",
                ],
            ),
            (
                "Decisões de negócio",
                [
                    "<b>Valor total do pedido</b> = valor_unitario x quantidade (pedidos). O "
                    "valor_pagamento difere em 21.297 dos 60.096 pedidos (ex.: desconto no PIX), "
                    "portanto não representa o valor do pedido.",
                    "<b>Join 1:1</b>: 60.096 pedidos e 60.096 pagamentos, ambos com id_pedido único; "
                    "inner join por id_pedido, sem duplicidade.",
                    "<b>Filtros</b>: ano de data_criacao = 2025; status = false e "
                    "avaliacao_fraude.fraude = false. Resultado real: 1.473 pedidos.",
                    "<b>Ordenação</b>: uf, forma_pagamento e data do pedido; gravação em parquet "
                    "(1 partição para preservar a ordem).",
                ],
            ),
            (
                "Logging e tratamento de erros",
                [
                    "RelatorioPedidos importa logging, configura logging.basicConfig(level=INFO) e "
                    "registra a contagem de cada etapa. Falhas caem em try/except, são registradas "
                    "com logging.error e relançadas como RelatorioPedidosError.",
                ],
            ),
            (
                "Como executar (detalhes no README.md do repositório)",
                [
                    "pip install -r requirements.txt; pip install .; "
                    "python scripts/download_datasets.py; python main.py; pytest",
                ],
            ),
        ]
        fluxo = []
        for titulo, paragrafos in itens:
            fluxo.append(Paragraph(titulo, self._h2))
            fluxo += [Paragraph(p, self._txt) for p in paragrafos]
        fluxo.append(Spacer(1, 0.3 * cm))
        fluxo.append(Paragraph(f"Repositório: {REPO_URL}", self._txt))
        fluxo.append(PageBreak())
        return fluxo

    def _evidencias_flow(self) -> list:
        fluxo, titulo_atual = [], None
        for ev in self._evidencias:
            bloco = []
            if ev.titulo != titulo_atual:
                bloco.append(Paragraph(ev.titulo, self._h2))
                titulo_atual = ev.titulo
            largura_px = Image(str(ev.imagem)).imageWidth
            altura_px = Image(str(ev.imagem)).imageHeight
            escala = min(0.62, LARGURA_UTIL_PT / largura_px)
            bloco.append(Paragraph(ev.legenda, self._leg))
            bloco.append(
                Image(str(ev.imagem), width=largura_px * escala, height=altura_px * escala, hAlign="LEFT")
            )
            bloco.append(Spacer(1, 0.25 * cm))
            fluxo.append(KeepTogether(bloco))
        return fluxo


def main() -> None:
    executor, maker = Executor(), ImageMaker(IMAGENS)
    for antigo in IMAGENS.glob("*.png"):
        antigo.unlink()
    evidencias: list[Evidencia] = []

    for caminho, descricao in ARQUIVOS_CODIGO:
        evidencias += maker.codigo(caminho, descricao)

    print("Executando o pipeline ...")
    log = executor.run([sys.executable, "main.py"])
    evidencias += maker.terminal("Execução do pipeline (logs)", "python main.py", log)

    print("Validando a saída ...")
    validacao = executor.run([sys.executable, "scripts/validar_saida.py"])
    checagens, amostra = validacao.split("root", 1)
    evidencias += maker.terminal("Validações do parquet", "python scripts/validar_saida.py", checagens)
    evidencias += maker.terminal(
        "Amostra do parquet (schema e primeiras linhas)",
        "python scripts/validar_saida.py",
        "root" + amostra,
    )

    print("Executando os testes ...")
    testes = executor.run([sys.executable, "-m", "pytest", "-v", "--no-header", "-p", "no:cacheprovider"])
    evidencias += maker.terminal("Execução dos testes (pytest)", "pytest -v", testes)

    PdfBuilder(evidencias).build()
    print(f"{len(evidencias)} imagens em {IMAGENS}; PDF em {PDF}")


if __name__ == "__main__":
    main()
