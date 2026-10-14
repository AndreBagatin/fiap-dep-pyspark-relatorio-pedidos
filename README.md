# Relatório de pedidos com pagamento recusado e fraude = false (PySpark)

Trabalho final da disciplina **Data Engineering Programming** (FIAP MBA – Prof. Marcelo Barbosa Pinto).

Repositório: https://github.com/AndreBagatin/fiap-dep-pyspark-relatorio-pedidos

## Integrantes

| Nome | RM |
| --- | --- |
| Priscila Cristina Mendes Cardoso | 373153 |
| Andre Victor Bagatin | 374773 |
| Felipe Nogueira da Silva | 373111 |
| Heloisa Cordeiro Silva | 373370 |
| Leonardo Machado de Andrade | 373150 |
| Nicolas Galhardo Lhen | 374205 |

## 1. O que o projeto faz

Gera um relatório (parquet) com os pedidos de venda **de 2025** cujo pagamento foi **recusado (`status=false`)** e que, na avaliação de fraude, foram classificados como **legítimos (`fraude=false`)**.

| Coluna | Origem |
| --- | --- |
| `id_pedido` | pedidos.`id_pedido` |
| `uf` | pedidos.`uf` |
| `forma_pagamento` | pagamentos.`forma_pagamento` |
| `valor_total` | pedidos.`valor_unitario` × pedidos.`quantidade` |
| `data_pedido` | pedidos.`data_criacao` (timestamp) |

Ordenação: `uf`, `forma_pagamento`, `data_pedido` (e `id_pedido` apenas como desempate determinístico).

### Decisões de negócio

* **Valor total do pedido = `valor_unitario × quantidade`** (dataset de pedidos). Conferimos nos dados reais que o `valor_pagamento` do dataset de pagamentos *não* é um substituto: em 21.297 dos 60.096 pedidos ele difere do valor do pedido (ex.: pedido de R$ 3.000 pago via PIX com valor de pagamento R$ 2.850, ou seja, desconto). O "valor total do pedido" pedido pela gestão é o valor do pedido, e não o valor cobrado no pagamento.
* **Join pedido × pagamento é 1:1.** Verificado nos dados: 60.096 pedidos com `id_pedido` únicos, 60.096 pagamentos com `id_pedido` únicos, 60.096 linhas no join. Usamos `inner join` por `id_pedido`; portanto não há duplicidade a tratar.
* **"Pedidos de 2025"** = `year(data_criacao) == 2025` (data de criação do pedido). O ano é configurável.
* **Recusado e legítimo** = `status == false` **e** `avaliacao_fraude.fraude == false`. Registros com `status` ou `fraude` nulos não entram (não há como afirmar que foram recusados/legítimos).
* Resultado com os dados reais: **1.473 pedidos**.
* Tipos numéricos como `double` (e não `float`) para não introduzir erro de arredondamento no valor total.
* O parquet é gravado em **1 partição** (`coalesce(1)` depois da ordenação), de modo que a ordem global do relatório fica preservada no arquivo.

## 2. Pré-requisitos

| Item | Versão |
| --- | --- |
| Python | 3.10 ou superior (testado com 3.12) |
| Java (JDK) | 17 ou superior – exigido pelo PySpark 4 (testado com Temurin 17). Verifique com `java -version` |
| PySpark | 4.x (instalado pelo pip; fixado em `requirements.txt`) |

Linux, macOS, WSL e GitHub Codespaces não exigem nada além disso.

**Somente Windows (nativo):** o Hadoop usado pelo Spark precisa de `winutils.exe` e `hadoop.dll` para gravar em disco local. Coloque-os em uma pasta `...\hadoop\bin`, defina `HADOOP_HOME` apontando para `...\hadoop` e adicione `%HADOOP_HOME%\bin` ao `PATH` (ex.: https://github.com/cdarlint/winutils). Exemplo em PowerShell:

```powershell
$env:HADOOP_HOME = "C:\hadoop"
$env:Path = "$env:HADOOP_HOME\bin;$env:Path"
```

## 3. Instalação

```bash
git clone https://github.com/AndreBagatin/fiap-dep-pyspark-relatorio-pedidos.git
cd fiap-dep-pyspark-relatorio-pedidos

python -m venv .venv
# Linux/macOS:
source .venv/bin/activate
# Windows (PowerShell):
#   .\.venv\Scripts\Activate.ps1

pip install -r requirements.txt
pip install .
```

* `pip install -r requirements.txt` instala as dependências fixadas (inclui as ferramentas de teste/lint e as usadas para gerar o PDF).
* `pip install .` instala o pacote `relatorio_pedidos` (usa `pyproject.toml` e `MANIFEST.in`). Para desenvolver, use `pip install -e .`.

## 4. Obter os datasets

Os dados **não** são versionados. O script abaixo baixa os dois repositórios do professor e extrai apenas as pastas de dados para `data/input/` (não precisa de `git`):

```bash
python scripts/download_datasets.py
```

Resultado esperado:

```
data/input/datasets-csv-pedidos/data/pedidos/pedidos-2024-01.csv.gz ... (25 arquivos)
data/input/dataset-json-pagamentos/data/pagamentos/pagamentos-2024-01.json.gz ... (25 arquivos)
```

Alternativa manual: `git clone` de https://github.com/infobarbosa/datasets-csv-pedidos e https://github.com/infobarbosa/dataset-json-pagamentos dentro de `data/input/`.

## 5. Executar o pipeline

A partir da raiz do projeto (com o venv ativo):

```bash
python main.py
```

Os logs mostram a contagem de registros em cada etapa. A saída é gravada em **`data/output/relatorio_pedidos/`** (arquivo `part-*.parquet`).

Para conferir o resultado contra os dados de origem (colunas, 2025, status/fraude, valor total, ordenação, duplicidade):

```bash
python scripts/validar_saida.py
```

### Configuração

Todos os parâmetros ficam em [`config/settings.yaml`](config/settings.yaml) (lido pela classe `Settings`). Para usar outro arquivo: `python main.py caminho/outro.yaml` ou a variável `RELATORIO_CONFIG`. Variáveis de ambiente sobrescrevem o YAML:

| Variável | Efeito |
| --- | --- |
| `PEDIDOS_PATH` | pasta/arquivo(s) CSV de pedidos |
| `PAGAMENTOS_PATH` | pasta/arquivo(s) JSON de pagamentos |
| `OUTPUT_PATH` | pasta de saída do parquet |
| `ANO_RELATORIO` | ano do filtro (padrão 2025) |

Exemplo: `ANO_RELATORIO=2024 OUTPUT_PATH=/tmp/rel2024 python main.py`.

## 6. Testes

```bash
pytest
```

São 19 testes (`tests/`): filtro de status/fraude (inclusive nulos), filtro de 2025 (bordas do ano), cálculo do valor total, join, colunas finais, ordenação, fluxo de ponta a ponta, caminho de erro (log + `RelatorioPedidosError`), contagens em log, orquestração com *mocks* injetados, configuração (YAML/ambiente), schemas explícitos, leitura/escrita parquet e execução completa da `Application` com dados em disco temporário. Usam uma `SparkSession` local criada na fixture de `tests/conftest.py`.

Lint/formatação: `ruff check .` e `ruff format --check .`.

## 7. Estrutura do projeto

```
.
├── main.py                              # Aggregation Root (classe Application)
├── pyproject.toml  requirements.txt  MANIFEST.in  README.md  .gitignore
├── config/settings.yaml                 # caminhos, opções de leitura, ano, Spark
├── src/relatorio_pedidos/
│   ├── config/settings.py               # classe Settings (padrões + YAML + ambiente)
│   ├── session/spark_session_manager.py # classe SparkSessionManager (criar/obter/encerrar)
│   ├── io_utils/
│   │   ├── schemas.py                   # PedidosSchema, PagamentosSchema (StructType explícitos)
│   │   ├── readers.py                   # DataReader (CSV e JSON com schema)
│   │   └── writers.py                   # ParquetWriter
│   ├── business/relatorio_pedidos.py    # RelatorioPedidos (regras + logging + try/except)
│   └── pipeline/pipeline.py             # Pipeline (orquestra leitura -> negócio -> escrita)
├── scripts/
│   ├── download_datasets.py             # baixa os datasets para data/input/
│   ├── validar_saida.py                 # valida o parquet gerado
│   └── gerar_evidencias.py              # gera as imagens e o PDF de entrega
├── tests/                               # pytest
├── entrega/Trabalho_Final_DEP.pdf       # documento de entrega (capa, evidências, link)
└── data/ (input/ e output/)             # gerados localmente, fora do git
```

## 8. Arquitetura e injeção de dependências

```
main.py  (Application = Aggregation Root)
  │  instancia:
  ├── Settings.load()                      -> configuração
  ├── SparkSessionManager(settings)        -> sessão Spark
  ├── DataReader(spark, settings)          -> leitura com schema explícito
  ├── RelatorioPedidos(ano)                -> regras de negócio
  ├── ParquetWriter(settings)              -> escrita
  └── Pipeline(reader, relatorio, writer)  -> orquestração (recebe tudo pronto)
```

* Somente `main.py` instancia dependências; nenhuma classe cria internamente as suas (o `Pipeline` recebe `reader`, `relatorio` e `writer` no construtor, o que permite substituí-los por *mocks* nos testes).
* **Schemas explícitos:** nenhum `inferSchema`; todos os DataFrames, inclusive os dos testes, são criados com `StructType` (incluindo o struct aninhado `avaliacao_fraude`).
* **Logging:** `RelatorioPedidos` importa `logging`, chama `logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')` e registra cada etapa com a contagem de registros.
* **Tratamento de erros:** cada etapa da lógica de negócio passa por `RelatorioPedidos._executar`, que usa `try/except`, registra com `logging.error` e relança `RelatorioPedidosError` (causa original encadeada). O `main.py` converte falhas em código de saída 1.

## 9. Documento de entrega

`entrega/Trabalho_Final_DEP.pdf` reúne capa, link do repositório, descrição da arquitetura/decisões e as evidências (imagens de código, execução, testes e parquet, cada uma com no máximo 20 linhas). Para regerar: `python scripts/gerar_evidencias.py` (requer a saída do pipeline e `pytest` já executados – o script os executa e captura).
