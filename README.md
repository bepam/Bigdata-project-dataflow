# 🛒 Pipeline ShopBrasil — DataFlow Analytics

**Projeto Final · Big Data Processing · MBA em Engenharia de Dados — Universidade Presbiteriana Mackenzie**
Professor: Alexandre Tavares · **Opção A — Pipeline de E-commerce**

Pipeline de produção que consolida as vendas de **três parceiros em três formatos** (CSV legado, JSON de API, Parquet), aplica **regras de qualidade com quarentena** e publica **métricas de negócio** para o dashboard executivo da ShopBrasil — tudo orquestrado pelo **Airflow**, processado com **PySpark** em arquitetura **Medallion** e empacotado em **Docker Compose**.

```
docker compose up -d --build   →   sensor → Bronze → Silver → quality gate → Gold → quality gate → notificação
```

## 👥 Integrantes

| Nome | RA | Responsabilidade principal |
|---|---|---|
| _Nome completo_ | _RA_ | Ingestão (Bronze) |
| _Nome completo_ | _RA_ | Transformação (Silver) |
| _Nome completo_ | _RA_ | Qualidade de dados |
| _Nome completo_ | _RA_ | Agregações (Gold) |
| _Nome completo_ | _RA_ | Orquestração e Docker |

---

## 🚀 Como rodar

Há duas formas. As duas usam o mesmo `docker-compose.yml`, e os dados de entrada já estão no repositório (`data/raw/`), então não é preciso baixar nada.

### Opção 1 — GitHub Codespaces (recomendada: nada para instalar)

1. No GitHub do projeto: **Code → Codespaces → ⋯ → New with options**.
2. Em *Machine type*, escolha **4-core** (16 GB RAM) → **Create codespace**.
3. Aguarde. O Codespace já roda `docker compose up -d --build` sozinho. A primeira vez leva de 5 a 8 minutos; acompanhe no terminal com `docker compose ps`.
4. Aba **PORTS** → porta **8080** → 🌐 abre o Airflow. A 4040 é a Spark UI (só aparece enquanto um job roda).

> **Custo:** contas pessoais têm 120 core-horas grátis por mês, ou seja, **~30 h** numa máquina de 4 cores. **Pare o Codespace** quando não estiver usando (*Codespaces → Stop*); ele hiberna sozinho após 30 min parado e, ao reabrir, o ambiente sobe de novo automaticamente.

### Opção 2 — Docker na própria máquina

**Pré-requisitos:** Docker Desktop (ou Docker Engine) com Compose 2.x, 8 GB de RAM livres para o Docker, 4 CPUs, ~5 GB de disco. Portas **8080** e **4040** livres.

```bash
git clone <url-do-repositorio> projeto-final
cd projeto-final

# Somente Linux: arquivos gerados ficam com o seu usuário
echo "AIRFLOW_UID=$(id -u)" > .env

docker compose up -d --build
```

A primeira subida constrói a imagem (Airflow + Java + PySpark), leva de 3 a 6 minutos e precisa de internet. As próximas sobem em segundos.

### Depois que o ambiente subiu (vale para as duas opções)

1. Abra o Airflow (**http://localhost:8080** no Docker local, ou a porta 8080 da aba PORTS no Codespaces) → usuário `admin`, senha `admin`.
2. A DAG **`shopbrasil_pipeline_vendas`** já aparece **ativa** e dispara sozinha na primeira subida. Para rodar de novo: botão ▶ **Trigger DAG**.
3. Durante os jobs Spark, a Spark UI fica em **http://localhost:4040**.
4. Ao final (~2 minutos), veja os resultados no terminal:

```bash
docker compose exec airflow-scheduler python scripts/mostrar_resultados.py
# seções: --secao fluxo | gates | quarentena | gold
```

**Parar / limpar**

```bash
docker compose down              # para os containers (mantém dados e histórico)
docker compose down -v           # também apaga o banco do Airflow
rm -rf data/bronze data/silver data/gold data/quarentena data/quality data/notificacoes logs
```

---

## 🏗️ Arquitetura

```mermaid
flowchart LR
    RAW["📂 data/raw<br/>CSV · JSON · Parquet"] --> B["🥉 Bronze<br/>dado bruto + metadados"]
    B --> S["🥈 Silver<br/>schema unificado · limpo · deduplicado"]
    B -. reprovados .-> Q["🚧 Quarentena<br/>+ motivos"]
    S --> QG{{"Quality gate"}} --> G["🥇 Gold<br/>estado · mês · pagamento · categoria×segmento"]
    G --> QG2{{"Quality gate"}} --> N["📣 Notificação"]
```

Detalhes completos (diagramas da DAG, contrato de cada camada, regras de qualidade e decisões): **[docs/arquitetura.md](docs/arquitetura.md)**.

### Fontes de dados

Somente arquivos do [repositório oficial da disciplina](https://github.com/AleTavares/Mackenzie_BigDataProcessing/tree/main/datasets), copiados **sem nenhuma alteração** (mesmo checksum SHA-256) para `data/raw/`.

**Vendas — `datasets/aula_03`:**

| Fonte | Arquivo | Formato | Registros |
|---|---|---|---:|
| `parceiro_a` | `vendas_legacy_01_2023.csv` | CSV ISO-8859-1, separador `;`, colunas em português | 495 |
| `parceiro_a` | `vendas_legacy_06_2023.csv` | idem | 5.292 |
| `parceiro_a` | `vendas_legacy_12_2023.csv` | idem | 1.084 |
| `parceiro_b` | `api_dump_page_001.json` … `003.json` | JSON de API paginado (`data[]` + metadados) | 30.000 |
| `parceiro_c` | `vendas_parceiro_c.parquet` | Parquet | 80.000 |
| **Total de vendas** | 7 arquivos | | **116.871** |

**Dimensões — `datasets/aula_02`** (exigidas pela Opção A):

| Fonte | Arquivo | Formato | Registros |
|---|---|---|---:|
| `clientes` | `clientes.parquet` | Parquet | 500.000 clientes |
| `categorias` | `categorias.json` | JSON aninhado (categorias → subcategorias) | 10 categorias |

Todos os `customer_id` das vendas existem no cadastro de clientes. Como o JSON de categorias não traz `product_id`, o produto é ligado à categoria pela regra do lab da Aula 2: faixas de 500 produtos (`PROD_0001–0500` → `CAT_01`, …).

> Os dados são **sintéticos**, gerados pelo professor com `datasets/gerar_datasets.py` (Faker pt_BR, seeds fixas). Todos os números deste projeto saem do processamento desses arquivos pelo pipeline.

### Camadas

| Camada | Job | Saída |
|---|---|---|
| Bronze | `spark_jobs/ingestao.py` | `data/bronze/vendas/parceiro_a`, `…/parceiro_b`, `…/parceiro_c`, `data/bronze/clientes`, `data/bronze/categorias` |
| Silver | `spark_jobs/transformacao.py` | `data/silver/vendas` (partição `ano_mes`), `data/silver/clientes`, `data/silver/categorias`, `data/quarentena/vendas` |
| Gold | `spark_jobs/agregacao.py` | `data/gold/faturamento_por_estado`, `data/gold/analise_mensal`, `data/gold/faturamento_por_pagamento`, `data/gold/faturamento_categoria_segmento` |
| Qualidade | `quality/checks.py` | `data/quality/ultima_execucao/*.json`, `data/quality/historico/` |

### Qualidade de dados

- **5 dimensões por registro:** completude (11 campos obrigatórios), validade de domínio (status, forma de pagamento, UF, valores > 0, data em 2023), consistência (`total = qtd × preço`), integridade referencial (cliente existe no cadastro) e unicidade (`order_id`).
- **Quarentena funcional:** registro reprovado vai para `data/quarentena/vendas` com a lista de motivos (`_motivos`) e o motivo principal. O funcionamento está coberto pelos testes em `tests/test_checks.py`.
- **Conservação garantida:** `Bronze = Silver + Quarentena` (verificado a cada execução).
- **Quality gates bloqueantes:** se a Silver não passar, a Gold não é publicada; a Gold é reconciliada contra a Silver antes da notificação.

**Resultado com os dados da Aula 3:** os 116.871 registros passaram em todas as regras, então a quarentena fica **vazia (0 registros)** e a DAG segue pelo ramo `sem_pendencias`. Isso é um resultado real: os arquivos da Aula 3 não têm nulos, duplicatas, valores fora do domínio, totais inconsistentes nem clientes fora do cadastro. O único problema encontrado é de linhagem: a coluna `partner_source` dentro dos arquivos não bate com o parceiro que enviou o arquivo (ver `docs/arquitetura.md`).

### Orquestração

`aguardar_arquivos_parceiros` (sensor) → `bronze_ingestao` → `silver_transformacao` → `quality_gate_silver` → `gold_agregacao` → `quality_gate_gold` → `verificar_quarentena` (branch) → `alertar_quarentena` | `sem_pendencias` → `notificar_sucesso`

Retries com backoff, callback de falha, `max_active_runs=1` e parâmetros `taxa_quarentena_max` e `volume_minimo_silver` ajustáveis pela UI. Alertas e resumos são gravados em `data/notificacoes/` (simulando Slack/e-mail).

---

## 📁 Estrutura do repositório

```
projeto-final/
├── README.md
├── .devcontainer/
│   └── devcontainer.json       # GitHub Codespaces (sobe tudo sozinho)
├── docker-compose.yml          # postgres + airflow-init + webserver + scheduler
├── Dockerfile                  # Airflow 2.8.4 + Java 17 + PySpark 3.5.1
├── requirements.txt
├── .env                        # AIRFLOW_UID
├── dags/
│   └── pipeline.py             # DAG shopbrasil_pipeline_vendas
├── spark_jobs/
│   ├── common.py               # caminhos, SparkSession, logging JSON, métricas
│   ├── ingestao.py             # Bronze
│   ├── transformacao.py        # Silver + quarentena
│   └── agregacao.py            # Gold
├── quality/
│   └── checks.py               # regras de linha, quarentena e quality gates
├── scripts/
│   └── mostrar_resultados.py   # resumo no terminal para a demo
├── tests/
│   └── test_checks.py          # testes unitários das regras de qualidade
├── conf/
│   └── log4j2.properties       # log do Spark enxuto
├── data/
│   └── raw/                    # vendas (aula_03) + clientes e categorias (aula_02)
└── docs/
    ├── arquitetura.md          # diagramas e decisões
    ├── apresentacao.md         # roteiro da apresentação + plano B
    └── slides/                 # apresentacao_projeto_final.pptx (+ gerar_slides.js)
```

## 🧪 Rodar sem Docker (desenvolvimento)

Requer Python 3.10+ e Java 17.

```bash
python -m venv .venv && source .venv/bin/activate
pip install pyspark==3.5.1 pandas pyarrow pytest
export DATA_DIR=$(pwd)/data PYTHONPATH=$(pwd) SPARK_CONF_DIR=$(pwd)/conf

pytest -q tests/                                     # testes das regras
spark-submit spark_jobs/ingestao.py
spark-submit spark_jobs/transformacao.py
spark-submit quality/checks.py --camada silver
spark-submit spark_jobs/agregacao.py
spark-submit quality/checks.py --camada gold
python scripts/mostrar_resultados.py
```

## 🛠️ Stack

| Tecnologia | Versão |
|---|---|
| Python | 3.11 |
| Apache Spark (PySpark) | 3.5.1 |
| Apache Airflow | 2.8.4 (+ provider apache-spark 4.7.1) |
| PostgreSQL (metadados Airflow) | 13 |
| Docker Compose | 2.x |
| Formato de saída | Parquet (Snappy) |

## ❓ Problemas comuns

| Sintoma | Solução |
|---|---|
| Codespaces: página do Airflow não abre | Aguarde `docker compose ps` mostrar o webserver *healthy*; na aba PORTS, clique no 🌐 da 8080 de novo |
| Codespaces: ambiente não subiu depois de reabrir | No terminal: `AIRFLOW_UID=$(id -u) docker compose up -d` |
| Porta 8080 ocupada | Pare o outro serviço ou troque `"8080:8080"` por `"8081:8080"` no compose |
| `Permission denied` em `data/` (Linux) | `echo "AIRFLOW_UID=$(id -u)" > .env` e `docker compose up -d --force-recreate` |
| DAG não aparece | `docker compose logs airflow-scheduler`; aguarde ~30 s após o webserver ficar *healthy* |
| Task Spark com `Java gateway process exited` | Docker com pouca memória: reserve ao menos 6 GB em *Settings → Resources* |
| Quero ver o gate reprovando | *Trigger DAG w/ config* → `volume_minimo_silver = 200000` |
