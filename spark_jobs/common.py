"""
Utilitários compartilhados pelos jobs Spark do pipeline ShopBrasil.

Centraliza:
  - caminhos das camadas do data lake (raw → bronze → silver → gold)
  - criação da SparkSession (funciona via spark-submit ou `python job.py`)
  - parsing de argumentos padrão (--data-ref, --run-id)
  - logging estruturado e gravação de métricas em JSON
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Caminhos do data lake
# ---------------------------------------------------------------------------
# Dentro do container, DATA_DIR = /opt/airflow/data (bind mount de ./data).
# Para rodar fora do Docker: export DATA_DIR=$(pwd)/data
DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/airflow/data"))

RAW = DATA_DIR / "raw"
BRONZE = DATA_DIR / "bronze"
SILVER = DATA_DIR / "silver"
GOLD = DATA_DIR / "gold"
QUARENTENA = DATA_DIR / "quarentena"
QUALITY = DATA_DIR / "quality"
METRICAS = QUALITY / "ultima_execucao"   # JSONs da última execução (lidos pela DAG)
HISTORICO = QUALITY / "historico"        # Parquet append-only (monitoramento)

# Fontes de vendas (parceiros) — nome lógico → pasta em raw/
PARCEIROS = ["parceiro_a", "parceiro_b", "parceiro_c"]


def caminho(p: Path) -> str:
    """Spark trabalha com strings; mantemos Path no restante do código."""
    return str(p)


# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------
def get_spark(app_name: str) -> SparkSession:
    """
    Cria a SparkSession.

    - Via spark-submit (Airflow): o master vem do próprio spark-submit
      (conexão `spark_local` → local[2]); não sobrescrevemos.
    - Via `python job.py` (desenvolvimento): usa SPARK_MASTER ou local[*].
    """
    builder = SparkSession.builder.appName(app_name)
    # Sob spark-submit, a JVM já existe e injeta PYSPARK_GATEWAY_PORT no
    # processo Python; nesse caso o master já foi definido pelo spark-submit.
    if "PYSPARK_GATEWAY_PORT" not in os.environ:
        builder = builder.master(os.environ.get("SPARK_MASTER", "local[*]"))

    spark = (
        builder
        # Volume pequeno (~170K linhas): 200 partições de shuffle seria desperdício
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Sobrescrever apenas as partições tocadas quando escrevemos particionado
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Argumentos
# ---------------------------------------------------------------------------
def parse_args(descricao: str, extra=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=descricao)
    parser.add_argument(
        "--data-ref",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Data lógica da execução (Airflow: {{ ds }})",
    )
    parser.add_argument(
        "--run-id",
        default=f"manual_{datetime.now().strftime('%Y%m%dT%H%M%S')}",
        help="Identificador da execução (Airflow: {{ run_id }})",
    )
    if extra:
        extra(parser)
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Logging estruturado
# ---------------------------------------------------------------------------
class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "nivel": record.levelname,
            "job": record.name,
            "msg": record.getMessage(),
        }
        extra = getattr(record, "dados", None)
        if extra:
            payload.update(extra)
        return json.dumps(payload, ensure_ascii=False, default=str)


def get_logger(nome: str) -> logging.Logger:
    logger = logging.getLogger(nome)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def log(logger: logging.Logger, msg: str, **dados) -> None:
    logger.info(msg, extra={"dados": dados})


# ---------------------------------------------------------------------------
# Métricas
# ---------------------------------------------------------------------------
def salvar_metricas(nome: str, metricas: dict) -> Path:
    """Grava métricas da etapa em quality/ultima_execucao/<nome>.json."""
    METRICAS.mkdir(parents=True, exist_ok=True)
    destino = METRICAS / f"{nome}.json"
    metricas = {"etapa": nome, "gerado_em": datetime.now().isoformat(timespec="seconds"), **metricas}
    destino.write_text(json.dumps(metricas, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    return destino


def ler_metricas(nome: str) -> dict:
    arquivo = METRICAS / f"{nome}.json"
    if not arquivo.exists():
        return {}
    return json.loads(arquivo.read_text(encoding="utf-8"))
