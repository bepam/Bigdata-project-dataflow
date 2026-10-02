"""
Camada BRONZE — Ingestão multi-formato (dados brutos + metadados de ingestão)
==============================================================================

Lê as fontes da ShopBrasil exatamente como chegam e grava em Parquet, sem
nenhuma regra de negócio. A única coisa adicionada são metadados de
rastreabilidade:

    _source         fonte de origem (definida pelo pipeline)
    _source_file    arquivo físico de onde a linha veio
    _ingestion_ts   momento da ingestão
    _batch_id       run_id do Airflow (liga a linha à execução que a gerou)
    _data_ref       data lógica da execução ({{ ds }})

Vendas:
    vendas_csv       CSV     (ISO-8859-1, separador ';', colunas em português)
    vendas_json      JSON    (multiLine, registros dentro de data[], paginado)
    vendas_parquet   Parquet

Cadastros:
    clientes     Parquet     (cadastro de clientes)
    categorias   JSON        (hierarquia de categorias de produto)

Execução:
    spark-submit spark_jobs/ingestao.py --data-ref 2024-01-01 --run-id manual
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_jobs.common import (
    BRONZE, RAW, caminho, get_logger, get_spark, log, parse_args, salvar_metricas,
)

logger = get_logger("bronze.ingestao")

# Contrato mínimo de colunas por fonte: se a fonte mudar o layout, a
# ingestão falha cedo (schema drift) em vez de propagar lixo para a Silver.
CONTRATO_COLUNAS = {
    "vendas_csv": {"cod_pedido", "cod_cliente", "cod_produto", "qtd", "preco_unit",
                   "valor_total", "data_pedido", "forma_pagamento", "uf_entrega", "situacao"},
    "vendas_json": {"order_id", "customer_id", "product_id", "quantity", "unit_price",
                   "total_amount", "order_date", "payment_method", "shipping_state", "status"},
    "vendas_parquet": {"order_id", "customer_id", "product_id", "quantity", "unit_price",
                   "total_amount", "order_date", "payment_method", "shipping_state", "status"},
    "clientes": {"customer_id", "state", "segment"},
    "categorias": {"categorias"},
}


class SchemaDriftError(Exception):
    """Fonte chegou sem colunas obrigatórias do contrato."""


# ---------------------------------------------------------------------------
# Leitores por formato
# ---------------------------------------------------------------------------
def ler_vendas_csv(spark: SparkSession) -> DataFrame:
    # Sem inferSchema: Bronze guarda o dado como veio (tudo texto no CSV).
    return spark.read.csv(
        caminho(RAW / "vendas_csv" / "*.csv"),
        header=True, sep=";", encoding="ISO-8859-1",
    )


def ler_vendas_json(spark: SparkSession) -> DataFrame:
    bruto = spark.read.json(caminho(RAW / "vendas_json" / "*.json"), multiLine=True)
    # Cada arquivo traz as vendas dentro de uma lista chamada "data"
    return bruto.select(F.explode("data").alias("registro")).select("registro.*")


def ler_vendas_parquet(spark: SparkSession) -> DataFrame:
    return spark.read.parquet(caminho(RAW / "vendas_parquet"))


def ler_clientes(spark: SparkSession) -> DataFrame:
    return spark.read.parquet(caminho(RAW / "clientes"))


def ler_categorias(spark: SparkSession) -> DataFrame:
    # Mantém a estrutura aninhada original (array de categorias → subcategorias)
    return spark.read.json(caminho(RAW / "categorias" / "*.json"), multiLine=True)


FONTES = {
    # nome       (leitor,          destino bronze)
    "vendas_csv": (ler_vendas_csv, BRONZE / "vendas" / "vendas_csv"),
    "vendas_json": (ler_vendas_json, BRONZE / "vendas" / "vendas_json"),
    "vendas_parquet": (ler_vendas_parquet, BRONZE / "vendas" / "vendas_parquet"),
    "clientes": (ler_clientes, BRONZE / "clientes"),
    "categorias": (ler_categorias, BRONZE / "categorias"),
}


# ---------------------------------------------------------------------------
def adicionar_metadados(df: DataFrame, fonte: str, run_id: str, data_ref: str) -> DataFrame:
    return (
        df.withColumn("_source", F.lit(fonte))
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_batch_id", F.lit(run_id))
        .withColumn("_data_ref", F.lit(data_ref).cast("date"))
    )


def validar_contrato(df: DataFrame, fonte: str) -> None:
    faltando = CONTRATO_COLUNAS[fonte] - set(df.columns)
    if faltando:
        raise SchemaDriftError(f"{fonte}: colunas obrigatórias ausentes {sorted(faltando)}")


def main() -> None:
    args = parse_args("Bronze: ingestão multi-formato")
    spark = get_spark("shopbrasil_bronze_ingestao")
    log(logger, "Início da ingestão Bronze", data_ref=args.data_ref, run_id=args.run_id)

    metricas = {"data_ref": args.data_ref, "run_id": args.run_id, "fontes": {}}
    for fonte, (leitor, destino) in FONTES.items():
        df = leitor(spark)
        validar_contrato(df, fonte)
        df = adicionar_metadados(df, fonte, args.run_id, args.data_ref)

        # Overwrite = idempotente: reexecutar o mesmo lote não duplica dados
        df.write.mode("overwrite").parquet(caminho(destino))

        registros = spark.read.parquet(caminho(destino)).count()
        arquivos = df.select("_source_file").distinct().count()
        metricas["fontes"][fonte] = {"registros": registros, "arquivos": arquivos,
                                     "destino": caminho(destino)}
        log(logger, "Fonte ingerida", fonte=fonte, registros=registros, arquivos=arquivos)

    metricas["total_vendas_bronze"] = sum(
        v["registros"] for k, v in metricas["fontes"].items() if k.startswith("vendas_")
    )
    salvar_metricas("bronze", metricas)
    log(logger, "Bronze concluída", total_vendas=metricas["total_vendas_bronze"])
    spark.stop()


if __name__ == "__main__":
    main()
