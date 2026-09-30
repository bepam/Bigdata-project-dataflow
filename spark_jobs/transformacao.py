"""
Camada SILVER — Normalização, limpeza, validação e quarentena
==============================================================

Entrada : bronze/vendas/<parceiro>   (parceiro_a, parceiro_b, parceiro_c)
          bronze/clientes, bronze/categorias
Saída   : silver/vendas              (particionado por ano_mes)
          silver/clientes            (dimensão, minimizada — LGPD)
          silver/categorias          (dimensão achatada)
          quarentena/vendas          (registros reprovados + motivos)

Passos para vendas:
  1. Mapear cada parceiro para o schema unificado (A tem colunas em português)
  2. Tipar (int/double/timestamp/date) e padronizar texto (trim, caixa, vazio→NULL)
  3. Aplicar regras de qualidade (quality/checks.py) → válidos × quarentena
  4. Deduplicar por order_id (duplicatas excedentes vão para a quarentena)
  5. Derivar colunas de negócio (ano_mes, category_id)

Invariante garantido e verificado pelo quality gate:
    registros Bronze == registros Silver + registros Quarentena
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from quality.checks import aplicar_quarentena, resumir_quarentena
from spark_jobs.common import (
    BRONZE, PARCEIROS, QUARENTENA, SILVER, caminho, get_logger, get_spark, log,
    parse_args, salvar_metricas,
)

logger = get_logger("silver.transformacao")

# Schema unificado de vendas (ordem das colunas na Silver)
COLUNAS_VENDAS = [
    "order_id", "customer_id", "product_id", "quantity", "unit_price", "total_amount",
    "order_date", "payment_method", "shipping_city", "shipping_state", "status", "partner_source",
]
METADADOS = ["_source", "_source_file", "_ingestion_ts", "_batch_id", "_data_ref"]

# Parceiro A (ERP legado) usa nomes em português
MAPEAMENTO_PARCEIRO_A = {
    "cod_pedido": "order_id", "cod_cliente": "customer_id", "cod_produto": "product_id",
    "qtd": "quantity", "preco_unit": "unit_price", "valor_total": "total_amount",
    "data_pedido": "order_date", "forma_pagamento": "payment_method",
    "cidade_entrega": "shipping_city", "uf_entrega": "shipping_state",
    "situacao": "status", "origem": "partner_source",
}


# ---------------------------------------------------------------------------
def _texto(c: str) -> F.Column:
    """trim + string vazia vira NULL (conta como falha de completude)."""
    return F.nullif(F.trim(F.col(c).cast("string")), F.lit(""))


def padronizar_vendas(df: DataFrame, parceiro: str) -> DataFrame:
    if parceiro == "parceiro_a":
        for antigo, novo in MAPEAMENTO_PARCEIRO_A.items():
            df = df.withColumnRenamed(antigo, novo)

    for c in COLUNAS_VENDAS:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("string"))

    return df.select(
        F.upper(_texto("order_id")).alias("order_id"),
        F.upper(_texto("customer_id")).alias("customer_id"),
        F.upper(_texto("product_id")).alias("product_id"),
        # try_cast: texto não numérico vira NULL em vez de derrubar o job
        F.expr("try_cast(quantity AS INT)").alias("quantity"),
        F.expr("try_cast(unit_price AS DOUBLE)").alias("unit_price"),
        F.expr("try_cast(total_amount AS DOUBLE)").alias("total_amount"),
        F.to_timestamp(F.col("order_date").cast("string")).alias("order_ts"),
        F.lower(_texto("payment_method")).alias("payment_method"),
        _texto("shipping_city").alias("shipping_city"),
        F.upper(_texto("shipping_state")).alias("shipping_state"),
        F.lower(_texto("status")).alias("status"),
        F.lower(_texto("partner_source")).alias("partner_source"),
        *METADADOS,
    ).withColumn("order_date", F.to_date("order_ts"))


def carregar_vendas_bronze(spark: SparkSession) -> DataFrame:
    vendas = None
    for parceiro in PARCEIROS:
        df = padronizar_vendas(spark.read.parquet(caminho(BRONZE / "vendas" / parceiro)), parceiro)
        vendas = df if vendas is None else vendas.unionByName(df)
    return vendas


def transformar_clientes(spark: SparkSession) -> DataFrame:
    # Minimização (LGPD): nome, e-mail e telefone não são necessários para as
    # métricas de negócio, então não seguem para a Silver.
    bruto = spark.read.parquet(caminho(BRONZE / "clientes"))
    return (
        bruto.select(
            F.upper(_texto("customer_id")).alias("customer_id"),
            _texto("city").alias("customer_city"),
            F.upper(_texto("state")).alias("customer_state"),
            F.initcap(_texto("segment")).alias("segment"),
            F.to_date("registration_date").alias("registration_date"),
            "_source", "_ingestion_ts",
        )
        .filter(F.col("customer_id").isNotNull())
        .dropDuplicates(["customer_id"])
    )


def transformar_categorias(spark: SparkSession) -> DataFrame:
    bruto = spark.read.parquet(caminho(BRONZE / "categorias"))
    return (
        bruto.select(F.explode("categorias").alias("cat"), "_source", "_ingestion_ts")
        .select(
            F.col("cat.category_id").alias("category_id"),
            F.col("cat.category_name").alias("category_name"),
            F.size("cat.subcategories").alias("qtd_subcategorias"),
            "_source", "_ingestion_ts",
        )
    )


def enriquecer(validos: DataFrame) -> DataFrame:
    # Regra de catálogo do lab da Aula 2: 5.000 produtos, 500 por categoria
    # PROD_0001–0500 → CAT_01, PROD_0501–1000 → CAT_02, ... (o JSON de
    # categorias não traz product_id, por isso a ligação usa essa regra)
    num_produto = F.regexp_extract("product_id", r"PROD_(\d+)", 1).cast("int")
    return (
        validos
        .withColumn("category_id",
                    F.format_string("CAT_%02d", F.ceil(num_produto / F.lit(500)).cast("int")))
        .withColumn("ano_mes", F.date_format("order_date", "yyyy-MM"))
        .withColumn("_silver_ts", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
def main() -> None:
    args = parse_args("Silver: normalização + qualidade + quarentena")
    spark = get_spark("shopbrasil_silver_transformacao")
    log(logger, "Início da Silver", data_ref=args.data_ref, run_id=args.run_id)

    # Dimensões
    transformar_clientes(spark).write.mode("overwrite").parquet(caminho(SILVER / "clientes"))
    transformar_categorias(spark).coalesce(1).write.mode("overwrite").parquet(caminho(SILVER / "categorias"))
    clientes = spark.read.parquet(caminho(SILVER / "clientes"))
    categorias = spark.read.parquet(caminho(SILVER / "categorias"))

    # Fato vendas
    vendas = carregar_vendas_bronze(spark)
    total_entrada = vendas.count()

    validos, quarentena = aplicar_quarentena(vendas, clientes)
    silver = enriquecer(validos)

    (silver.repartition("ano_mes")
        .write.mode("overwrite").partitionBy("ano_mes")
        # overwrite estático: a Silver é recalculada inteira a cada execução
        .option("partitionOverwriteMode", "static")
        .parquet(caminho(SILVER / "vendas")))
    quarentena.coalesce(1).write.mode("overwrite").parquet(caminho(QUARENTENA / "vendas"))

    n_silver = spark.read.parquet(caminho(SILVER / "vendas")).count()
    quarentena_salva = spark.read.parquet(caminho(QUARENTENA / "vendas"))
    n_quar = quarentena_salva.count()
    por_regra = resumir_quarentena(quarentena_salva)
    por_fonte = {r["_source"]: r["count"] for r in quarentena_salva.groupBy("_source").count().collect()}

    metricas = {
        "data_ref": args.data_ref, "run_id": args.run_id,
        "entrada_bronze": total_entrada,
        "silver_vendas": n_silver,
        "quarentena": n_quar,
        "taxa_quarentena": round(n_quar / total_entrada, 4) if total_entrada else None,
        "conservacao_ok": total_entrada == n_silver + n_quar,
        "quarentena_por_regra": por_regra,
        "quarentena_por_fonte": por_fonte,
        "silver_clientes": clientes.count(),
        "silver_categorias": categorias.count(),
    }
    salvar_metricas("silver", metricas)
    log(logger, "Silver concluída", entrada=total_entrada, silver=n_silver, quarentena=n_quar,
        conservacao_ok=metricas["conservacao_ok"])
    spark.stop()


if __name__ == "__main__":
    main()
