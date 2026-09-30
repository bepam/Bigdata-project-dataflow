"""
Camada GOLD — Métricas de negócio para o dashboard executivo da ShopBrasil
===========================================================================

Entrada : silver/vendas, silver/clientes, silver/categorias
Saída   :
  gold/faturamento_por_estado          faturamento, pedidos, ticket médio,
                                       cancelamento, participação e ranking por UF
  gold/analise_mensal                  série mensal com crescimento MoM (lag)
                                       e faturamento acumulado (window)
  gold/faturamento_por_pagamento       faturamento, participação e cancelamento
                                       por forma de pagamento
  gold/faturamento_categoria_segmento  categoria de produto × segmento de cliente
                                       (broadcast join com as dimensões)

Regra de negócio: FATURAMENTO considera apenas pedidos não cancelados.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from spark_jobs.common import (
    GOLD, SILVER, caminho, get_logger, get_spark, log, parse_args, salvar_metricas,
)

logger = get_logger("gold.agregacao")

REGIOES = {
    "Norte": ["AC", "AP", "AM", "PA", "RO", "RR", "TO"],
    "Nordeste": ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"],
    "Centro-Oeste": ["DF", "GO", "MT", "MS"],
    "Sudeste": ["ES", "MG", "RJ", "SP"],
    "Sul": ["PR", "RS", "SC"],
}

def _faturado() -> F.Column:
    """Pedido conta como faturamento se não foi cancelado."""
    return F.col("status") != "cancelled"


def _regiao(col_uf: str) -> F.Column:
    expr = None
    for regiao, ufs in REGIOES.items():
        cond = F.col(col_uf).isin(ufs)
        expr = F.when(cond, regiao) if expr is None else expr.when(cond, regiao)
    return expr.otherwise("Desconhecida")


def _metricas_base() -> list[F.Column]:
    return [
        F.count("*").alias("pedidos_total"),
        F.sum(_faturado().cast("int")).alias("pedidos_faturados"),
        F.sum((~_faturado()).cast("int")).alias("pedidos_cancelados"),
        F.round(F.sum(F.when(_faturado(), F.col("total_amount")).otherwise(0.0)), 2).alias("faturamento"),
        F.countDistinct(F.when(_faturado(), F.col("customer_id"))).alias("clientes_unicos"),
        F.sum(F.when(_faturado(), F.col("quantity")).otherwise(0)).alias("itens_vendidos"),
    ]


def _derivadas(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("ticket_medio", F.round(F.col("faturamento") / F.col("pedidos_faturados"), 2))
        .withColumn("taxa_cancelamento_pct",
                    F.round(F.col("pedidos_cancelados") / F.col("pedidos_total") * 100, 2))
    )


def faturamento_por_estado(vendas: DataFrame) -> DataFrame:
    total = Window.partitionBy()  # janela global para participação %
    ranking = Window.orderBy(F.desc("faturamento"))
    return (
        _derivadas(vendas.groupBy("shipping_state").agg(*_metricas_base()))
        .withColumn("regiao", _regiao("shipping_state"))
        .withColumn("participacao_pct",
                    F.round(F.col("faturamento") / F.sum("faturamento").over(total) * 100, 2))
        .withColumn("ranking_faturamento", F.dense_rank().over(ranking))
        .withColumnRenamed("shipping_state", "uf")
        .select("ranking_faturamento", "uf", "regiao", "faturamento", "participacao_pct",
                "pedidos_faturados", "pedidos_cancelados", "pedidos_total", "taxa_cancelamento_pct",
                "ticket_medio", "clientes_unicos", "itens_vendidos")
        .orderBy("ranking_faturamento")
    )


def analise_mensal(vendas: DataFrame) -> DataFrame:
    cronologica = Window.orderBy("ano_mes")
    acumulada = cronologica.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return (
        _derivadas(vendas.groupBy("ano_mes").agg(*_metricas_base()))
        .withColumn("faturamento_mes_anterior", F.lag("faturamento").over(cronologica))
        .withColumn("crescimento_mom_pct",
                    F.round((F.col("faturamento") / F.col("faturamento_mes_anterior") - 1) * 100, 2))
        .withColumn("faturamento_acumulado", F.round(F.sum("faturamento").over(acumulada), 2))
        .withColumn("participacao_ano_pct",
                    F.round(F.col("faturamento") / F.sum("faturamento").over(Window.partitionBy()) * 100, 2))
        .select("ano_mes", "faturamento", "faturamento_mes_anterior", "crescimento_mom_pct",
                "faturamento_acumulado", "participacao_ano_pct", "pedidos_faturados",
                "pedidos_cancelados", "pedidos_total", "taxa_cancelamento_pct", "ticket_medio",
                "clientes_unicos", "itens_vendidos")
        .orderBy("ano_mes")
    )


def faturamento_por_pagamento(vendas: DataFrame) -> DataFrame:
    total = Window.partitionBy()
    return (
        _derivadas(vendas.groupBy("payment_method").agg(*_metricas_base()))
        .withColumn("participacao_pct",
                    F.round(F.col("faturamento") / F.sum("faturamento").over(total) * 100, 2))
        .withColumnRenamed("payment_method", "forma_pagamento")
        .select("forma_pagamento", "faturamento", "participacao_pct", "pedidos_faturados",
                "pedidos_cancelados", "pedidos_total", "taxa_cancelamento_pct", "ticket_medio",
                "clientes_unicos")
        .orderBy(F.desc("faturamento"))
    )


def faturamento_categoria_segmento(vendas: DataFrame, clientes: DataFrame, categorias: DataFrame) -> DataFrame:
    # Dimensões pequenas → broadcast evita shuffle da tabela fato (Aula 2)
    base = (
        vendas.join(F.broadcast(categorias.select("category_id", "category_name")), "category_id", "left")
        .join(clientes.select("customer_id", "segment"), "customer_id", "left")
        .fillna({"category_name": "Sem categoria", "segment": "Sem segmento"})
    )
    por_categoria = Window.partitionBy("category_name")
    return (
        _derivadas(base.groupBy("category_name", "segment").agg(*_metricas_base()))
        .withColumn("participacao_na_categoria_pct",
                    F.round(F.col("faturamento") / F.sum("faturamento").over(por_categoria) * 100, 2))
        .select("category_name", "segment", "faturamento", "participacao_na_categoria_pct",
                "pedidos_faturados", "pedidos_cancelados", "taxa_cancelamento_pct",
                "ticket_medio", "clientes_unicos")
        .orderBy("category_name", F.desc("faturamento"))
    )


def main() -> None:
    args = parse_args("Gold: agregações de negócio")
    spark = get_spark("shopbrasil_gold_agregacao")
    log(logger, "Início da Gold", data_ref=args.data_ref, run_id=args.run_id)

    vendas = spark.read.parquet(caminho(SILVER / "vendas")).cache()
    clientes = spark.read.parquet(caminho(SILVER / "clientes"))
    categorias = spark.read.parquet(caminho(SILVER / "categorias"))

    tabelas = {
        "faturamento_por_estado": faturamento_por_estado(vendas),
        "analise_mensal": analise_mensal(vendas),
        "faturamento_por_pagamento": faturamento_por_pagamento(vendas),
        "faturamento_categoria_segmento": faturamento_categoria_segmento(vendas, clientes, categorias),
    }

    metricas = {"data_ref": args.data_ref, "run_id": args.run_id, "tabelas": {}}
    for nome, df in tabelas.items():
        destino = GOLD / nome
        # Tabelas pequenas: 1 arquivo facilita consumo pelo dashboard/BI
        df.coalesce(1).write.mode("overwrite").parquet(caminho(destino))
        n = spark.read.parquet(caminho(destino)).count()
        metricas["tabelas"][nome] = {"linhas": n, "destino": caminho(destino)}
        log(logger, "Tabela Gold gravada", tabela=nome, linhas=n)

    kpis = vendas.agg(
        F.round(F.sum(F.when(_faturado(), F.col("total_amount")).otherwise(0.0)), 2).alias("faturamento_total"),
        F.sum(_faturado().cast("int")).alias("pedidos_faturados"),
        F.countDistinct("customer_id").alias("clientes_unicos"),
    ).first().asDict()
    kpis["ticket_medio"] = round(kpis["faturamento_total"] / kpis["pedidos_faturados"], 2)
    top_uf = spark.read.parquet(caminho(GOLD / "faturamento_por_estado")).orderBy("ranking_faturamento").first()
    kpis["top_uf"] = {"uf": top_uf["uf"], "participacao_pct": top_uf["participacao_pct"]}
    metricas["kpis"] = kpis

    salvar_metricas("gold", metricas)
    log(logger, "Gold concluída", **kpis)
    spark.stop()


if __name__ == "__main__":
    main()
