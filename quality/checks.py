"""
Framework de Qualidade de Dados — ShopBrasil (implementação própria, sem GE/Soda)
=================================================================================

Duas responsabilidades:

1) REGRAS DE LINHA + QUARENTENA (usadas pela Silver, em transformacao.py)
   Cada regra é uma expressão booleana "o registro é válido?". Um registro
   que falha em qualquer regra vai para a quarentena com a lista de motivos.
   Dimensões cobertas:
     - completude          campos obrigatórios não nulos / não vazios
     - validade_dominio    status, forma de pagamento, UF, valores > 0, período
     - consistencia        total_amount == quantity × unit_price
     - integridade         customer_id existe no cadastro de clientes
     - unicidade           order_id único (duplicatas excedentes → quarentena)

2) QUALITY GATE (task própria na DAG)
   Reavalia as camadas já gravadas e decide se o pipeline pode seguir:
     python quality/checks.py --camada silver   (antes da Gold)
     python quality/checks.py --camada gold     (antes de notificar o negócio)
   Se algum check bloqueante falhar, o processo termina com erro → a task do
   Airflow falha → callback de alerta dispara e a Gold não é publicada.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Parâmetros de negócio (domínios válidos e limites do gate)
# ---------------------------------------------------------------------------
STATUS_VALIDOS = ["pending", "shipped", "delivered", "cancelled"]
PAGAMENTOS_VALIDOS = ["credit_card", "debit_card", "pix", "boleto"]
UFS_VALIDAS = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA",
    "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO",
]
# Contrato ShopBrasil: período de apuração dos dados enviados pelos parceiros
PERIODO_INICIO = "2023-01-01"
PERIODO_FIM = "2023-12-31"
TOLERANCIA_TOTAL = 0.01  # R$ — arredondamento aceitável em total = qtd × preço

CAMPOS_OBRIGATORIOS = [
    "order_id", "customer_id", "product_id", "quantity", "unit_price", "total_amount",
    "order_date", "payment_method", "shipping_city", "shipping_state", "status",
]

# Limites do gate — sobrescrevíveis por variável de ambiente (útil na demo:
# QUALITY_VOLUME_MINIMO_SILVER=200000 faz o gate reprovar e bloquear a Gold)
LIMITES_GATE = {
    "taxa_quarentena_max": float(os.environ.get("QUALITY_TAXA_QUARENTENA_MAX", "0.10")),
    "volume_minimo_silver": int(os.environ.get("QUALITY_VOLUME_MINIMO_SILVER", "50000")),
}


# ---------------------------------------------------------------------------
# 1) Regras de linha
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Regra:
    nome: str
    dimensao: str
    descricao: str
    valido: Callable[[], Column]  # expressão avaliada linha a linha (True = OK)


def _preenchido(coluna: str) -> Column:
    c = F.col(coluna)
    return c.isNotNull() & (F.trim(c.cast("string")) != "")


def construir_regras() -> list[Regra]:
    regras: list[Regra] = []

    # --- Completude: um check por campo obrigatório (motivo fica específico)
    for campo in CAMPOS_OBRIGATORIOS:
        regras.append(Regra(
            nome=f"completude.{campo}",
            dimensao="completude",
            descricao=f"{campo} obrigatório",
            valido=lambda campo=campo: _preenchido(campo),
        ))

    # --- Validade de domínio / consistência / integridade
    # Valor ausente NÃO é avaliado aqui (já reprovado pela completude): assim
    # cada problema aparece uma única vez no relatório da quarentena.
    def se_preenchido(*cols: str):
        def decorar(cond: Callable[[], Column]) -> Callable[[], Column]:
            def expr() -> Column:
                algum_nulo = F.lit(False)
                for c in cols:
                    algum_nulo = algum_nulo | F.col(c).isNull()
                return algum_nulo | cond()
            return expr
        return decorar

    regras += [
        Regra("dominio.status", "validade_dominio", f"status ∈ {STATUS_VALIDOS}",
              se_preenchido("status")(lambda: F.col("status").isin(STATUS_VALIDOS))),
        Regra("dominio.payment_method", "validade_dominio", f"forma de pagamento ∈ {PAGAMENTOS_VALIDOS}",
              se_preenchido("payment_method")(lambda: F.col("payment_method").isin(PAGAMENTOS_VALIDOS))),
        Regra("dominio.shipping_state", "validade_dominio", "UF brasileira válida",
              se_preenchido("shipping_state")(lambda: F.col("shipping_state").isin(UFS_VALIDAS))),
        Regra("dominio.quantity_positiva", "validade_dominio", "quantity > 0",
              se_preenchido("quantity")(lambda: F.col("quantity") > 0)),
        Regra("dominio.unit_price_positivo", "validade_dominio", "unit_price > 0",
              se_preenchido("unit_price")(lambda: F.col("unit_price") > 0)),
        Regra("dominio.total_amount_positivo", "validade_dominio", "total_amount > 0",
              se_preenchido("total_amount")(lambda: F.col("total_amount") > 0)),
        Regra("dominio.order_date_no_periodo", "validade_dominio",
              f"order_date entre {PERIODO_INICIO} e {PERIODO_FIM}",
              se_preenchido("order_date")(lambda: F.col("order_date").between(
                  F.lit(PERIODO_INICIO).cast("date"), F.lit(PERIODO_FIM).cast("date")))),
        Regra("consistencia.total_calculado", "consistencia",
              f"|total_amount − quantity × unit_price| ≤ {TOLERANCIA_TOTAL}",
              se_preenchido("total_amount", "quantity", "unit_price")(
                  lambda: F.abs(F.col("total_amount") - F.col("quantity") * F.col("unit_price"))
                  <= TOLERANCIA_TOTAL)),
        # _cliente_existe é criada pelo join com o cadastro (aplicar_quarentena)
        Regra("integridade.customer_id", "integridade", "customer_id existe no cadastro de clientes",
              se_preenchido("customer_id")(lambda: F.col("_cliente_existe"))),
    ]
    return regras


def avaliar_regras(df: DataFrame, regras: list[Regra]) -> DataFrame:
    """Adiciona _motivos: array com o nome de cada regra que falhou (vazio = válido)."""
    falhas = [
        # coalesce(..., False): comparação com NULL também é falha
        F.when(~F.coalesce(r.valido(), F.lit(False)), F.lit(r.nome))
        for r in regras
    ]
    return df.withColumn("_motivos", F.array_compact(F.array(*falhas)))


def aplicar_quarentena(
    vendas: DataFrame, clientes_ids: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """
    Separa registros válidos (→ Silver) de inválidos (→ Quarentena).

    Garante CONSERVAÇÃO: len(entrada) == len(validos) + len(quarentena).
    """
    # Integridade referencial via broadcast join (só a coluna de chave dos clientes)
    ids = clientes_ids.select("customer_id").distinct().withColumn("_cliente_existe", F.lit(True))
    base = (
        vendas.join(F.broadcast(ids), on="customer_id", how="left")
        .withColumn("_cliente_existe", F.coalesce("_cliente_existe", F.lit(False)))
    )
    avaliado = avaliar_regras(base, construir_regras()).cache()

    # Unicidade: entre os registros que passaram nas demais regras, mantém
    # a primeira ocorrência de cada order_id; as excedentes vão à quarentena.
    # (particiona também por "é válido?" para que um inválido não "ocupe"
    #  a 1ª posição e empurre o registro válido para a quarentena)
    ordem = (
        Window.partitionBy("order_id", "_valido_regras")
        .orderBy("_ingestion_ts", "_source", "_source_file", "order_ts")
    )
    avaliado = (
        avaliado.withColumn("_valido_regras", F.size("_motivos") == 0)
        .withColumn(
            "_ocorrencia",
            F.when(F.size("_motivos") == 0, F.row_number().over(ordem)),
        )
        .withColumn(
            "_motivos",
            F.when(F.col("_ocorrencia") > 1, F.array(F.lit("unicidade.order_id")))
            .otherwise(F.col("_motivos")),
        )
    )

    validos = (
        avaliado.filter(F.size("_motivos") == 0)
        .drop("_motivos", "_ocorrencia", "_cliente_existe", "_valido_regras")
    )
    quarentena = (
        avaliado.filter(F.size("_motivos") > 0)
        .withColumn("_motivo_principal", F.col("_motivos")[0])
        .withColumn("_dimensao", F.split(F.col("_motivo_principal"), r"\.")[0])
        .withColumn("_quarentena_ts", F.current_timestamp())
        .drop("_ocorrencia", "_cliente_existe", "_valido_regras")
    )
    return validos, quarentena


def resumir_quarentena(quarentena: DataFrame) -> dict:
    """Contagem de falhas por regra (um registro pode falhar em várias)."""
    linhas = (
        quarentena.select(F.explode("_motivos").alias("regra"))
        .groupBy("regra").count().orderBy(F.desc("count")).collect()
    )
    return {r["regra"]: r["count"] for r in linhas}


# ---------------------------------------------------------------------------
# 2) Quality gate sobre as camadas persistidas
# ---------------------------------------------------------------------------
class QualityGateError(Exception):
    pass


def _check(nome: str, dimensao: str, passou: bool, valor, esperado, bloqueante: bool = True) -> dict:
    return {"check": nome, "dimensao": dimensao, "status": "PASS" if passou else "FAIL",
            "valor": valor, "esperado": esperado, "bloqueante": bloqueante}


def gate_silver(spark: SparkSession, paths) -> list[dict]:
    bronze_total = sum(
        spark.read.parquet(str(paths.BRONZE / "vendas" / p)).count() for p in paths.PARCEIROS
    )
    silver = spark.read.parquet(str(paths.SILVER / "vendas")).cache()
    quarentena = spark.read.parquet(str(paths.QUARENTENA / "vendas"))
    n_silver, n_quar = silver.count(), quarentena.count()
    taxa = n_quar / bronze_total if bronze_total else 1.0

    nulos = silver.select([
        F.sum((~_preenchido(c)).cast("int")).alias(c) for c in CAMPOS_OBRIGATORIOS
    ]).first().asDict()
    total_nulos = sum(v or 0 for v in nulos.values())
    duplicados = n_silver - silver.select("order_id").distinct().count()

    regras_linha = [r for r in construir_regras() if r.dimensao != "integridade"]
    fora_dominio = avaliar_regras(silver, regras_linha).filter(F.size("_motivos") > 0).count()
    clientes = spark.read.parquet(str(paths.SILVER / "clientes")).select("customer_id")
    orfaos = silver.join(clientes, "customer_id", "left_anti").count()

    return [
        _check("conservacao_bronze_silver_quarentena", "conservacao",
               bronze_total == n_silver + n_quar,
               {"bronze": bronze_total, "silver": n_silver, "quarentena": n_quar},
               "bronze == silver + quarentena"),
        _check("completude_silver", "completude", total_nulos == 0,
               {k: v for k, v in nulos.items() if v}, "0 nulos em campos obrigatórios"),
        _check("unicidade_order_id_silver", "unicidade", duplicados == 0, duplicados, 0),
        _check("validade_dominio_silver", "validade_dominio", fora_dominio == 0, fora_dominio, 0),
        _check("integridade_clientes_silver", "integridade", orfaos == 0, orfaos,
               "0 vendas com cliente fora do cadastro"),
        _check("taxa_quarentena", "monitoramento", taxa <= LIMITES_GATE["taxa_quarentena_max"],
               round(taxa, 4), f"<= {LIMITES_GATE['taxa_quarentena_max']}"),
        _check("volume_minimo_silver", "volume", n_silver >= LIMITES_GATE["volume_minimo_silver"],
               n_silver, f">= {LIMITES_GATE['volume_minimo_silver']}"),
    ]


def gate_gold(spark: SparkSession, paths) -> list[dict]:
    silver = spark.read.parquet(str(paths.SILVER / "vendas"))
    faturado = silver.filter(F.col("status") != "cancelled")
    fat_silver = faturado.agg(F.sum("total_amount")).first()[0] or 0.0
    pedidos_silver = faturado.count()

    estado = spark.read.parquet(str(paths.GOLD / "faturamento_por_estado"))
    mensal = spark.read.parquet(str(paths.GOLD / "analise_mensal"))
    pagamento = spark.read.parquet(str(paths.GOLD / "faturamento_por_pagamento"))
    categoria = spark.read.parquet(str(paths.GOLD / "faturamento_categoria_segmento"))

    fat_estado = estado.agg(F.sum("faturamento")).first()[0] or 0.0
    fat_mensal = mensal.agg(F.sum("faturamento")).first()[0] or 0.0
    fat_pagamento = pagamento.agg(F.sum("faturamento")).first()[0] or 0.0
    fat_categoria = categoria.agg(F.sum("faturamento")).first()[0] or 0.0
    ped_mensal = mensal.agg(F.sum("pedidos_faturados")).first()[0] or 0

    def bate(a, b):  # tolerância de centavos por arredondamento
        return abs(a - b) < 1.0

    negativos = estado.filter(F.col("faturamento") < 0).count() + mensal.filter(F.col("faturamento") < 0).count()
    return [
        _check("reconciliacao_estado_vs_silver", "consistencia", bate(fat_estado, fat_silver),
               round(fat_estado, 2), round(fat_silver, 2)),
        _check("reconciliacao_mensal_vs_silver", "consistencia", bate(fat_mensal, fat_silver),
               round(fat_mensal, 2), round(fat_silver, 2)),
        _check("reconciliacao_pagamento_vs_silver", "consistencia", bate(fat_pagamento, fat_silver),
               round(fat_pagamento, 2), round(fat_silver, 2)),
        _check("reconciliacao_categoria_vs_silver", "consistencia", bate(fat_categoria, fat_silver),
               round(fat_categoria, 2), round(fat_silver, 2)),
        _check("gold_sem_categoria_ou_segmento_desconhecido", "integridade",
               categoria.filter(F.col("category_name").isin("Sem categoria")
                                | F.col("segment").isin("Sem segmento")).count() == 0,
               categoria.filter(F.col("category_name").isin("Sem categoria")
                                | F.col("segment").isin("Sem segmento")).count(), 0),
        _check("pedidos_mensal_vs_silver", "consistencia", ped_mensal == pedidos_silver,
               ped_mensal, pedidos_silver),
        _check("gold_estado_27_ufs", "completude", estado.count() == len(UFS_VALIDAS),
               estado.count(), len(UFS_VALIDAS), bloqueante=False),
        _check("gold_sem_faturamento_negativo", "validade_dominio", negativos == 0, negativos, 0),
    ]


def executar_gate(camada: str, data_ref: str, run_id: str, taxa_max: float | None = None,
                  volume_min: int | None = None) -> dict:
    from spark_jobs import common as paths
    from spark_jobs.common import get_logger, get_spark, log, salvar_metricas

    logger = get_logger(f"quality.gate_{camada}")
    if taxa_max is not None:  # parâmetro da DAG (Trigger DAG w/ config)
        LIMITES_GATE["taxa_quarentena_max"] = taxa_max
    if volume_min is not None:
        LIMITES_GATE["volume_minimo_silver"] = volume_min
    spark = get_spark(f"shopbrasil_quality_gate_{camada}")

    checks = gate_silver(spark, paths) if camada == "silver" else gate_gold(spark, paths)
    falhas_bloqueantes = [c for c in checks if c["status"] == "FAIL" and c["bloqueante"]]
    resultado = {
        "camada": camada, "data_ref": data_ref, "run_id": run_id,
        "status": "FAILED" if falhas_bloqueantes else "PASSED",
        "checks_total": len(checks),
        "checks_ok": sum(c["status"] == "PASS" for c in checks),
        "checks": checks,
    }
    if camada == "silver":
        resultado["quarentena_por_regra"] = paths.ler_metricas("silver").get("quarentena_por_regra", {})
    for c in checks:
        log(logger, f"[{c['status']}] {c['check']}", valor=c["valor"], esperado=c["esperado"])
    salvar_metricas(f"gate_{camada}", resultado)

    # Histórico append-only para monitoramento da qualidade ao longo do tempo
    historico = spark.createDataFrame([
        (datetime.now(), data_ref, run_id, camada, c["check"], c["dimensao"], c["status"], str(c["valor"]))
        for c in checks
    ], "executado_em timestamp, data_ref string, run_id string, camada string, check string, "
       "dimensao string, status string, valor string")
    historico.coalesce(1).write.mode("append").parquet(str(paths.HISTORICO))

    spark.stop()
    if falhas_bloqueantes:
        nomes = [c["check"] for c in falhas_bloqueantes]
        raise QualityGateError(f"Quality gate {camada} REPROVADO: {nomes}")
    log(logger, f"Quality gate {camada} APROVADO", checks_ok=resultado["checks_ok"])
    return resultado


if __name__ == "__main__":
    from spark_jobs.common import parse_args

    args = parse_args(
        "Quality gate das camadas Silver/Gold",
        extra=lambda p: (
            p.add_argument("--camada", choices=["silver", "gold"], required=True),
            p.add_argument("--taxa-quarentena-max", type=float, default=None,
                           help="Sobrescreve o limite de taxa de quarentena do gate"),
            p.add_argument("--volume-minimo", type=int, default=None,
                           help="Sobrescreve o volume mínimo exigido na Silver"),
        ),
    )
    executar_gate(args.camada, args.data_ref, args.run_id, args.taxa_quarentena_max, args.volume_minimo)
