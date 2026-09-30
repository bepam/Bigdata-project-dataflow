"""
Testes unitários das regras de qualidade (rodam fora do Docker, sem Airflow).

    pip install pyspark==3.5.1 pytest
    pytest -q tests/
"""
from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from quality.checks import aplicar_quarentena

COLS = ("order_id customer_id product_id quantity unit_price total_amount order_date "
        "payment_method shipping_city shipping_state status partner_source "
        "_source _source_file _ingestion_ts order_ts").split()


@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
    yield s
    s.stop()


def _venda(order_id, **kw):
    ts = datetime(2023, 5, 10)
    base = dict(order_id=order_id, customer_id="C1", product_id="PROD_0001", quantity=2,
                unit_price=10.0, total_amount=20.0, order_date=ts.date(), payment_method="pix",
                shipping_city="Santos", shipping_state="SP", status="delivered",
                partner_source="parceiro_a", _source="parceiro_a", _source_file="f.csv",
                _ingestion_ts=ts, order_ts=ts)
    base.update(kw)
    return tuple(base[c] for c in COLS)


def _rodar(spark, linhas):
    schema = ("order_id string, customer_id string, product_id string, quantity int, unit_price double, "
              "total_amount double, order_date date, payment_method string, shipping_city string, "
              "shipping_state string, status string, partner_source string, _source string, "
              "_source_file string, _ingestion_ts timestamp, order_ts timestamp")
    vendas = spark.createDataFrame(linhas, schema)
    clientes = spark.createDataFrame([("C1",)], "customer_id string")
    validos, quarentena = aplicar_quarentena(vendas, clientes)
    motivos = {r["order_id"]: r["_motivos"] for r in quarentena.collect()}
    return validos.count(), motivos


def test_registro_valido_passa(spark):
    n_validos, motivos = _rodar(spark, [_venda("A")])
    assert n_validos == 1 and motivos == {}


def test_cada_dimensao_vai_para_quarentena(spark):
    linhas = [
        _venda("NULO", shipping_state=None),                               # completude
        _venda("STATUS", status="erro"),                                    # domínio
        _venda("UF", shipping_state="XX"),                                  # domínio
        _venda("NEG", total_amount=-20.0),                                  # domínio (+ consistência)
        _venda("FUTURO", order_date=datetime(2025, 1, 1).date()),           # domínio (período)
        _venda("CALC", total_amount=99.0),                                  # consistência
        _venda("ORFAO", customer_id="C999"),                                # integridade
    ]
    n_validos, motivos = _rodar(spark, linhas)
    assert n_validos == 0
    assert motivos["NULO"] == ["completude.shipping_state"]   # nulo não conta 2x
    assert motivos["STATUS"] == ["dominio.status"]
    assert motivos["UF"] == ["dominio.shipping_state"]
    assert "dominio.total_amount_positivo" in motivos["NEG"]
    assert motivos["FUTURO"] == ["dominio.order_date_no_periodo"]
    assert motivos["CALC"] == ["consistencia.total_calculado"]
    assert motivos["ORFAO"] == ["integridade.customer_id"]


def test_unicidade_mantem_primeira_e_conserva_total(spark):
    linhas = [_venda("DUP"), _venda("DUP"), _venda("DUP", status="erro"), _venda("UNICO")]
    n_validos, motivos = _rodar(spark, linhas)
    # 1 DUP válido + UNICO na Silver; 1 duplicata + 1 inválido na quarentena
    assert n_validos == 2
    assert n_validos + 2 == len(linhas)  # conservação
