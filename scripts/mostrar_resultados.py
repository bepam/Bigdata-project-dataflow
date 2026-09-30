"""
Mostra no terminal o resultado da última execução do pipeline (para a demo).

Uso (com o ambiente no ar):
    docker compose exec airflow-scheduler python scripts/mostrar_resultados.py
    docker compose exec airflow-scheduler python scripts/mostrar_resultados.py --secao quarentena

Lê direto os Parquet das camadas com pandas — não precisa de Spark.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import pandas as pd

DATA = Path(os.environ.get("DATA_DIR", "/opt/airflow/data"))
pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)
pd.set_option("display.float_format", lambda v: f"{v:,.2f}")


def titulo(txt: str) -> None:
    print("\n" + "=" * 80 + f"\n  {txt}\n" + "=" * 80)


def metricas(nome: str) -> dict:
    arq = DATA / "quality" / "ultima_execucao" / f"{nome}.json"
    return json.loads(arq.read_text(encoding="utf-8")) if arq.exists() else {}


def secao_fluxo() -> None:
    titulo("FLUXO DE DADOS  (Bronze → Silver + Quarentena)")
    b, s = metricas("bronze"), metricas("silver")
    for fonte, m in b.get("fontes", {}).items():
        print(f"  bronze/{fonte:<12} {m['registros']:>9,} registros  ({m['arquivos']} arquivo(s))")
    print(f"\n  Vendas na Bronze : {s.get('entrada_bronze', 0):>9,}")
    print(f"  Vendas na Silver : {s.get('silver_vendas', 0):>9,}")
    print(f"  Em quarentena    : {s.get('quarentena', 0):>9,}  ({s.get('taxa_quarentena', 0):.2%})")
    print(f"  Conservação OK?  : {s.get('conservacao_ok')}")


def secao_gates() -> None:
    for camada in ("silver", "gold"):
        g = metricas(f"gate_{camada}")
        titulo(f"QUALITY GATE {camada.upper()} — {g.get('status')} ({g.get('checks_ok')}/{g.get('checks_total')})")
        for c in g.get("checks", []):
            print(f"  [{c['status']}] {c['check']:<40} valor={c['valor']}  esperado={c['esperado']}")


def secao_quarentena() -> None:
    titulo("QUARENTENA — motivos")
    for regra, qtd in metricas("silver").get("quarentena_por_regra", {}).items():
        print(f"  {regra:<35} {qtd:>7,}")
    caminho = DATA / "quarentena" / "vendas"
    if not metricas("silver").get("quarentena"):
        print("  Nenhum registro em quarentena nesta execução — todos os checks passaram.")
    elif caminho.exists():
        df = pd.read_parquet(caminho)
        print("\n  Exemplos (1 por motivo principal):")
        ex = df.groupby("_motivo_principal").head(1)[
            ["_motivo_principal", "order_id", "customer_id", "quantity", "unit_price",
             "total_amount", "order_date", "status", "shipping_state", "_source"]
        ]
        print(ex.to_string(index=False))


def secao_gold() -> None:
    gold = DATA / "gold"
    titulo("GOLD · faturamento_por_estado (top 10)")
    est = pd.read_parquet(gold / "faturamento_por_estado").sort_values("ranking_faturamento")
    print(est[["ranking_faturamento", "uf", "regiao", "faturamento", "participacao_pct",
               "pedidos_faturados", "ticket_medio", "taxa_cancelamento_pct"]].head(10).to_string(index=False))

    titulo("GOLD · analise_mensal")
    men = pd.read_parquet(gold / "analise_mensal").sort_values("ano_mes")
    print(men[["ano_mes", "faturamento", "crescimento_mom_pct", "faturamento_acumulado",
               "pedidos_faturados", "ticket_medio"]].to_string(index=False))

    titulo("GOLD · faturamento_por_pagamento")
    pag = pd.read_parquet(gold / "faturamento_por_pagamento").sort_values("faturamento", ascending=False)
    print(pag[["forma_pagamento", "faturamento", "participacao_pct", "pedidos_faturados",
               "taxa_cancelamento_pct", "ticket_medio"]].to_string(index=False))

    titulo("GOLD · faturamento_categoria_segmento (top 10)")
    cat = pd.read_parquet(gold / "faturamento_categoria_segmento").sort_values("faturamento", ascending=False)
    print(cat[["category_name", "segment", "faturamento", "participacao_na_categoria_pct",
               "pedidos_faturados", "ticket_medio"]].head(10).to_string(index=False))

    kpis = metricas("gold").get("kpis", {})
    if kpis:
        titulo("KPIs EXECUTIVOS")
        for k, v in kpis.items():
            print(f"  {k:<20} {v}")


SECOES = {"fluxo": secao_fluxo, "gates": secao_gates, "quarentena": secao_quarentena, "gold": secao_gold}

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--secao", choices=[*SECOES, "tudo"], default="tudo")
    args = ap.parse_args()
    for nome, func in SECOES.items():
        if args.secao in (nome, "tudo"):
            func()
    print()
