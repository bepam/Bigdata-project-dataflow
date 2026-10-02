"""
DAG: shopbrasil_pipeline_vendas — DataFlow Analytics × ShopBrasil
==================================================================

Pipeline de produção (arquitetura Medallion) orquestrado pelo Airflow:

  aguardar_arquivos_vendas      PythonSensor (reschedule) — vendas, clientes e categorias chegaram?
        │
  bronze_ingestao               spark-submit ingestao.py      (raw → bronze)
        │
  silver_transformacao          spark-submit transformacao.py (bronze → silver + quarentena)
        │
  quality_gate_silver           spark-submit checks.py --camada silver   ◀ bloqueia a Gold
        │
  gold_agregacao                spark-submit agregacao.py     (silver → gold)
        │
  quality_gate_gold             spark-submit checks.py --camada gold     ◀ reconciliação
        │
  verificar_quarentena          BranchPythonOperator
     ├── alertar_quarentena     (há registros em quarentena → alerta p/ time de dados)
     └── sem_pendencias
        │
  notificar_sucesso             resumo executivo (trigger_rule: none_failed_min_one_success)

Falhas em qualquer task disparam `alerta_falha` (on_failure_callback), que
registra o alerta em data/notificacoes/. Retries: 2, com backoff.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.python import PythonSensor

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
PROJECT_DIR = Path(os.environ.get("PROJECT_DIR", "/opt/airflow"))
DATA_DIR = Path(os.environ.get("DATA_DIR", PROJECT_DIR / "data"))
RAW_DIR = DATA_DIR / "raw"
METRICAS_DIR = DATA_DIR / "quality" / "ultima_execucao"
NOTIFICACOES_DIR = DATA_DIR / "notificacoes"

SPARK_CONN_ID = "spark_local"  # definida via env AIRFLOW_CONN_SPARK_LOCAL (docker-compose)

# Fonte → padrão de arquivo esperado em data/raw/
FONTES_ESPERADAS = {
    "vendas_csv": "*.csv",
    "vendas_json": "*.json",
    "vendas_parquet": "*.parquet",
    "clientes": "*.parquet",
    "categorias": "*.json",
}


# ---------------------------------------------------------------------------
# Callbacks e funções Python
# ---------------------------------------------------------------------------
def _gravar_notificacao(tipo: str, conteudo: dict) -> Path:
    """Simula o envio (Slack/e-mail) gravando a mensagem em data/notificacoes/."""
    NOTIFICACOES_DIR.mkdir(parents=True, exist_ok=True)
    arquivo = NOTIFICACOES_DIR / f"{datetime.now():%Y%m%dT%H%M%S}_{tipo}.json"
    arquivo.write_text(json.dumps(conteudo, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    return arquivo


def alerta_falha(context) -> None:
    ti = context["task_instance"]
    conteudo = {
        "tipo": "FALHA",
        "dag": ti.dag_id,
        "task": ti.task_id,
        "tentativa": ti.try_number,
        "data_ref": context["ds"],
        "run_id": context["run_id"],
        "erro": str(context.get("exception")),
        "log_url": ti.log_url,
    }
    if ti.task_id.startswith("quality_gate_"):
        gate = _ler(ti.task_id.replace("quality_", ""))  # gate_silver / gate_gold
        conteudo["checks_reprovados"] = [c for c in gate.get("checks", []) if c["status"] == "FAIL"]
    arquivo = _gravar_notificacao("falha", conteudo)
    print(f"🚨 [ALERTA] Task '{ti.task_id}' falhou — detalhes em {arquivo}")


def verificar_arquivos() -> bool:
    """Sensor: True quando TODAS as fontes têm ao menos 1 arquivo não vazio."""
    faltando = []
    for fonte, padrao in FONTES_ESPERADAS.items():
        arquivos = [a for a in (RAW_DIR / fonte).glob(padrao) if a.stat().st_size > 0]
        print(f"  {fonte:<11} {len(arquivos)} arquivo(s)")
        if not arquivos:
            faltando.append(fonte)
    if faltando:
        print(f"⏳ Aguardando fontes: {faltando}")
        return False
    print("✅ Todas as fontes disponíveis")
    return True


def _ler(nome: str) -> dict:
    arquivo = METRICAS_DIR / f"{nome}.json"
    return json.loads(arquivo.read_text(encoding="utf-8")) if arquivo.exists() else {}


def decidir_quarentena() -> str:
    silver = _ler("silver")
    qtd = silver.get("quarentena", 0)
    print(f"Registros em quarentena: {qtd} (taxa {silver.get('taxa_quarentena')})")
    return "alertar_quarentena" if qtd > 0 else "sem_pendencias"


def alertar_quarentena(**context) -> None:
    silver = _ler("silver")
    conteudo = {
        "tipo": "QUARENTENA",
        "data_ref": context["ds"],
        "run_id": context["run_id"],
        "registros_em_quarentena": silver.get("quarentena"),
        "taxa_quarentena": silver.get("taxa_quarentena"),
        "por_regra": silver.get("quarentena_por_regra"),
        "por_fonte": silver.get("quarentena_por_fonte"),
        "acao": "Revisar data/quarentena/vendas e corrigir na fonte de origem",
    }
    arquivo = _gravar_notificacao("quarentena", conteudo)
    print(f"⚠️  {conteudo['registros_em_quarentena']} registros em quarentena — {arquivo}")
    for regra, qtd in (conteudo["por_regra"] or {}).items():
        print(f"    {regra:<35} {qtd:>6}")


def notificar_sucesso(**context) -> None:
    bronze, silver, gold = _ler("bronze"), _ler("silver"), _ler("gold")
    gate_s, gate_g = _ler("gate_silver"), _ler("gate_gold")
    kpis = gold.get("kpis", {})
    resumo = {
        "tipo": "SUCESSO",
        "data_ref": context["ds"],
        "run_id": context["run_id"],
        "fluxo": {
            "bronze_vendas": bronze.get("total_vendas_bronze"),
            "silver_vendas": silver.get("silver_vendas"),
            "quarentena": silver.get("quarentena"),
            "conservacao_ok": silver.get("conservacao_ok"),
        },
        "quality_gates": {
            "silver": f"{gate_s.get('status')} ({gate_s.get('checks_ok')}/{gate_s.get('checks_total')})",
            "gold": f"{gate_g.get('status')} ({gate_g.get('checks_ok')}/{gate_g.get('checks_total')})",
        },
        "kpis": kpis,
        "tabelas_gold": list(gold.get("tabelas", {}).keys()),
    }
    _gravar_notificacao("sucesso", resumo)

    fat = kpis.get("faturamento_total", 0)
    print("=" * 62)
    print("  ✅ PIPELINE SHOPBRASIL CONCLUÍDO")
    print("=" * 62)
    f = resumo["fluxo"]
    print(f"  Bronze {f['bronze_vendas']:,} → Silver {f['silver_vendas']:,} + Quarentena {f['quarentena']:,}"
          .replace(",", "."))
    print(f"  Quality gates   silver: {resumo['quality_gates']['silver']} | gold: {resumo['quality_gates']['gold']}")
    print(f"  Faturamento     R$ {fat:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
    print(f"  Ticket médio    R$ {kpis.get('ticket_medio', 0):,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
    print(f"  Top UF          {kpis.get('top_uf')}")
    print("=" * 62)


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "dataflow-engenharia",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=20),
    "on_failure_callback": alerta_falha,
}

SPARK_ARGS = ["--data-ref", "{{ ds }}", "--run-id", "{{ run_id }}"]


def spark_task(task_id: str, script: str, extra_args: list[str] | None = None,
               retries: int | None = None) -> SparkSubmitOperator:
    kwargs = {} if retries is None else {"retries": retries}
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id=SPARK_CONN_ID,
        application=str(PROJECT_DIR / script),
        application_args=SPARK_ARGS + (extra_args or []),
        name=f"shopbrasil_{task_id}",
        driver_memory="1g",
        conf={"spark.ui.port": "4040", "spark.ui.showConsoleProgress": "false"},
        verbose=False,
        **kwargs,
    )


with DAG(
    dag_id="shopbrasil_pipeline_vendas",
    description="Medallion ShopBrasil: sensor → bronze → silver → quality → gold → notificação",
    default_args=default_args,
    schedule="0 6 * * *",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,  # camadas são sobrescritas: duas execuções simultâneas colidiriam
    tags=["shopbrasil", "medallion", "spark", "qualidade", "projeto-final"],
    doc_md=__doc__,
    # "Trigger DAG w/ config" permite mudar os limites na demo
    # (ex.: volume_minimo_silver = 200000 → gate reprova e a Gold não é publicada)
    params={
        "taxa_quarentena_max": Param(0.10, type="number", minimum=0, maximum=1,
                                     description="Taxa máxima de registros em quarentena aceita pelo gate"),
        "volume_minimo_silver": Param(50000, type="integer", minimum=0,
                                      description="Quantidade mínima de vendas válidas na Silver"),
    },
) as dag:

    aguardar = PythonSensor(
        task_id="aguardar_arquivos_vendas",
        python_callable=verificar_arquivos,
        poke_interval=20,
        timeout=30 * 60,
        mode="reschedule",  # libera o slot do executor enquanto espera
    )

    bronze = spark_task("bronze_ingestao", "spark_jobs/ingestao.py")
    silver = spark_task("silver_transformacao", "spark_jobs/transformacao.py")
    gate_silver = spark_task(
        "quality_gate_silver", "quality/checks.py",
        ["--camada", "silver",
         "--taxa-quarentena-max", "{{ params.taxa_quarentena_max }}",
         "--volume-minimo", "{{ params.volume_minimo_silver }}"],
        retries=0,  # reprovação de qualidade é determinística: retry não resolve
    )
    gold = spark_task("gold_agregacao", "spark_jobs/agregacao.py")
    gate_gold = spark_task("quality_gate_gold", "quality/checks.py", ["--camada", "gold"], retries=0)

    branch = BranchPythonOperator(task_id="verificar_quarentena", python_callable=decidir_quarentena)
    alerta = PythonOperator(task_id="alertar_quarentena", python_callable=alertar_quarentena)
    sem_pendencias = EmptyOperator(task_id="sem_pendencias")

    notificar = PythonOperator(
        task_id="notificar_sucesso",
        python_callable=notificar_sucesso,
        trigger_rule="none_failed_min_one_success",
    )

    aguardar >> bronze >> silver >> gate_silver >> gold >> gate_gold >> branch
    branch >> [alerta, sem_pendencias] >> notificar
