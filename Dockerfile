# =============================================================================
# Imagem única: Airflow 2.8.4 + Java 17 + PySpark 3.5 (Spark em modo local)
# =============================================================================
# O Airflow orquestra e o próprio worker executa `spark-submit --master local[2]`.
# Escolha deliberada para rodar com folga em 8 GB RAM / 4 cores e reduzir
# pontos de falha na demo (ver docs/arquitetura.md → "Decisões").
FROM apache/airflow:2.8.4-python3.11

USER root
# Java é pré-requisito do Spark; procps fornece `ps`, usado pelos scripts do Spark
RUN mkdir -p /usr/share/man/man1 \
 && apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && apt-get clean && rm -rf /var/lib/apt/lists/* \
 # JAVA_HOME independente de arquitetura (amd64 / arm64 — Macs M1/M2/M3)
 && ln -sfn "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java-home
ENV JAVA_HOME=/opt/java-home

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.4/constraints-3.11.txt"

# Código do projeto é montado via volume (docker-compose); estas variáveis
# fazem `spark_jobs` e `quality` serem importáveis e silenciam logs do Spark.
ENV PYTHONPATH=/opt/airflow \
    PROJECT_DIR=/opt/airflow \
    DATA_DIR=/opt/airflow/data \
    SPARK_CONF_DIR=/opt/airflow/conf
