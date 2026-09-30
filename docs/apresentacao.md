# Roteiro da Apresentação — 20 min + 5 min de perguntas

> Regra do professor: **todos falam pelo menos 2 minutos**, demo ao vivo obrigatória, cronômetro visível.
> O roteiro abaixo está dividido para **5 pessoas (P1–P5)**. Com 4, P5 assume o bloco de P4 e o fechamento; com 3, junte P2+P3 e P4+P5.

## Antes da aula (checklist)

- [ ] **Codespaces (recomendado):** abrir o Codespace de 4 cores **15 min antes** da aula, para dar tempo de subir e rodar uma vez. Se for Docker local: na véspera, `docker compose build` (~5 min, precisa de internet)
- [ ] Conferir se ainda há horas grátis de Codespaces na conta de quem vai apresentar (github.com/settings/billing)
- [ ] Rodar o pipeline completo **2 vezes** do zero (`docker compose down -v` + limpar `data/…`) e cronometrar
- [ ] Gravar o vídeo do plano B (ver abaixo) e tirar os prints
- [ ] Docker Desktop com **≥ 6 GB** de memória reservada
- [ ] Deixar abas prontas: Airflow (porta 8080 — no Codespaces, pela aba PORTS), Spark UI (localhost:4040), terminal na pasta do projeto, `docs/arquitetura.md` renderizado no GitHub
- [ ] Fechar Slack/Teams/notificações

## No dia — 2 minutos antes de começar

```bash
docker compose down -v && rm -rf data/bronze data/silver data/gold data/quarentena data/quality data/notificacoes logs
docker compose up -d          # imagem já construída → sobe em ~30–60 s
# (no Codespaces, use: AIRFLOW_UID=$(id -u) docker compose up -d)
```

A DAG dispara sozinha. Enquanto P1 e P2 falam (6 min), o pipeline já está rodando — na hora da demo ele terá terminado ou estará no fim, e dá para mostrar as tasks verdes e disparar uma **segunda execução ao vivo**.

---

## Bloco a bloco

### 0–3 min · P1 — Problema de negócio e dados
- A DataFlow precisa entregar à ShopBrasil um dashboard executivo confiável.
- Três parceiros, três formatos (arquivos oficiais de `datasets/aula_03`): **CSV legado** (Latin-1, `;`, colunas em português), **JSON de API** paginado e **Parquet**.
- Mais o cadastro de **clientes** (Parquet, 500 mil) e as **categorias** de produto (JSON aninhado), de `datasets/aula_02`.
- **116.871 vendas** de 2023 no total. Pergunta de negócio: *quanto faturamos, onde, quando, como o cliente paga e em quais categorias?*

### 3–6 min · P2 — Arquitetura
- Mostrar o diagrama de `docs/arquitetura.md`.
- Medallion: Bronze guarda **como chegou** + metadados (`_source`, `_source_file`, `_ingestion_ts`, `_batch_id`); Silver **limpa e unifica**; Gold responde ao negócio.
- Stack: Airflow 2.8 (LocalExecutor + Postgres) → `SparkSubmitOperator` → PySpark 3.5 em modo local; tudo em um `docker compose up`.
- Decisão consciente: Spark local para caber em 8 GB e reduzir risco na demo; para escalar, basta trocar a conexão por um cluster standalone, sem mudar os jobs.

### 6–15 min · Demo ao vivo (P3 conduz, P4 comenta)
1. **Terminal:** `docker compose ps` → 3 serviços *healthy* (+ init concluído).
2. **Airflow → Graph** da `shopbrasil_pipeline_vendas`: percorrer as 10 tasks (sensor, 5 spark-submit, branch, notificação).
3. **Log do `bronze_ingestao`**: logs JSON com registros por fonte (6.871 / 30.000 / 80.000 vendas + 500.000 clientes + categorias).
4. **Log do `silver_transformacao`**: `entrada 116.871 → silver 116.871 + quarentena 0, conservacao_ok: true`. No Graph, mostrar que o branch foi para `sem_pendencias`.
5. **Trigger DAG** de novo ao vivo e abrir a **Spark UI (4040)** enquanto um job roda → jobs/stages.
6. **Idempotência:** a segunda execução gera exatamente os mesmos números (overwrite).
7. **Mostrar o gate bloqueando:** *Trigger DAG w/ config* → `volume_minimo_silver: 200000` → `quality_gate_silver` fica **vermelho** na hora (gates não têm retry), `gold_agregacao` **não roda**, e o alerta aparece em `data/notificacoes/…_falha.json`.
   *(Se o tempo estiver curto, pule este passo e mostre o print.)*

### 15–18 min · P4 — Qualidade e resultados na Gold
```bash
docker compose exec airflow-scheduler python scripts/mostrar_resultados.py --secao gates
docker compose exec airflow-scheduler python scripts/mostrar_resultados.py --secao quarentena
docker compose exec airflow-scheduler python scripts/mostrar_resultados.py --secao gold
```
- 5 dimensões: completude, domínio, consistência, integridade (cliente existe), unicidade — e todas as 116.871 vendas passaram.
- Quarentena vazia é um **resultado**, não uma falha: os dados da Aula 3 vêm limpos. Mostrar `pytest -q tests/` provando que a quarentena pega nulo, status inválido, UF inválida, total negativo, data fora de 2023, total inconsistente e duplicata.
- Gate Gold **reconcilia**: faturamento por estado = por mês = por pagamento = por categoria×segmento = Silver.
- Números para citar: faturamento **R$ 2,76 bi** em 105.054 pedidos, SP = **22%**, pico em **junho** (R$ 404 mi), queda forte em nov/dez, cartão de crédito = **35%**.

### 18–20 min · P5 — Dificuldades, aprendizados e próximos passos
- **Achado:** a coluna `partner_source` dos arquivos não bate com o arquivo real → linhagem confiável vem do `_source` do pipeline.
- **Dificuldades:** encoding Latin-1 do CSV legado, JSON com envelope de API, tipos diferentes de data entre parceiros, parceiro B com JSON aninhado e metadados de paginação.
- **Próximos passos:** carga incremental por `_data_ref`, cluster Spark standalone, Delta Lake/Iceberg para *time travel*, alertas reais no Slack, dashboard (Superset/Metabase) em cima da Gold.

---

## Plano B (se o Docker falhar no dia)

1. **Vídeo de 3 min** gravado na véspera: `docker compose up` → DAG verde → `mostrar_resultados.py`. Deixar o arquivo **local**, não depender de internet.
2. **Prints** (pasta `docs/img/`): Graph da DAG verde, log da Silver com a conservação, Spark UI, saída do `mostrar_resultados.py`, gate vermelho com `volume_minimo_silver=200000`.
3. **Plano C:** rodar os jobs sem Docker (seção "Rodar sem Docker" do README) — leva ~2 min.

## Perguntas prováveis (e respostas curtas)

| Pergunta | Resposta |
|---|---|
| Por que Spark local e não cluster? | Requisito de 8 GB / 4 cores e demo ao vivo. O código não muda para um cluster: só a conexão `spark_local` → `spark://master:7077`. |
| O que acontece se rodar duas vezes? | Nada duplica: todas as camadas usam overwrite e `max_active_runs=1`. Só o histórico de qualidade acumula, de propósito. |
| Como provar que nenhum registro se perdeu? | Check de conservação: Bronze = Silver + Quarentena, verificado em toda execução pelo gate. |
| E se um parceiro mudar o layout do arquivo? | O contrato de colunas na Bronze lança `SchemaDriftError` e a DAG para antes da Silver. |
| Por que não Great Expectations? | Proibido pelo enunciado; as regras são expressões PySpark em `quality/checks.py`, testadas em `tests/test_checks.py`. |
| Um registro com 2 problemas conta 2 vezes? | Na lista `_motivos` sim (fica registrado tudo); na contagem da quarentena ele é 1 linha. Nulo só conta em completude. |
| Qual duplicata vocês mantêm? | A primeira por `_ingestion_ts`, `_source`, `_source_file`, `order_ts`, entre os registros válidos. |
| Como ligaram produto à categoria? | O JSON de categorias não tem product_id; usamos a regra do lab da Aula 2 (faixas de 500 produtos por categoria). |
| LGPD? | A Silver de clientes não leva nome, e-mail nem telefone: minimização de dados. |
| Por que a distribuição por segmento é 50/30/15/5? | Reflete a base de clientes gerada pelo professor (Bronze 50%, Prata 30%, Ouro 15%, Platina 5%). |
| A quarentena está vazia, então ela funciona? | Sim: os testes unitários injetam registros ruins e provam cada regra. Os dados da Aula 3 simplesmente passaram em todas. |
