// Gera docs/slides/apresentacao_projeto_final.pptx
// Números vêm da execução do pipeline sobre datasets/aula_03 (ver README).
const pptxgen = require("pptxgenjs");
const pres = new pptxgen();
pres.layout = "LAYOUT_16x9"; // 10 x 5.625 in
pres.title = "Pipeline ShopBrasil — Projeto Final Big Data Processing";

const C = {
  ink: "14213D", ink2: "22335C", white: "FFFFFF", paper: "F4F6F9", text: "1F2937", muted: "5B6472",
  amber: "FCA311", bronze: "B87333", silver: "8C96A3", gold: "C9A227", green: "2E7D5B", red: "B83A3A",
};
const HEAD = "Cambria", BODY = "Calibri";

function t(slide, text, o) { slide.addText(text, Object.assign({ isTextBox: true, fontFace: BODY, color: C.text, margin: 0 }, o)); }
function title(slide, text, sub) {
  t(slide, text, { x: 0.5, y: 0.35, w: 8.3, h: 0.6, fontFace: HEAD, fontSize: 30, bold: true, color: C.ink });
  if (sub) t(slide, sub, { x: 0.5, y: 0.95, w: 8.6, h: 0.35, fontSize: 14, color: C.muted });
}
function chip(slide, who, dark) {
  slide.addShape(pres.shapes.ROUNDED_RECTANGLE, { x: 8.95, y: 0.38, w: 0.6, h: 0.32, rectRadius: 0.16,
    fill: { color: dark ? C.amber : C.ink } });
  t(slide, who, { x: 8.95, y: 0.38, w: 0.6, h: 0.32, fontSize: 12, bold: true, align: "center", valign: "middle",
    color: dark ? C.ink : C.white });
}
function card(slide, x, y, w, h, fill) {
  slide.addShape(pres.shapes.ROUNDED_RECTANGLE, { x, y, w, h, rectRadius: 0.08, fill: { color: fill || C.paper },
    shadow: { type: "outer", color: "000000", opacity: 0.12, blur: 4, offset: 1.5, angle: 90 } });
}
function medal(slide, x, y, color, label) {
  slide.addShape(pres.shapes.OVAL, { x, y, w: 0.5, h: 0.5, fill: { color } });
  t(slide, label, { x, y, w: 0.5, h: 0.5, fontSize: 13, bold: true, color: C.white, align: "center", valign: "middle" });
}

// 1 — Capa ------------------------------------------------------------------
{
  const s = pres.addSlide(); s.background = { color: C.ink };
  [C.bronze, C.silver, C.gold].forEach((c, i) => s.addShape(pres.shapes.OVAL, { x: 0.5 + i * 0.42, y: 0.7, w: 0.32, h: 0.32, fill: { color: c } }));
  t(s, "Pipeline ShopBrasil", { x: 0.5, y: 1.35, w: 9, h: 0.9, fontFace: HEAD, fontSize: 44, bold: true, color: C.white });
  t(s, "Vendas de 3 parceiros → Bronze · Silver · Gold, com qualidade de dados e orquestração", { x: 0.5, y: 2.25, w: 8.5, h: 0.5, fontSize: 18, color: "CADCFC" });
  t(s, "PySpark 3.5  ·  Airflow 2.8  ·  Docker Compose  ·  Parquet", { x: 0.5, y: 2.85, w: 8.5, h: 0.4, fontSize: 14, italic: true, color: C.amber });
  t(s, "Projeto Final — Big Data Processing · MBA em Engenharia de Dados · Mackenzie\nProf. Alexandre Tavares · Opção A — DataFlow Analytics", { x: 0.5, y: 4.05, w: 6.5, h: 0.7, fontSize: 12, color: "CADCFC" });
  t(s, "Integrantes: [nomes do grupo]", { x: 0.5, y: 4.8, w: 6.5, h: 0.35, fontSize: 12, bold: true, color: C.white });
  s.addNotes("P1 abre: apresentar o grupo e o tema. Enquanto isso o docker compose up já está rodando (subido 2 minutos antes).");
}

// 2 — Problema de negócio -------------------------------------------------
{
  const s = pres.addSlide(); s.background = { color: C.white }; chip(s, "P1");
  title(s, "O problema: três parceiros, três formatos", "A ShopBrasil quer um dashboard executivo confiável — hoje cada parceiro manda os dados de um jeito");
  const cards = [
    ["Parceiro A", "CSV legado", "ISO-8859-1, separador ';', colunas em português", "6.871", "3 arquivos"],
    ["Parceiro B", "JSON de API", "Paginado, registros dentro de data[] + metadados", "30.000", "3 arquivos"],
    ["Parceiro C", "Parquet", "Data lake moderno, tipos já definidos", "80.000", "1 arquivo"],
  ];
  cards.forEach((c, i) => {
    const x = 0.5 + i * 2.35;
    card(s, x, 1.55, 2.15, 3.3);
    t(s, c[0], { x: x + 0.2, y: 1.7, w: 1.8, h: 0.3, fontSize: 12, color: C.muted, bold: true });
    t(s, c[1], { x: x + 0.2, y: 2.0, w: 1.8, h: 0.4, fontFace: HEAD, fontSize: 20, bold: true, color: C.ink });
    t(s, c[2], { x: x + 0.2, y: 2.45, w: 1.8, h: 0.8, fontSize: 12, color: C.text, valign: "top" });
    t(s, c[3], { x: x + 0.2, y: 3.5, w: 1.8, h: 0.6, fontFace: HEAD, fontSize: 30, bold: true, color: C.bronze });
    t(s, "vendas · " + c[4], { x: x + 0.2, y: 4.1, w: 1.8, h: 0.3, fontSize: 11, color: C.muted });
  });
  s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x: 7.65, y: 1.55, w: 1.9, h: 3.3, rectRadius: 0.08, fill: { color: C.ink } });
  t(s, "116.871", { x: 7.75, y: 2.2, w: 1.7, h: 0.7, fontFace: HEAD, fontSize: 30, bold: true, color: C.amber, align: "center" });
  t(s, "vendas de 2023\nem 7 arquivos", { x: 7.75, y: 2.9, w: 1.7, h: 0.6, fontSize: 13, color: C.white, align: "center" });
  t(s, "+ 500 mil clientes (Parquet)\n+ 10 categorias (JSON)", { x: 7.75, y: 3.6, w: 1.7, h: 0.5, fontSize: 11, color: C.white, align: "center" });
  t(s, "Fonte: datasets/aula_03 e aula_02 (repo da disciplina)", { x: 7.75, y: 4.2, w: 1.7, h: 0.5, fontSize: 9, color: "CADCFC", align: "center" });
  s.addNotes("Pergunta de negócio: quanto faturamos, onde, quando, como o cliente paga e em quais categorias? Vendas = arquivos oficiais da Aula 3; clientes e categorias = Aula 2. Nenhum arquivo alterado. São sintéticos (gerados pelo professor).");
}

// 3 — Arquitetura Medallion ------------------------------------------------
{
  const s = pres.addSlide(); s.background = { color: C.white }; chip(s, "P2");
  title(s, "Arquitetura Medallion", "Cada camada tem um contrato claro — e nenhum registro se perde no caminho");
  const cols = [
    ["RAW", C.muted, "data/raw", ["Vendas: 3 CSV · 3 JSON · 1 Parquet", "Clientes (Parquet)", "Categorias (JSON)", "Como a fonte enviou"]],
    ["B", C.bronze, "Bronze", ["Schema original, sem regra", "+ _source, _source_file,", "  _ingestion_ts, _batch_id", "Contrato de colunas"]],
    ["S", C.silver, "Silver", ["Schema unificado e tipado", "Deduplicado por order_id", "Particionado por ano_mes", "Dimensões clientes (sem PII) e categorias"]],
    ["G", C.gold, "Gold", ["faturamento_por_estado", "analise_mensal", "faturamento_por_pagamento", "faturamento_categoria_ segmento"]],
  ];
  cols.forEach((c, i) => {
    const x = 0.5 + i * 2.3;
    card(s, x, 1.55, 2.0, 2.75);
    medal(s, x + 0.2, 1.72, c[1], c[0] === "RAW" ? "R" : c[0]);
    t(s, c[2], { x: x + 0.8, y: 1.72, w: 1.1, h: 0.5, fontFace: HEAD, fontSize: 18, bold: true, color: C.ink, valign: "middle" });
    t(s, c[3].map((l, k) => ({ text: l, options: { breakLine: k < c[3].length - 1 } })),
      { x: x + 0.15, y: 2.4, w: 1.8, h: 1.8, fontSize: 10.5, valign: "top", paraSpaceAfter: 4 });
    if (i < 3) s.addShape(pres.shapes.RIGHT_TRIANGLE, { x: x + 2.05, y: 2.8, w: 0.2, h: 0.25, rotate: 90, fill: { color: C.amber }, line: { color: C.amber } });
  });
  card(s, 2.8, 4.5, 4.3, 0.7, "FDF1DC");
  t(s, [{ text: "Quarentena  ", options: { bold: true, color: C.bronze } },
        { text: "registros reprovados saem da Silver com a lista de motivos (_motivos)" }],
    { x: 3.0, y: 4.5, w: 4.0, h: 0.7, fontSize: 12, valign: "middle" });
  t(s, "Bronze = Silver + Quarentena", { x: 7.3, y: 4.5, w: 2.3, h: 0.7, fontSize: 13, bold: true, italic: true, color: C.green, valign: "middle" });
  s.addNotes("Bronze guarda como chegou (o parceiro A continua com colunas em português). Silver limpa e unifica. Gold responde ao negócio. Metadados de linhagem ligam cada linha ao arquivo e à execução do Airflow.");
}

// 4 — Orquestração ---------------------------------------------------------
{
  const s = pres.addSlide(); s.background = { color: C.white }; chip(s, "P2");
  title(s, "Orquestração no Airflow", "DAG shopbrasil_pipeline_vendas · 10 tasks · retries · callback de falha · parâmetros na UI");
  const steps = [
    ["Sensor", "aguardar_arquivos_parceiros", C.muted],
    ["Spark", "bronze_ingestao", C.bronze],
    ["Spark", "silver_transformacao", C.silver],
    ["Gate", "quality_gate_silver", C.amber],
    ["Spark", "gold_agregacao", C.gold],
    ["Gate", "quality_gate_gold", C.amber],
  ];
  steps.forEach((st, i) => {
    const col = i % 3, row = Math.floor(i / 3);
    const x = 0.5 + col * 3.05, y = 1.6 + row * 1.25;
    card(s, x, y, 2.75, 0.95);
    s.addShape(pres.shapes.OVAL, { x: x + 0.15, y: y + 0.22, w: 0.5, h: 0.5, fill: { color: st[2] } });
    t(s, String(i + 1), { x: x + 0.15, y: y + 0.22, w: 0.5, h: 0.5, fontSize: 14, bold: true, color: C.white, align: "center", valign: "middle" });
    t(s, st[0], { x: x + 0.8, y: y + 0.13, w: 1.85, h: 0.3, fontSize: 11, color: C.muted, bold: true });
    t(s, st[1], { x: x + 0.75, y: y + 0.43, w: 1.95, h: 0.4, fontSize: 10.5, color: C.ink, bold: true });
  });
  card(s, 0.5, 4.2, 5.8, 0.95, "EAF3EE");
  t(s, [{ text: "Branch  ", options: { bold: true, color: C.green } },
        { text: "verificar_quarentena → alertar_quarentena | sem_pendencias → notificar_sucesso (resumo executivo)" }],
    { x: 0.7, y: 4.2, w: 5.5, h: 0.95, fontSize: 12, valign: "middle" });
  card(s, 6.55, 4.2, 2.95, 0.95, "FBEAEA");
  t(s, [{ text: "Falhou?  ", options: { bold: true, color: C.red } },
        { text: "on_failure_callback grava o alerta em data/notificacoes/" }],
    { x: 6.75, y: 4.2, w: 2.65, h: 0.95, fontSize: 12, valign: "middle" });
  s.addNotes("Spark roda via SparkSubmitOperator em modo local[2], dentro do container do Airflow (LocalExecutor + Postgres). Gates não têm retry: reprovação de qualidade é determinística. max_active_runs=1 porque as camadas são sobrescritas.");
}

// 5 — Decisões --------------------------------------------------------------
{
  const s = pres.addSlide(); s.background = { color: C.white }; chip(s, "P2");
  title(s, "Decisões de arquitetura", "Escolhas conscientes para caber em 8 GB / 4 cores e não falhar ao vivo");
  const d = [
    ["Spark em modo local", "Uma imagem só (Airflow + Java 17 + PySpark). Para escalar, troca-se a conexão spark_local por um cluster — os jobs não mudam."],
    ["Overwrite idempotente", "Rodar a DAG duas vezes gera os mesmos números. Nada duplica; só o histórico de qualidade acumula, de propósito."],
    ["Contrato na Bronze", "Se um parceiro mudar o layout do arquivo, SchemaDriftError para o pipeline antes de contaminar a Silver."],
    ["Um comando sobe tudo", "docker compose up: Postgres, init (migra banco, cria admin), webserver e scheduler. Também roda no GitHub Codespaces."],
  ];
  d.forEach((it, i) => {
    const x = 0.5 + (i % 2) * 4.55, y = 1.55 + Math.floor(i / 2) * 1.8;
    card(s, x, y, 4.35, 1.6);
    medal(s, x + 0.2, y + 0.2, [C.bronze, C.silver, C.gold, C.ink][i], String(i + 1));
    t(s, it[0], { x: x + 0.85, y: y + 0.2, w: 3.3, h: 0.5, fontFace: HEAD, fontSize: 17, bold: true, color: C.ink, valign: "middle" });
    t(s, it[1], { x: x + 0.85, y: y + 0.72, w: 3.3, h: 0.8, fontSize: 12, valign: "top" });
  });
  s.addNotes("Se perguntarem por que não cluster: requisito de 8 GB e risco da demo ao vivo. É só trocar a conexão.");
}

// 6 — Demo ------------------------------------------------------------------
{
  const s = pres.addSlide(); s.background = { color: C.ink }; chip(s, "P3", true);
  t(s, "Demo ao vivo", { x: 0.5, y: 0.35, w: 8, h: 0.8, fontFace: HEAD, fontSize: 40, bold: true, color: C.white });
  t(s, "docker compose up  →  pipeline end-to-end", { x: 0.5, y: 1.1, w: 8, h: 0.4, fontSize: 16, italic: true, color: C.amber });
  const passos = [
    "docker compose ps — serviços healthy",
    "Airflow → Graph da DAG: 10 tasks verdes",
    "Log da Bronze: 6.871 · 30.000 · 80.000 por parceiro",
    "Log da Silver: 116.871 → 116.871 + 0, conservação OK",
    "Trigger de novo + Spark UI (porta 4040) — mesmos números",
    "Trigger w/ config volume_minimo_silver = 200000 → gate vermelho, Gold não roda",
  ];
  passos.forEach((p, i) => {
    const y = 1.75 + i * 0.58;
    s.addShape(pres.shapes.OVAL, { x: 0.5, y, w: 0.42, h: 0.42, fill: { color: C.amber } });
    t(s, String(i + 1), { x: 0.5, y, w: 0.42, h: 0.42, fontSize: 13, bold: true, color: C.ink, align: "center", valign: "middle" });
    t(s, p, { x: 1.1, y, w: 8.3, h: 0.42, fontSize: 15, color: C.white, valign: "middle" });
  });
  s.addNotes("P3 conduz, P4 comenta. Plano B: vídeo gravado na véspera + prints em docs/img. Se o tempo apertar, pular o passo 6 e mostrar o print.");
}

// 7 — Qualidade -------------------------------------------------------------
{
  const s = pres.addSlide(); s.background = { color: C.white }; chip(s, "P4");
  title(s, "Qualidade de dados", "Sem Great Expectations/Soda: regras PySpark em quality/checks.py, testadas com pytest");
  const dims = [
    ["Completude", "11 campos obrigatórios não nulos e não vazios"],
    ["Validade de domínio", "status, forma de pagamento, 27 UFs, valores > 0, data em 2023"],
    ["Consistência", "total_amount = quantity × unit_price (± R$ 0,01)"],
    ["Integridade", "customer_id existe no cadastro de clientes (broadcast join)"],
    ["Unicidade", "1ª ocorrência de order_id fica; as demais → quarentena"],
  ];
  dims.forEach((d, i) => {
    const y = 1.5 + i * 0.68;
    card(s, 0.5, y, 4.6, 0.58);
    t(s, d[0], { x: 0.7, y, w: 1.7, h: 0.58, fontSize: 13, bold: true, color: C.ink, valign: "middle" });
    t(s, d[1], { x: 2.4, y, w: 2.6, h: 0.58, fontSize: 11, valign: "middle" });
  });
  card(s, 5.4, 1.5, 4.1, 3.3, C.ink);
  t(s, "Quality gates bloqueantes", { x: 5.6, y: 1.65, w: 3.8, h: 0.4, fontFace: HEAD, fontSize: 17, bold: true, color: C.amber });
  t(s, [
    { text: "Gate Silver (antes da Gold)", options: { bold: true, breakLine: true } },
    { text: "conservação · completude · unicidade · domínio · integridade · quarentena ≤ 10% · volume ≥ 50 mil", options: { breakLine: true } },
    { text: " ", options: { breakLine: true, fontSize: 6 } },
    { text: "Gate Gold (antes de notificar)", options: { bold: true, breakLine: true } },
    { text: "Σ faturamento por estado = mês = pagamento = categoria×segmento = Silver · sem categoria/segmento desconhecido · nenhum valor negativo" },
  ], { x: 5.6, y: 2.25, w: 3.75, h: 2.4, fontSize: 13.5, color: C.white, valign: "top", paraSpaceAfter: 4 });
  s.addNotes("Nulo só reprova em completude — cada problema aparece uma vez no relatório. Histórico de cada gate vai para data/quality/historico (Parquet append-only) para monitoramento.");
}

// 8 — Resultado da qualidade ------------------------------------------------
{
  const s = pres.addSlide(); s.background = { color: C.white }; chip(s, "P4");
  title(s, "O que a qualidade encontrou");
  const stats = [["116.871", "vendas na Bronze", C.bronze], ["116.871", "vendas na Silver", C.silver], ["0", "em quarentena", C.green], ["15/15", "checks dos gates", C.ink]];
  stats.forEach((st, i) => {
    const x = 0.5 + i * 2.3;
    t(s, st[0], { x, y: 1.2, w: 2.1, h: 0.8, fontFace: HEAD, fontSize: 36, bold: true, color: st[2] });
    t(s, st[1], { x, y: 1.95, w: 2.1, h: 0.3, fontSize: 12, color: C.muted });
  });
  card(s, 0.5, 2.55, 4.4, 2.1);
  t(s, "Quarentena vazia é resultado, não falha", { x: 0.7, y: 2.7, w: 4.0, h: 0.4, fontSize: 15, bold: true, color: C.ink });
  t(s, "Os arquivos não têm nulos, duplicatas, valores fora do domínio, totais inconsistentes nem clientes fora do cadastro. Os testes unitários injetam registros ruins e provam que cada regra manda para a quarentena.",
    { x: 0.7, y: 3.15, w: 4.0, h: 1.4, fontSize: 13, valign: "top" });
  card(s, 5.1, 2.55, 4.4, 2.1, "FDF1DC");
  t(s, "Achado: linhagem declarada não é confiável", { x: 5.3, y: 2.7, w: 4.0, h: 0.4, fontSize: 15, bold: true, color: C.bronze });
  t(s, [{ text: "71%", options: { fontFace: HEAD, fontSize: 30, bold: true, color: C.bronze, breakLine: true } },
        { text: "das linhas trazem um partner_source diferente do parceiro que enviou o arquivo. Por isso a linhagem usa o _source definido pelo pipeline." }],
    { x: 5.3, y: 3.1, w: 4.0, h: 1.5, fontSize: 12.5, valign: "top" });
  s.addNotes("Comando para mostrar: docker compose exec airflow-scheduler python scripts/mostrar_resultados.py --secao gates. 71% = 83.425 de 116.871 linhas com partner_source ≠ arquivo de origem.");
}

// 9 — Gold: mensal ----------------------------------------------------------
{
  const s = pres.addSlide(); s.background = { color: C.white }; chip(s, "P4");
  title(s, "Gold · analise_mensal", "Faturamento de pedidos não cancelados, em R$ milhões — pico em junho, queda forte em nov/dez");
  const meses = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"];
  const vals = [39.3, 70.2, 132.5, 173.8, 235.5, 403.9, 340.7, 390.6, 397.3, 308.4, 182.7, 85.4];
  s.addChart(pres.charts.BAR, [{ name: "Faturamento (R$ mi)", labels: meses, values: vals }], {
    x: 0.5, y: 1.4, w: 6.4, h: 3.8, barDir: "col", chartColors: [C.gold],
    showValue: true, dataLabelPosition: "outEnd", dataLabelFontSize: 9, dataLabelColor: C.text, dataLabelFormatCode: "0",
    catAxisLabelColor: C.muted, valAxisLabelColor: C.muted, valGridLine: { color: "E5E7EB", size: 0.5 }, catGridLine: { style: "none" },
    showLegend: false, valAxisLabelFormatCode: "0",
  });
  const kp = [["R$ 2,76 bi", "faturamento 2023"], ["105.054", "pedidos faturados"], ["R$ 26,3 mil", "ticket médio"]];
  kp.forEach((k, i) => {
    card(s, 7.2, 1.4 + i * 1.3, 2.3, 1.1);
    t(s, k[0], { x: 7.35, y: 1.5 + i * 1.3, w: 2.1, h: 0.55, fontFace: HEAD, fontSize: 22, bold: true, color: C.ink });
    t(s, k[1], { x: 7.35, y: 2.02 + i * 1.3, w: 2.1, h: 0.3, fontSize: 11, color: C.muted });
  });
  s.addNotes("Crescimento MoM calculado com lag() e acumulado com window. Junho: R$ 403,9 mi (+71,6% sobre maio). Ticket médio alto porque os dados são sintéticos do gerador do professor.");
}

// 10 — Gold: estado + pagamento ---------------------------------------------
{
  const s = pres.addSlide(); s.background = { color: C.white }; chip(s, "P4");
  title(s, "Gold · por estado e por forma de pagamento", "R$ milhões — SP sozinho responde por 22% do faturamento");
  const ufs = ["ES", "CE", "PE", "SC", "BA", "PR", "RS", "MG", "RJ", "SP"];
  const fat = [82.1, 96.7, 105.6, 121.5, 169.9, 194.3, 200.7, 291.7, 367.5, 608.4];
  s.addChart(pres.charts.BAR, [{ name: "Faturamento (R$ mi)", labels: ufs, values: fat }], {
    x: 0.4, y: 1.35, w: 5.0, h: 3.9, barDir: "bar", chartColors: [C.ink],
    showValue: true, dataLabelPosition: "outEnd", dataLabelFontSize: 9, dataLabelColor: C.text, dataLabelFormatCode: "0",
    catAxisLabelColor: C.text, valAxisHidden: true, valGridLine: { style: "none" }, catGridLine: { style: "none" }, showLegend: false,
    showTitle: true, title: "Top 10 UFs", titleFontSize: 12, titleColor: C.muted,
  });
  s.addChart(pres.charts.DOUGHNUT, [{ name: "Pagamento", labels: ["Cartão de crédito", "Pix", "Cartão de débito", "Boleto"], values: [967.3, 828.8, 553.0, 411.2] }], {
    x: 5.6, y: 1.35, w: 3.9, h: 3.9, holeSize: 55, chartColors: [C.ink, C.gold, C.bronze, C.silver],
    showPercent: true, showValue: false, dataLabelColor: C.white, dataLabelFontSize: 10,
    showLegend: true, legendPos: "b", legendFontSize: 10, legendColor: C.text,
    showTitle: true, title: "Forma de pagamento", titleFontSize: 12, titleColor: C.muted,
  });
  s.addNotes("Ranking por dense_rank(). Participação %, taxa de cancelamento (~10% em todos os estados) e ticket médio também estão na tabela. Cartão de crédito 35%, Pix 30%, débito 20%, boleto 15%.");
}

// 10b — Gold: categoria × segmento -----------------------------------------
{
  const s = pres.addSlide(); s.background = { color: C.white }; chip(s, "P4");
  title(s, "Gold · categoria × segmento de cliente", "R$ milhões — vendas ligadas às dimensões da Aula 2 com broadcast join");
  s.addChart(pres.charts.BAR, [{ name: "Faturamento (R$ mi)", labels: ["Platina", "Ouro", "Prata", "Bronze"], values: [141.2, 412.3, 831.5, 1375.3] }], {
    x: 0.4, y: 1.35, w: 4.6, h: 3.2, barDir: "bar", chartColors: [C.gold],
    showValue: true, dataLabelPosition: "outEnd", dataLabelFontSize: 10, dataLabelColor: C.text, dataLabelFormatCode: "0",
    catAxisLabelColor: C.text, valAxisHidden: true, valGridLine: { style: "none" }, catGridLine: { style: "none" }, showLegend: false,
    showTitle: true, valAxisMinVal: 0, title: "Por segmento de cliente", titleFontSize: 12, titleColor: C.muted,
  });
  const cats = ["Alimentos", "Casa e Decoração", "Automotivo", "Livros", "Esportes", "Moda", "Informática", "Saúde e Beleza", "Brinquedos", "Eletrônicos"];
  const vc = [270.9, 273.4, 273.9, 274.8, 275.6, 275.7, 278.4, 278.6, 278.8, 280.2];
  s.addChart(pres.charts.BAR, [{ name: "Faturamento (R$ mi)", labels: cats, values: vc }], {
    x: 5.1, y: 1.35, w: 4.5, h: 3.2, barDir: "bar", chartColors: [C.ink],
    showValue: true, dataLabelPosition: "outEnd", dataLabelFontSize: 9, dataLabelColor: C.text, dataLabelFormatCode: "0",
    catAxisLabelColor: C.text, catAxisLabelFontSize: 9, valAxisHidden: true, valAxisMinVal: 0, valGridLine: { style: "none" }, catGridLine: { style: "none" }, showLegend: false,
    showTitle: true, title: "Por categoria de produto", titleFontSize: 12, titleColor: C.muted,
  });
  card(s, 0.5, 4.7, 9.0, 0.6, "FDF1DC");
  t(s, "Leitura honesta: segmentos seguem a base de clientes (50/30/15/5%) e categorias ficam quase iguais — é o perfil dos dados sintéticos do curso.",
    { x: 0.7, y: 4.7, w: 8.6, h: 0.6, fontSize: 11.5, valign: "middle", color: C.text });
  s.addNotes("Produto → categoria pela regra do lab da Aula 2 (500 produtos por categoria), porque o JSON de categorias não tem product_id. 40 linhas na tabela (10 categorias × 4 segmentos). Gate Gold confirma que nenhuma venda ficou sem categoria ou sem segmento.");
}

// 11 — Aprendizados ---------------------------------------------------------
{
  const s = pres.addSlide(); s.background = { color: C.white }; chip(s, "P5");
  title(s, "O que aprendemos");
  const blocos = [
    ["Dificuldades", C.red, ["Encoding Latin-1 e ';' no CSV legado", "JSON com envelope de API (data[] + paginação)", "Datas com e sem hora entre parceiros", "Permissões de arquivo entre host e container"]],
    ["Aprendizados", C.green, ["Bronze fiel à origem facilita auditoria", "Gate bloqueante evita publicar número errado", "Idempotência simplifica reprocessar", "Linhagem declarada ≠ linhagem real"]],
    ["Próximos passos", C.ink, ["Carga incremental por _data_ref", "Cluster Spark standalone", "Delta Lake / Iceberg (time travel)", "Alertas no Slack + dashboard na Gold"]],
  ];
  blocos.forEach((b, i) => {
    const x = 0.5 + i * 3.05;
    card(s, x, 1.25, 2.85, 3.5);
    s.addShape(pres.shapes.OVAL, { x: x + 0.2, y: 1.45, w: 0.28, h: 0.28, fill: { color: b[1] } });
    t(s, b[0], { x: x + 0.6, y: 1.38, w: 2.1, h: 0.42, fontFace: HEAD, fontSize: 17, bold: true, color: C.ink, valign: "middle" });
    t(s, b[2].map((l, k) => ({ text: l, options: { bullet: true, breakLine: k < b[2].length - 1 } })),
      { x: x + 0.2, y: 2.0, w: 2.5, h: 2.9, fontSize: 12.5, valign: "top", paraSpaceAfter: 8 });
  });
  s.addNotes("P5 fecha. Cada dificuldade tem uma história curta — escolher 1 ou 2 para contar.");
}

// 12 — Encerramento ---------------------------------------------------------
{
  const s = pres.addSlide(); s.background = { color: C.ink };
  [C.bronze, C.silver, C.gold].forEach((c, i) => s.addShape(pres.shapes.OVAL, { x: 0.5 + i * 0.42, y: 1.2, w: 0.32, h: 0.32, fill: { color: c } }));
  t(s, "Obrigado!", { x: 0.5, y: 1.75, w: 9, h: 0.9, fontFace: HEAD, fontSize: 44, bold: true, color: C.white });
  t(s, "Perguntas?", { x: 0.5, y: 2.6, w: 9, h: 0.6, fontSize: 24, color: C.amber });
  t(s, "Repositório: [link do GitHub]\nREADME · docs/arquitetura.md · docs/apresentacao.md", { x: 0.5, y: 3.8, w: 8, h: 0.8, fontSize: 13, color: "CADCFC" });
  s.addNotes("Respostas curtas para perguntas prováveis estão em docs/apresentacao.md.");
}

pres.writeFile({ fileName: __dirname + "/apresentacao_projeto_final.pptx" }).then(f => console.log("ok", f));
