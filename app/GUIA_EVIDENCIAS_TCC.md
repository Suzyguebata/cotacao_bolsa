# Guia de evidencias do TCC

Este roteiro ajuda a coletar evidencias reproduziveis da API e do pipeline de cotacoes. A janela planejada para a evidencia e o pregao de acoes, das 09:45 as 18:00 no horario de Brasilia, incluindo o after-market. O fluxo atual e baseado em polling: o agendador chama a rota interna FastAPI `GET /coletar/{ticker}`; essa rota consulta primeiro a rota externa Brapi v2 `GET /api/v2/stocks/quote?symbols={ticker}`, valida a resposta e publica no Kafka. Se a v2 falhar, a API tenta o endpoint legado Brapi `/api/quote/{ticker}` como fallback. A rota interna `/coletar/{ticker}` continua existindo; a migracao para v2 e na chamada da API para a Brapi. Isso demonstra ingestao near real time por polling, nao notificacoes/webhooks da Brapi. A configuracao atual cobre os 15 tickers de acoes definidos em `.env`; ela nao coleta opcoes, ETFs ou futuros.

## 1. Preparar a execucao

No Git Bash, entre em `app` e confira o arquivo `.env`:

```bash
cd app
grep -E '^(MARKET_DATA_POLL_INTERVAL_MINUTES|MARKET_DATA_TICKERS|LOG_LEVEL)=' .env
```

Esse `grep` apenas le e mostra os valores dessas configuracoes uma vez; nao acompanha o processo, nao inicia a coleta e nao registra logs. A coleta e iniciada pelo `docker-compose up`/`start_pipeline.sh` e continua enquanto os containers do agendador e da API estiverem rodando. Confirme a lista de ativos e o intervalo antes de iniciar. O token deve estar definido em `BRAPI_TOKEN`, mas nao o imprima no terminal nem o inclua em capturas de tela.

**Para esta coleta controlada, nao execute `./start_pipeline.sh`:** esse script executa `docker-compose up -d --build` sem limitar os servicos, o que tambem inicia o agendador e dispara coleta imediatamente. Em vez disso, prepare apenas os servicos listados abaixo; esta etapa nao inicia o agendador. Execute-a antes das 09:45:

```bash
docker-compose up -d --build zookeeper kafka kafka-init minio spark-master spark-worker trino api spark-bronze spark-silver spark-quarentena spark-gold-operacional spark-gold-financeiro prometheus grafana
docker-compose ps
```

Confira em `docker-compose ps` que os servicos preparados estao ativos e que `agendador` **nao** esta ativo. `docker-compose ps` apenas lista o estado; nao inicia servicos nem chama a Brapi.

Se a API precisar carregar codigo ou variaveis atualizadas, recrie-a antes da sessao; este comando nao inicia o agendador:

```bash
docker-compose up -d --build api
```

Nao execute `reset_pipeline.sh` para uma coleta que precise preservar dados: o reset remove volumes Docker e dados persistidos.

Verifique a API:

```bash
curl -i http://localhost:8000/health
```

Confirme que `.env` esta configurado com os 15 tickers e intervalo de cinco minutos. Na secao 2, inicie primeiro a captura de logs; em seguida, as 09:45, inicie o agendador em outro terminal.

**Confira a quota Brapi antes de iniciar o agendador.** Com 15 tickers a cada cinco minutos entre 09:45 e 18:00, o plano representa ate 99 ciclos por ticker: aproximadamente **1.485 chamadas externas Brapi** se cada chamada v2 for bem-sucedida. Quando a v2 falha, o fallback legado pode acrescentar outra chamada para aquele ticker. Voce informou uma media observada de 111 requisicoes/dia; 1.485 e cerca de 13,4 vezes esse volume. A media do painel nao confirma o limite nem o saldo disponivel do seu plano: confira a quota restante e nao inicie esta configuracao se ela nao comportar o teste. Se for necessario reduzir tickers ou aumentar o intervalo, ajuste tambem os valores esperados na query 14 do SQL antes da coleta.

As 1.485 chamadas sao uma projecao, nao garantia: falhas, reinicios, duracao dos ciclos e respostas da fonte alteram o total. O agendador consulta os tickers sequencialmente e inicia um ciclo imediatamente; se um ciclo exceder os cinco minutos configurados, o APScheduler pode nao iniciar outra execucao enquanto a anterior estiver ativa.

Se as tabelas Delta ja existem no MinIO e estao registradas no Trino, nao faca nada. Numa primeira execucao, espere a primeira coleta ser processada e as pastas Delta serem criadas; depois registre-as:

```bash
./register_trino_tables.sh
```

Se essa etapa ja foi feita e os servicos/tabelas continuam disponiveis, nao e necessario repeti-la.

## 2. Capturar logs do Docker Compose

Crie a pasta local de evidencias:

```bash
mkdir -p evidencias/logs
```

Com os servicos ativos, inicie este comando em um terminal pouco antes de iniciar o agendador. Ele acompanha os eventos novos, incluindo API, scheduler, Kafka e Spark:

```bash
docker-compose logs --follow --tail=0 --timestamps --no-color | tee evidencias/logs/pipeline-sessao.log
```

Com a captura de logs rodando, abra outro terminal na pasta `app`. As 09:45, inicie o agendador; ele coleta imediatamente e depois segue o intervalo:

```bash
docker-compose up -d agendador
```

Deixe a captura de logs rodando durante a coleta. As 18:00, no segundo terminal, pare somente o agendador para encerrar novas requisicoes sem derrubar Kafka, Spark, MinIO ou os dados:

```bash
docker-compose stop agendador
```

Em seguida, pressione `Ctrl+C` no terminal de logs para encerrar apenas o acompanhamento, nao os containers. Aguarde pelo menos um ciclo de processamento Spark (cinco minutos) e confira os logs Bronze/Silver para confirmar que as mensagens finais foram processadas. Depois, salve tambem os logs ainda retidos pelo Docker, incluindo a inicializacao dos servicos:

```bash
docker-compose logs --since 10h --timestamps --no-color > evidencias/logs/pipeline-sessao-completa.log
```

Os arquivos locais de log podem desaparecer quando containers sao removidos; preserve-os antes de executar `docker-compose down`. Nao execute `reset_pipeline.sh`, pois ele remove volumes Docker e dados persistidos.

Para acompanhar somente as tentativas de chamada Brapi em tempo real durante a coleta e gravar essas linhas em um arquivo separado, abra um segundo terminal Git Bash na pasta `app`:

```bash
docker-compose logs --follow --tail=0 --no-color api | grep --line-buffered -E '"event": "brapi_request_(succeeded|failed)"' | tee evidencias/logs/brapi-rotas-live.log
```

Esse comando acompanha o log do container `api`; nao inicia nem controla a coleta. Ele mostra as chamadas externas para a Brapi, e nao o endpoint interno `GET /coletar/{ticker}`. Use `Ctrl+C` para encerrar apenas esse acompanhamento.

Extraia os eventos mais relevantes dos logs capturados:

```bash
grep -E '"event": "(brapi_request_succeeded|brapi_request_failed|brapi_invalid_json|brapi_schema_validation_failed)"' evidencias/logs/pipeline-sessao.log > evidencias/logs/brapi.log

grep -E '"event": "(kafka_publish_succeeded|kafka_publish_failed|kafka_publish_unexpected_error)"' evidencias/logs/pipeline-sessao.log > evidencias/logs/kafka-publish.log

grep -E '"event": "(scheduler_started|scheduler_cycle_started|ticker_collection_succeeded|ticker_collection_failed|ticker_collection_unexpected_error|scheduler_cycle_finished)"' evidencias/logs/pipeline-sessao.log > evidencias/logs/agendador.log

grep -E '"level": "(WARNING|ERROR)"' evidencias/logs/pipeline-sessao.log > evidencias/logs/avisos-erros.log
```

Os eventos `brapi_request_*` registram ticker, rota, status HTTP e duracao; falhas de HTTP incluem o codigo de status. `kafka_publish_*` registra confirmacao ou falha de publicacao e duracao. Os eventos do agendador registram resultado/duracao por ticker e duracao total do ciclo. Revise os arquivos antes de compartilha-los e nunca inclua o `.env` ou o token da Brapi nas evidencias.

Para verificar rapidamente qual rota da Brapi foi usada nos ultimos cinco minutos:

```bash
docker-compose logs --since 5m --no-color api | grep -E '"event": "brapi_request_(succeeded|failed)"'
```

Interprete o campo `endpoint` nos eventos:

- `https://brapi.dev/api/v2/stocks/quote`: rota v2 atual;
- `https://brapi.dev/api/quote/{ticker}`: rota legada, usada como fallback.

Um evento `brapi_request_succeeded` mostra qual rota respondeu com sucesso. Se houver falha na v2 seguida por falha no endpoint legado para o mesmo ticker, ambas foram tentadas e nenhuma teve sucesso. Sem linhas no intervalo consultado, nao houve evento de requisicao Brapi nos logs retidos para esses cinco minutos; isso, por si so, nao indica falha.

## 3. Gerar e interpretar o relatorio Trino

As consultas do relatorio filtram Bronze e Silver pela janela de 09:45-18:00 da data informada (12:45-21:00 UTC) e Gold pelo inicio das janelas de agregacao. A consulta de cobertura espera 99 ciclos por ticker. Gere o relatorio depois de parar o agendador e aguardar o processamento Spark. Informe a data da sessao explicitamente; para a coleta planejada nesta sexta-feira, 02/10/2026:

```bash
bash ./exportar_relatorio_tcc.sh 2026-10-02
```

O script salva o resultado em `evidencias/queries/relatorio-<data-hora>.md` e os diagnosticos do CLI em um arquivo `.stderr.log` separado. O parametro de data evita que o relatorio dependa do relogio ou do dia em que for exportado; informe sempre a data real da coleta no formato `YYYY-MM-DD`. O relatorio inclui uma secao com os limites UTC e os parametros usados. Abra o `.md` em um visualizador Markdown (por exemplo, a visualizacao de Markdown do VS Code ou GitHub) para ver as colunas como tabela; em um editor de texto simples, aparecera a sintaxe Markdown com barras verticais. O script requer Python 3 instalado no ambiente em que o comando e executado para formatar os titulos das secoes. Confira os diagnosticos: o aviso JLine `Unable to create a system terminal` pode aparecer porque o container e executado sem terminal interativo e, sozinho, nao indica erro SQL. Se o script indicar falha, nao use o relatorio como resultado valido.

Para executar manualmente e explorar resultados, ainda e possivel abrir o Trino:

```bash
docker-compose exec trino trino
```

No arquivo `queries/relatorio_tcc_metricas.sql`, cada consulta vem precedida de um marcador `secao`; o exportador transforma esses marcadores em titulos Markdown, em vez de tabelas de uma coluna. Rode cada instrucao `SELECT` e os resultados aparecem em ordem. A sequencia recomendada vai do estado geral as metricas detalhadas:

| Ordem | Query/trecho do SQL | Evidencia |
|---|---|---|
| 1 | **1. Visao geral do Data Lake** | Registros nas camadas Bronze, Silver, rejeitados e Gold. |
| 2 | **4. Qualidade dos dados** | Parsing Bronze, regras Silver, rejeicoes e duplicidades. Esta secao tem quatro `SELECT`s. |
| 3 | **14. Cobertura por ticker** | Mensagens Bronze parseadas por ativo contra 99 ciclos esperados no periodo. |
| 4 | **15. Intervalos entre mensagens** | Gaps maiores que sete minutos por ticker na Bronze durante a sessao. |
| 5 | **6. Latencia por etapa** | Atraso reportado pela fonte e tempos Kafka-Bronze, Bronze-Silver e Kafka-Silver. |
| 6 | **11. Percentis de latencia do pipeline** | P50, P95 e P99 Kafka-Silver por ticker. |
| 7 | **12. Atraso da fonte** | Frequencia de atraso da cotacao entre o horario de mercado e a chegada ao Kafka. |
| 8 | **9 e 10. Agregados Gold** | Janelas operacionais por ingestao e financeiras por horario da cotacao. |
| 9 | **13. Data Quality Score** | Validade, completude, freshness operacional e score global por ticker. |
| 10 | **2, 3, 5, 7 e 8** | Throughput, estatisticas por ativo, duracao observada, amostras recentes e particoes. |

Registre a data/hora de cada execucao e salve os resultados (por exemplo, em capturas de tela ou em um arquivo de resultados). Em especial, a query 1 confirma quais camadas ja estao materializadas; resultados vazios nas Gold podem significar que ainda nao houve tempo para os triggers Spark concluirem.

### Interpretacao e premissas

- A query 14 estima cobertura para **15 tickers**, com intervalo de **5 minutos**, das **09:45 as 18:00** (99 ciclos esperados por ticker; 1.485 mensagens no total se todos os ciclos tiverem sucesso). Ajuste lista, horario e intervalo no SQL se a configuracao real for diferente.
- A cobertura conta registros parseados na Bronze, nao chamadas planejadas nem tentativas recusadas pela Brapi. Falhas/ausencias devem ser confrontadas com os logs.
- A query 15 usa mensagens efetivamente presentes na Bronze durante a sessao e mostra gaps acima de 420 segundos; ela nao identifica sozinha se a causa foi agendador, fonte, API, Kafka ou processamento Bronze.
- Atraso `data_hora_atualizacao_valor -> data_hora_kafka` inclui o frescor da cotacao fornecida pela Brapi. Latencia `data_hora_kafka -> data_hora_processamento_silver` mede o processamento interno Kafka-Silver. Nao interprete o atraso da fonte como tempo controlado pelo pipeline.
- O SLA de 120 segundos e aplicado a latencia interna Kafka-Silver no DQS. O processamento Spark usa triggers de cinco minutos; verifique os resultados medidos, pois a configuracao de trigger nao garante por si so cumprimento do SLA.
- Os dados de mercado podem nao mudar a cada polling. A contagem de mensagens mede ingestao, nao quantidade de variacoes de preco distintas.
- As tabelas Gold sao agregacoes em janelas. Use Bronze/Silver e logs para evidenciar cada coleta; nao use contagens Gold como contagem de chamadas a API.
- A janela SQL e baseada no horario do Kafka, que delimita a chegada dos eventos, e o calendario local esperado e BRT (UTC-3). A cobertura mede mensagens parseadas na Bronze, nao chamadas tentadas nem precos distintos.

## 4. Evidencias a preservar

Para a apresentacao, guarde em local seguro:

- logs completos e arquivos filtrados de Brapi, Kafka, agendador e avisos/erros;
- resultados das queries, com data/hora e janela de coleta;
- captura do dashboard Grafana e das aplicacoes Spark em execucao;
- configuracao nao sensivel da coleta (tickers, intervalo e duracao), sem token ou credenciais.

Os logs e resultados podem conter grande volume de dados e ficam fora do controle de retencao do Docker quando copiados para arquivo. Revise-os e armazene-os de acordo com as regras da instituicao e do projeto.
