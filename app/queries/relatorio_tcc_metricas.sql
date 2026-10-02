-- ==============================================================================
-- RELATORIO DE METRICAS E ANALISE - ARQUITETURA MEDALLION (TCC)
-- Queries alinhadas ao schema atual gerado por consumer/tratamento.py.
-- Exportar com secoes e cabecalhos: bash ./exportar_relatorio_tcc.sh (a partir de app/)
-- Janela da evidencia para a data selecionada: 09:45-18:00 America/Sao_Paulo (12:45-21:00 UTC),
-- na data informada ao exportador (YYYY-MM-DD).
-- ==============================================================================

SELECT '00 - Janela analisada (horario de Brasilia: 09:45-18:00)' AS secao;
SELECT
    with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '12' HOUR + INTERVAL '45' MINUTE, 'UTC') as inicio_janela_utc,
    with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '21' HOUR, 'UTC') as fim_janela_utc,
    'America/Sao_Paulo' as fuso_horario_local,
    5 as intervalo_coleta_minutos,
    99 as ciclos_esperados_por_ticker,
    15 as tickers_configurados;

SELECT '01 - Visao geral do Data Lake' AS secao;
SELECT 'Bronze' as camada, count(*) as total FROM delta.bronze.cotacoes
WHERE data_hora_kafka >= with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '12' HOUR + INTERVAL '45' MINUTE, 'UTC')
  AND data_hora_kafka < with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '21' HOUR, 'UTC')
UNION ALL
SELECT 'Silver' as camada, count(*) as total FROM delta.silver.cotacoes
WHERE data_hora_kafka >= with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '12' HOUR + INTERVAL '45' MINUTE, 'UTC')
  AND data_hora_kafka < with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '21' HOUR, 'UTC')
UNION ALL
SELECT 'Silver Rejeitados' as camada, count(*) as total FROM delta.silver.cotacoes_rejeitadas
WHERE data_hora_kafka >= with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '12' HOUR + INTERVAL '45' MINUTE, 'UTC')
  AND data_hora_kafka < with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '21' HOUR, 'UTC')
UNION ALL
SELECT 'Gold Operacional' as camada, count(*) as total FROM delta.gold.media_precos_ingestao_5min
WHERE inicio_periodo >= with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '12' HOUR + INTERVAL '45' MINUTE, 'UTC')
  AND inicio_periodo < with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '21' HOUR, 'UTC')
UNION ALL
SELECT 'Gold Atualizacao Brapi' as camada, count(*) as total FROM delta.gold.media_precos_atualizacao_5min
WHERE inicio_periodo >= with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '12' HOUR + INTERVAL '45' MINUTE, 'UTC')
  AND inicio_periodo < with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '21' HOUR, 'UTC');

SELECT '02 - Throughput de processamento Silver' AS secao;
SELECT
    date_trunc('minute', data_hora_ingestao) as minuto_processamento,
    count(*) as total_registros,
    round(count(*) / 60.0, 2) as registros_por_segundo
FROM delta.silver.cotacoes
WHERE data_hora_kafka >= with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '12' HOUR + INTERVAL '45' MINUTE, 'UTC')
  AND data_hora_kafka < with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '21' HOUR, 'UTC')
GROUP BY 1
ORDER BY 1 DESC;

SELECT '03 - Analise de negocio por ativo' AS secao;
SELECT
    ticket_ativo_b3,
    count(*) as num_amostras,
    round(avg(valor_atual), 2) as preco_medio,
    min(valor_atual) as preco_minimo,
    max(valor_atual) as preco_maximo,
    round(max(valor_atual) - min(valor_atual), 2) as volatilidade_janela
FROM delta.silver.cotacoes
WHERE data_hora_kafka >= with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '12' HOUR + INTERVAL '45' MINUTE, 'UTC')
  AND data_hora_kafka < with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '21' HOUR, 'UTC')
GROUP BY ticket_ativo_b3
ORDER BY num_amostras DESC;

SELECT '04a - Qualidade dos dados Bronze' AS secao;
SELECT
    count(*) as total_bronze,
    count_if(symbol IS NOT NULL AND trim(symbol) <> '') as com_ticker,
    count_if(regularMarketPrice IS NOT NULL AND regularMarketPrice > 0) as com_preco_valido,
    count_if(try(from_iso8601_timestamp(regularMarketTime)) IS NOT NULL) as com_timestamp_evento_valido,
    count_if(data_hora_ingestao IS NOT NULL) as com_ingestao_valida
FROM delta.bronze.cotacoes
WHERE data_hora_kafka >= with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '12' HOUR + INTERVAL '45' MINUTE, 'UTC')
  AND data_hora_kafka < with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '21' HOUR, 'UTC');

SELECT '04b - Qualidade dos dados Silver' AS secao;
SELECT
    count(*) as total_silver_validos,
    count_if(ticket_ativo_b3 IS NULL OR trim(ticket_ativo_b3) = '') as tickers_invalidos_remanescentes,
    count_if(valor_atual IS NULL OR valor_atual <= 0) as precos_invalidos_remanescentes,
    count_if(data_hora_atualizacao_valor IS NULL) as atualizacoes_invalidas_remanescentes,
    count_if(data_hora_ingestao IS NULL) as ingestoes_invalidas_remanescentes
FROM delta.silver.cotacoes
WHERE data_hora_kafka >= with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '12' HOUR + INTERVAL '45' MINUTE, 'UTC')
  AND data_hora_kafka < with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '21' HOUR, 'UTC');

SELECT '04c - Registros rejeitados' AS secao;
SELECT
    rejection_reason,
    count(*) as total_rejeitados
FROM delta.silver.cotacoes_rejeitadas
WHERE data_hora_kafka >= with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '12' HOUR + INTERVAL '45' MINUTE, 'UTC')
  AND data_hora_kafka < with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '21' HOUR, 'UTC')
GROUP BY rejection_reason
ORDER BY total_rejeitados DESC;

SELECT '04d - Possiveis duplicidades na Bronze' AS secao;
SELECT
    symbol as ticket_ativo_b3,
    regularMarketTime as data_hora_atualizacao_valor,
    regularMarketPrice as valor_atual,
    count(*) as ocorrencias_repetidas
FROM delta.bronze.cotacoes
WHERE symbol IS NOT NULL
  AND regularMarketTime IS NOT NULL
  AND regularMarketPrice IS NOT NULL
  AND data_hora_kafka >= with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '12' HOUR + INTERVAL '45' MINUTE, 'UTC')
  AND data_hora_kafka < with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '21' HOUR, 'UTC')
GROUP BY symbol, regularMarketTime, regularMarketPrice
HAVING count(*) > 1
ORDER BY ocorrencias_repetidas DESC;

SELECT '05 - Duracao observada do pipeline' AS secao;
SELECT
    min(data_hora_ingestao) as primeira_ingestao,
    max(data_hora_ingestao) as ultima_ingestao,
    date_diff('second', min(data_hora_ingestao), max(data_hora_ingestao)) as duracao_total_segundos,
    date_diff('minute', min(data_hora_ingestao), max(data_hora_ingestao)) as duracao_total_minutos
FROM delta.silver.cotacoes
WHERE data_hora_kafka >= with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '12' HOUR + INTERVAL '45' MINUTE, 'UTC')
  AND data_hora_kafka < with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '21' HOUR, 'UTC');

SELECT '06 - Latencia por etapa e ticker' AS secao;
-- O atraso da fonte (Brapi -> Kafka) nao e latencia interna do pipeline.
-- data_hora_ingestao marca a chegada na Bronze; data_hora_processamento_silver marca a Silver.
SELECT
    ticket_ativo_b3,
    count(*) as total_amostras,
    round(avg(date_diff('second', data_hora_atualizacao_valor, data_hora_kafka)), 2) as media_fonte_para_kafka_segundos,
    round(avg(date_diff('second', data_hora_kafka, data_hora_ingestao)), 2) as media_kafka_para_bronze_segundos,
    round(avg(date_diff('second', data_hora_ingestao, data_hora_processamento_silver)), 2) as media_bronze_para_silver_segundos,
    round(avg(date_diff('second', data_hora_kafka, data_hora_processamento_silver)), 2) as media_pipeline_kafka_para_silver_segundos,
    round(avg(date_diff('second', data_hora_atualizacao_valor, data_hora_processamento_silver)), 2) as media_fonte_para_silver_segundos,
    min(date_diff('second', data_hora_kafka, data_hora_processamento_silver)) as menor_latencia_pipeline_segundos,
    max(date_diff('second', data_hora_kafka, data_hora_processamento_silver)) as maior_latencia_pipeline_segundos
FROM delta.silver.cotacoes
WHERE data_hora_atualizacao_valor IS NOT NULL
  AND data_hora_kafka IS NOT NULL
  AND data_hora_ingestao IS NOT NULL
  AND data_hora_processamento_silver IS NOT NULL
  AND data_hora_kafka >= with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '12' HOUR + INTERVAL '45' MINUTE, 'UTC')
  AND data_hora_kafka < with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '21' HOUR, 'UTC')
GROUP BY ticket_ativo_b3
ORDER BY media_pipeline_kafka_para_silver_segundos DESC;

SELECT '07 - Amostras recentes de latencia' AS secao;
SELECT
    ticket_ativo_b3,
    data_hora_atualizacao_valor,
    data_hora_kafka,
    data_hora_ingestao,
    data_hora_processamento_silver,
    date_diff('second', data_hora_atualizacao_valor, data_hora_kafka) as fonte_para_kafka_segundos,
    date_diff('second', data_hora_kafka, data_hora_ingestao) as kafka_para_bronze_segundos,
    date_diff('second', data_hora_ingestao, data_hora_processamento_silver) as bronze_para_silver_segundos,
    date_diff('second', data_hora_kafka, data_hora_processamento_silver) as pipeline_kafka_para_silver_segundos,
    date_diff('second', data_hora_atualizacao_valor, data_hora_processamento_silver) as fonte_para_silver_segundos
FROM delta.silver.cotacoes
WHERE data_hora_atualizacao_valor IS NOT NULL
  AND data_hora_kafka IS NOT NULL
  AND data_hora_ingestao IS NOT NULL
  AND data_hora_processamento_silver IS NOT NULL
  AND data_hora_kafka >= with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '12' HOUR + INTERVAL '45' MINUTE, 'UTC')
  AND data_hora_kafka < with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '21' HOUR, 'UTC')
ORDER BY data_hora_ingestao DESC
LIMIT 20;

SELECT '08 - Particionamento Silver' AS secao;
SELECT
    ticket_ativo_b3,
    ano_mes_dia,
    COUNT(*) AS registros_na_particao
FROM delta.silver.cotacoes
WHERE data_hora_kafka >= with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '12' HOUR + INTERVAL '45' MINUTE, 'UTC')
  AND data_hora_kafka < with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '21' HOUR, 'UTC')
GROUP BY ticket_ativo_b3, ano_mes_dia
ORDER BY ticket_ativo_b3, ano_mes_dia;

SELECT '09 - Gold operacional por tempo de ingestao' AS secao;
SELECT
    inicio_periodo,
    fim_periodo,
    periodo_base,
    ticket_ativo_b3,
    preco_medio_periodo,
    preco_minimo_periodo,
    preco_maximo_periodo,
    quantidade_amostras,
    data_hora_processamento,
    date_diff('second', fim_periodo, data_hora_processamento) as latencia_calculo_gold_segundos
FROM delta.gold.media_precos_ingestao_5min
WHERE inicio_periodo >= with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '12' HOUR + INTERVAL '45' MINUTE, 'UTC')
  AND inicio_periodo < with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '21' HOUR, 'UTC')
ORDER BY inicio_periodo DESC;

SELECT '10 - Gold financeira por tempo da cotacao' AS secao;
SELECT
    inicio_periodo,
    fim_periodo,
    periodo_base,
    ticket_ativo_b3,
    preco_medio_periodo,
    preco_minimo_periodo,
    preco_maximo_periodo,
    quantidade_amostras,
    data_hora_processamento
FROM delta.gold.media_precos_atualizacao_5min
WHERE inicio_periodo >= with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '12' HOUR + INTERVAL '45' MINUTE, 'UTC')
  AND inicio_periodo < with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '21' HOUR, 'UTC')
ORDER BY inicio_periodo DESC;

SELECT '11 - Percentis de latencia interna Kafka-Silver' AS secao;
SELECT
    ticket_ativo_b3,
    approx_percentile(date_diff('second', data_hora_kafka, data_hora_processamento_silver), 0.50) AS p50_pipeline_segundos,
    approx_percentile(date_diff('second', data_hora_kafka, data_hora_processamento_silver), 0.95) AS p95_pipeline_segundos,
    approx_percentile(date_diff('second', data_hora_kafka, data_hora_processamento_silver), 0.99) AS p99_pipeline_segundos,
    count(*) AS total
FROM delta.silver.cotacoes
WHERE data_hora_kafka IS NOT NULL
  AND data_hora_processamento_silver IS NOT NULL
  AND data_hora_kafka >= with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '12' HOUR + INTERVAL '45' MINUTE, 'UTC')
  AND data_hora_kafka < with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '21' HOUR, 'UTC')
GROUP BY ticket_ativo_b3
ORDER BY p95_pipeline_segundos DESC;

SELECT '12 - Atraso reportado pela fonte Brapi' AS secao;
SELECT
    ticket_ativo_b3,
    count_if(date_diff('second', data_hora_atualizacao_valor, data_hora_kafka) > 300) AS amostras_fonte_com_atraso,
    round(
        100.0 * count_if(date_diff('second', data_hora_atualizacao_valor, data_hora_kafka) > 300) / count(*),
        2
    ) AS pct_amostras_fonte_com_atraso,
    approx_percentile(date_diff('second', data_hora_atualizacao_valor, data_hora_kafka), 0.95) AS p95_atraso_fonte_segundos
FROM delta.silver.cotacoes
WHERE data_hora_atualizacao_valor IS NOT NULL
  AND data_hora_kafka IS NOT NULL
  AND data_hora_kafka >= with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '12' HOUR + INTERVAL '45' MINUTE, 'UTC')
  AND data_hora_kafka < with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '21' HOUR, 'UTC')
GROUP BY ticket_ativo_b3
ORDER BY amostras_fonte_com_atraso DESC;

SELECT '13 - Data Quality Score por ticker' AS secao;
-- Freshness aqui mede SLA interno Kafka -> Silver (660s = 2 triggers de 5 min Bronze/Silver + 60s de processamento);
-- atraso da fonte e medido na query 12.
WITH metrics AS (
    SELECT
        ticket_ativo_b3,
        count(*) as total_processado,
        count_if(ticket_ativo_b3 IS NOT NULL AND valor_atual > 0) as registros_validos,
        count_if(valor_mercado_total IS NOT NULL AND valor_mercado_total > 0) as com_marketcap,
        count_if(
            data_hora_kafka IS NOT NULL
            AND data_hora_processamento_silver IS NOT NULL
            AND date_diff('second', data_hora_kafka, data_hora_processamento_silver) BETWEEN 0 AND 660
        ) as dentro_sla_pipeline,
        count_if(variacao_valor_dia_anterior IS NOT NULL) as com_variacao
    FROM delta.silver.cotacoes
    WHERE data_hora_kafka >= with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '12' HOUR + INTERVAL '45' MINUTE, 'UTC')
      AND data_hora_kafka < with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '21' HOUR, 'UTC')
    GROUP BY ticket_ativo_b3
)
SELECT
    ticket_ativo_b3,
    total_processado,
    round(100.0 * registros_validos / total_processado, 2) as validade_score,
    round(100.0 * com_marketcap / total_processado, 2) as completude_score,
    round(100.0 * dentro_sla_pipeline / total_processado, 2) as freshness_pipeline_score,
    round(100.0 * com_variacao / total_processado, 2) as variacao_score,
    round(
        (
            (1.0 * registros_validos / total_processado) +
            (1.0 * com_marketcap / total_processado) +
            (1.0 * dentro_sla_pipeline / total_processado)
        ) / 3.0 * 100,
        2
    ) as global_quality_score
FROM metrics
ORDER BY global_quality_score DESC;

SELECT '14 - Cobertura de coleta por ticker na sessao' AS secao;
-- Espera 15 tickers x 99 ciclos (09:45-18:00, intervalo de 5 min).
WITH configuracao_base AS (
    SELECT
        with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '12' HOUR + INTERVAL '45' MINUTE, 'UTC') as inicio_periodo,
        with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '21' HOUR, 'UTC') as fim_periodo,
        5 as intervalo_minutos
),
configuracao AS (
    SELECT
        inicio_periodo,
        fim_periodo,
        intervalo_minutos,
        8 * 60 / intervalo_minutos + 15 / intervalo_minutos as ciclos_esperados
    FROM configuracao_base
),
tickers_esperados (ticker) AS (
    VALUES
        ('PETR4'), ('VALE3'), ('ITUB4'), ('BBAS3'), ('MGLU3'),
        ('BBDC4'), ('ABEV3'), ('WEGE3'), ('RENT3'), ('SUZB3'),
        ('B3SA3'), ('VIVT3'), ('EQTL3'), ('LREN3'), ('AXIA3')
),
recebidos AS (
    SELECT
        symbol as ticker,
        count(*) as registros_bronze,
        count_if(status_parse_bronze = 'parse_ok') as registros_parse_ok,
        min(data_hora_kafka) as primeira_mensagem,
        max(data_hora_kafka) as ultima_mensagem
    FROM delta.bronze.cotacoes
    WHERE data_hora_kafka >= (SELECT inicio_periodo FROM configuracao)
      AND data_hora_kafka < (SELECT fim_periodo FROM configuracao)
    GROUP BY symbol
)
SELECT
    esperado.ticker,
    coalesce(recebidos.registros_bronze, 0) as registros_bronze,
    coalesce(recebidos.registros_parse_ok, 0) as registros_parse_ok,
    (SELECT ciclos_esperados FROM configuracao) as ciclos_esperados,
    round(
        100.0 * coalesce(recebidos.registros_parse_ok, 0)
        / (SELECT ciclos_esperados FROM configuracao),
        2
    ) as cobertura_pct_estimado,
    recebidos.primeira_mensagem,
    recebidos.ultima_mensagem
FROM tickers_esperados esperado
LEFT JOIN recebidos ON recebidos.ticker = esperado.ticker
ORDER BY esperado.ticker;

SELECT '15 - Gaps entre mensagens Bronze na sessao' AS secao;
-- Mostra gaps acima de 7 minutos para localizar falhas de coleta/captura.
WITH intervalos AS (
    SELECT
        symbol as ticker,
        data_hora_kafka,
        lag(data_hora_kafka) OVER (PARTITION BY symbol ORDER BY data_hora_kafka) as mensagem_anterior
    FROM delta.bronze.cotacoes
    WHERE symbol IS NOT NULL
      AND data_hora_kafka >= with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '12' HOUR + INTERVAL '45' MINUTE, 'UTC')
      AND data_hora_kafka < with_timezone(CAST(DATE '{{DATA_COLETA}}' AS TIMESTAMP) + INTERVAL '21' HOUR, 'UTC')
)
SELECT
    ticker,
    mensagem_anterior,
    data_hora_kafka as mensagem_atual,
    date_diff('second', mensagem_anterior, data_hora_kafka) as intervalo_segundos
FROM intervalos
WHERE mensagem_anterior IS NOT NULL
  AND date_diff('second', mensagem_anterior, data_hora_kafka) > 420
ORDER BY intervalo_segundos DESC
LIMIT 200;
