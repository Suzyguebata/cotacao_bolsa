-- ==============================================================================
-- RELATÓRIO DE MÉTRICAS E ANÁLISE - ARQUITETURA MEDALLION (TCC)
-- Este arquivo contém as queries validadas para extração de métricas do pipeline.
-- Executar no console do Trino (docker exec -it app-trino-1 trino)
-- ==============================================================================

-- 1. VISÃO GERAL DO DATA LAKE (VERIFICAÇÃO DE SAÚDE)
-- Retorna o total de registros processados em cada camada.
SELECT 'Bronze' as camada, count(*) as total FROM delta.bronze.cotacoes
UNION ALL
SELECT 'Silver' as camada, count(*) as total FROM delta.silver.cotacoes
UNION ALL
SELECT 'Silver Rejeitados' as camada, count(*) as total FROM delta.silver.cotacoes_rejeitadas
UNION ALL
SELECT 'Gold Operacional' as camada, count(*) as total FROM delta.gold.media_precos_ingestao_5min
UNION ALL
SELECT 'Gold Financeira' as camada, count(*) as total FROM delta.gold.media_precos_evento_5min;

-- 2. THROUGHPUT (VAZÃO) DE PROCESSAMENTO
-- Analisa quantos registros foram processados por minuto na camada Silver.
SELECT 
    date_trunc('minute', ingestion_timestamp) as minuto_processamento,
    count(*) as total_registros,
    round(count(*) / 60.0, 2) as registros_por_segundo
FROM delta.silver.cotacoes
GROUP BY 1
ORDER BY 1 DESC;

-- 3. ANÁLISE DE NEGÓCIO (ATIVOS FINANCEIROS)
-- Calcula métricas financeiras sobre os dados refinados da Silver.
SELECT
    ticker,
    count(*) as num_amostras,
    round(avg(price), 2) as preco_medio,
    min(price) as preco_minimo,
    max(price) as preco_maximo,
    round(max(price) - min(price), 2) as volatilidade_janela
FROM delta.silver.cotacoes
GROUP BY ticker
ORDER BY num_amostras DESC;

-- 4. QUALIDADE DOS DADOS NA CAMADA SILVER
-- Quantifica registros válidos e inválidos segundo as regras aplicadas na transformação Silver.
SELECT
    count(*) as total_bronze,
    count_if(symbol IS NOT NULL AND trim(symbol) <> '') as com_ticker,
    count_if(regularMarketPrice IS NOT NULL AND regularMarketPrice > 0) as com_preco_valido,
    count_if(try(from_iso8601_timestamp(regularMarketTime)) IS NOT NULL) as com_timestamp_evento_valido,
    count_if(ingestion_timestamp IS NOT NULL) as com_ingestao_valida
FROM delta.bronze.cotacoes;

SELECT
    count(*) as total_silver_validos,
    count_if(ticker IS NULL OR trim(ticker) = '') as tickers_invalidos_remanescentes,
    count_if(price IS NULL OR price <= 0) as precos_invalidos_remanescentes,
    count_if(event_timestamp IS NULL) as eventos_invalidos_remanescentes,
    count_if(ingestion_timestamp IS NULL) as ingestoes_invalidas_remanescentes
FROM delta.silver.cotacoes;

SELECT
    rejection_reason,
    count(*) as total_rejeitados
FROM delta.silver.cotacoes_rejeitadas
GROUP BY rejection_reason
ORDER BY total_rejeitados DESC;

SELECT
    symbol as ticker,
    regularMarketTime,
    regularMarketPrice,
    count(*) as ocorrencias_repetidas
FROM delta.bronze.cotacoes
WHERE symbol IS NOT NULL
  AND regularMarketTime IS NOT NULL
  AND regularMarketPrice IS NOT NULL
GROUP BY symbol, regularMarketTime, regularMarketPrice
HAVING count(*) > 1
ORDER BY ocorrencias_repetidas DESC;

-- 5. LATÊNCIA E TEMPO DE OPERAÇÃO
-- Calcula a duração total do pipeline e a janela de tempo dos dados.
SELECT 
    min(ingestion_timestamp) as primeira_ingestao,
    max(ingestion_timestamp) as ultima_ingestao,
    date_diff('second', min(ingestion_timestamp), max(ingestion_timestamp)) as duracao_total_segundos,
    date_diff('minute', min(ingestion_timestamp), max(ingestion_timestamp)) as duracao_total_minutos
FROM delta.silver.cotacoes;

-- 6. LATÊNCIA FIM A FIM POR ETAPA
-- Separa a latência da fonte, da ingestão Kafka/Spark e o atraso total até a Silver.
-- event_timestamp vem da Brapi; kafka_timestamp vem do Kafka; ingestion_timestamp é a chegada na camada Silver.
SELECT
    ticker,
    count(*) as total_amostras,
    round(avg(date_diff('second', event_timestamp, kafka_timestamp)), 2) as media_fonte_para_kafka_segundos,
    round(avg(date_diff('second', kafka_timestamp, ingestion_timestamp)), 2) as media_kafka_para_silver_segundos,
    round(avg(date_diff('second', event_timestamp, ingestion_timestamp)), 2) as media_fonte_para_silver_segundos,
    min(date_diff('second', event_timestamp, ingestion_timestamp)) as menor_latencia_total_segundos,
    max(date_diff('second', event_timestamp, ingestion_timestamp)) as maior_latencia_total_segundos
FROM delta.silver.cotacoes
WHERE event_timestamp IS NOT NULL
  AND kafka_timestamp IS NOT NULL
  AND ingestion_timestamp IS NOT NULL
GROUP BY ticker
ORDER BY media_fonte_para_silver_segundos DESC;

-- 7. AMOSTRAS RECENTES DE LATÊNCIA
-- Ajuda a explicar casos extremos durante a apresentação.
SELECT
    ticker,
    event_timestamp,
    kafka_timestamp,
    ingestion_timestamp,
    date_diff('second', event_timestamp, kafka_timestamp) as fonte_para_kafka_segundos,
    date_diff('second', kafka_timestamp, ingestion_timestamp) as kafka_para_silver_segundos,
    date_diff('second', event_timestamp, ingestion_timestamp) as fonte_para_silver_segundos
FROM delta.silver.cotacoes
WHERE event_timestamp IS NOT NULL
ORDER BY ingestion_timestamp DESC
LIMIT 20;

-- 8. VERIFICAÇÃO DE PARTICIONAMENTO
-- Demonstra como o Delta Lake organiza os dados fisicamente por ticker e data.
SELECT ticker, "date", count(*) as registros_na_particao
FROM delta.silver.cotacoes
GROUP BY ticker, "date"
ORDER BY ticker, "date";

-- 9. AGREGADOS GOLD OPERACIONAL (JANELA DE INGESTÃO 5 MIN)
-- Consulta a camada de agregação operacional baseada em ingestion_timestamp.
SELECT 
    window_start,
    window_end,
    window_basis,
    ticker,
    avg_price,
    sample_count,
    calculation_timestamp,
    date_diff('second', window_end, calculation_timestamp) as latencia_calculo_gold_segundos
FROM delta.gold.media_precos_ingestao_5min
ORDER BY window_start DESC;

-- 11. PERCENTIS DE LATÊNCIA POR TICKER (P50, P95, P99)
-- Demonstra a distribuição do atraso entre evento e ingestão final.
SELECT ticker,
   approx_percentile(date_diff('second', event_timestamp, ingestion_timestamp), 0.5) AS p50,
   approx_percentile(date_diff('second', event_timestamp, ingestion_timestamp), 0.95) AS p95,
   approx_percentile(date_diff('second', event_timestamp, ingestion_timestamp), 0.99) AS p99,
   count(*) AS total
 FROM delta.silver.cotacoes
 WHERE event_timestamp IS NOT NULL AND ingestion_timestamp IS NOT NULL
 GROUP BY ticker
 ORDER BY p95 DESC;

-- 12. ANÁLISE DE LATE ARRIVALS (THRESHOLD 300S)
-- Quantifica dados que chegaram com mais de 5 minutos de atraso em relação ao mercado.
SELECT ticker, 
       count(*) AS late_count, 
       round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct_late
 FROM delta.silver.cotacoes
 WHERE date_diff('second', event_timestamp, ingestion_timestamp) > 300
 GROUP BY ticker 
 ORDER BY late_count DESC;

-- 14. DATA QUALITY SCORE (DQS) POR TICKER
-- Consolida as dimensões de qualidade em um score de 0 a 100.
-- Dimensões: Sucesso de Parsing, Completude (preço/ticker) e Integridade de Tempo.
WITH metrics AS (
    SELECT 
        ticker,
        count(*) as total_processado,
        -- Validade: Ticker e Preço presentes e corretos
        count_if(ticker IS NOT NULL AND price > 0) as registros_validos,
        -- Completude: MarketCap (que costuma falhar) presente
        count_if(marketCap IS NOT NULL AND marketCap > 0) as com_marketcap,
        -- Freshness: Latência menor que 2 minutos (Near Real Time)
        count_if(date_diff('second', event_timestamp, ingestion_timestamp) < 120) as dentro_sla_latencia,
        -- Integridade: Mudança de preço não nula
        count_if(change IS NOT NULL) as com_change
    FROM delta.silver.cotacoes
    GROUP BY ticker
)
SELECT 
    ticker,
    total_processado,
    round(100.0 * registros_validos / total_processado, 2) as validade_score,
    round(100.0 * com_marketcap / total_processado, 2) as completude_score,
    round(100.0 * dentro_sla_latencia / total_processado, 2) as freshness_score,
    round(
        ( (1.0 * registros_validos / total_processado) + 
          (1.0 * com_marketcap / total_processado) + 
          (1.0 * dentro_sla_latencia / total_processado) ) / 3.0 * 100, 2
    ) as global_quality_score
FROM metrics
ORDER BY global_quality_score DESC;
