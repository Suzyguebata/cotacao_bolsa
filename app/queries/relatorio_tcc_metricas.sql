-- ==============================================================================
-- RELATORIO DE METRICAS E ANALISE - ARQUITETURA MEDALLION (TCC)
-- Queries alinhadas ao schema atual gerado por consumer/tratamento.py.
-- Executar no console do Trino: docker exec -it app-trino-1 trino
-- ==============================================================================

-- 1. Visao geral do Data Lake.
SELECT 'Bronze' as camada, count(*) as total FROM delta.bronze.cotacoes
UNION ALL
SELECT 'Silver' as camada, count(*) as total FROM delta.silver.cotacoes
UNION ALL
SELECT 'Silver Rejeitados' as camada, count(*) as total FROM delta.silver.cotacoes_rejeitadas
UNION ALL
SELECT 'Gold Operacional' as camada, count(*) as total FROM delta.gold.media_precos_ingestao_5min
UNION ALL
SELECT 'Gold Atualizacao Brapi' as camada, count(*) as total FROM delta.gold.media_precos_atualizacao_5min;

-- 2. Throughput de processamento na Silver.
SELECT
    date_trunc('minute', data_hora_ingestao) as minuto_processamento,
    count(*) as total_registros,
    round(count(*) / 60.0, 2) as registros_por_segundo
FROM delta.silver.cotacoes
GROUP BY 1
ORDER BY 1 DESC;

-- 3. Analise de negocio por ativo.
SELECT
    ticket_ativo_b3,
    count(*) as num_amostras,
    round(avg(valor_atual), 2) as preco_medio,
    min(valor_atual) as preco_minimo,
    max(valor_atual) as preco_maximo,
    round(max(valor_atual) - min(valor_atual), 2) as volatilidade_janela
FROM delta.silver.cotacoes
GROUP BY ticket_ativo_b3
ORDER BY num_amostras DESC;

-- 4. Qualidade dos dados na Bronze e Silver.
SELECT
    count(*) as total_bronze,
    count_if(symbol IS NOT NULL AND trim(symbol) <> '') as com_ticker,
    count_if(regularMarketPrice IS NOT NULL AND regularMarketPrice > 0) as com_preco_valido,
    count_if(try(from_iso8601_timestamp(regularMarketTime)) IS NOT NULL) as com_timestamp_evento_valido,
    count_if(data_hora_ingestao IS NOT NULL) as com_ingestao_valida
FROM delta.bronze.cotacoes;

SELECT
    count(*) as total_silver_validos,
    count_if(ticket_ativo_b3 IS NULL OR trim(ticket_ativo_b3) = '') as tickers_invalidos_remanescentes,
    count_if(valor_atual IS NULL OR valor_atual <= 0) as precos_invalidos_remanescentes,
    count_if(data_hora_atualizacao IS NULL) as atualizacoes_invalidas_remanescentes,
    count_if(data_hora_ingestao IS NULL) as ingestoes_invalidas_remanescentes
FROM delta.silver.cotacoes;

SELECT
    rejection_reason,
    count(*) as total_rejeitados
FROM delta.silver.cotacoes_rejeitadas
GROUP BY rejection_reason
ORDER BY total_rejeitados DESC;

SELECT
    symbol as ticket_ativo_b3,
    regularMarketTime as data_hora_atualizacao,
    regularMarketPrice as valor_atual,
    count(*) as ocorrencias_repetidas
FROM delta.bronze.cotacoes
WHERE symbol IS NOT NULL
  AND regularMarketTime IS NOT NULL
  AND regularMarketPrice IS NOT NULL
GROUP BY symbol, regularMarketTime, regularMarketPrice
HAVING count(*) > 1
ORDER BY ocorrencias_repetidas DESC;

-- 5. Duracao total do pipeline pela janela de ingestao Silver.
SELECT
    min(data_hora_ingestao) as primeira_ingestao,
    max(data_hora_ingestao) as ultima_ingestao,
    date_diff('second', min(data_hora_ingestao), max(data_hora_ingestao)) as duracao_total_segundos,
    date_diff('minute', min(data_hora_ingestao), max(data_hora_ingestao)) as duracao_total_minutos
FROM delta.silver.cotacoes;

-- 6. Latencia fim a fim por etapa.
-- data_hora_atualizacao vem da Brapi; data_hora_kafka vem do Kafka; data_hora_ingestao e a chegada na Bronze/Silver.
SELECT
    ticket_ativo_b3,
    count(*) as total_amostras,
    round(avg(date_diff('second', data_hora_atualizacao, data_hora_kafka)), 2) as media_fonte_para_kafka_segundos,
    round(avg(date_diff('second', data_hora_kafka, data_hora_ingestao)), 2) as media_kafka_para_silver_segundos,
    round(avg(date_diff('second', data_hora_atualizacao, data_hora_ingestao)), 2) as media_fonte_para_silver_segundos,
    min(date_diff('second', data_hora_atualizacao, data_hora_ingestao)) as menor_latencia_total_segundos,
    max(date_diff('second', data_hora_atualizacao, data_hora_ingestao)) as maior_latencia_total_segundos
FROM delta.silver.cotacoes
WHERE data_hora_atualizacao IS NOT NULL
  AND data_hora_kafka IS NOT NULL
  AND data_hora_ingestao IS NOT NULL
GROUP BY ticket_ativo_b3
ORDER BY media_fonte_para_silver_segundos DESC;

-- 7. Amostras recentes de latencia.
SELECT
    ticket_ativo_b3,
    data_hora_atualizacao,
    data_hora_kafka,
    data_hora_ingestao,
    date_diff('second', data_hora_atualizacao, data_hora_kafka) as fonte_para_kafka_segundos,
    date_diff('second', data_hora_kafka, data_hora_ingestao) as kafka_para_silver_segundos,
    date_diff('second', data_hora_atualizacao, data_hora_ingestao) as fonte_para_silver_segundos
FROM delta.silver.cotacoes
WHERE data_hora_atualizacao IS NOT NULL
  AND data_hora_kafka IS NOT NULL
  AND data_hora_ingestao IS NOT NULL
ORDER BY data_hora_ingestao DESC
LIMIT 20;

-- 8. Verificacao de particionamento fisico da Silver.
SELECT ticket_ativo_b3, "data", count(*) as registros_na_particao
FROM delta.silver.cotacoes
GROUP BY ticket_ativo_b3, "data"
ORDER BY ticket_ativo_b3, "data";

-- 9. Agregados Gold Operacional, janela de ingestao de 5 minutos.
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
ORDER BY inicio_periodo DESC;

-- 10. Agregados Gold Atualizacao Brapi, janela por horario real da cotacao.
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
ORDER BY inicio_periodo DESC;

-- 11. Percentis de latencia por ticker.
SELECT
    ticket_ativo_b3,
    approx_percentile(date_diff('second', data_hora_atualizacao, data_hora_ingestao), 0.5) AS p50,
    approx_percentile(date_diff('second', data_hora_atualizacao, data_hora_ingestao), 0.95) AS p95,
    approx_percentile(date_diff('second', data_hora_atualizacao, data_hora_ingestao), 0.99) AS p99,
    count(*) AS total
FROM delta.silver.cotacoes
WHERE data_hora_atualizacao IS NOT NULL
  AND data_hora_ingestao IS NOT NULL
GROUP BY ticket_ativo_b3
ORDER BY p95 DESC;

-- 12. Late arrivals acima de 300 segundos.
SELECT
    ticket_ativo_b3,
    count(*) AS late_count,
    round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct_late
FROM delta.silver.cotacoes
WHERE date_diff('second', data_hora_atualizacao, data_hora_ingestao) > 300
GROUP BY ticket_ativo_b3
ORDER BY late_count DESC;

-- 13. Data Quality Score por ticker.
WITH metrics AS (
    SELECT
        ticket_ativo_b3,
        count(*) as total_processado,
        count_if(ticket_ativo_b3 IS NOT NULL AND valor_atual > 0) as registros_validos,
        count_if(valor_mercado_total IS NOT NULL AND valor_mercado_total > 0) as com_marketcap,
        count_if(date_diff('second', data_hora_atualizacao, data_hora_ingestao) < 600) as dentro_sla_latencia,
        count_if(variacao_valor_dia_anterior IS NOT NULL) as com_variacao
    FROM delta.silver.cotacoes
    GROUP BY ticket_ativo_b3
)
SELECT
    ticket_ativo_b3,
    total_processado,
    round(100.0 * registros_validos / total_processado, 2) as validade_score,
    round(100.0 * com_marketcap / total_processado, 2) as completude_score,
    round(100.0 * dentro_sla_latencia / total_processado, 2) as freshness_score,
    round(100.0 * com_variacao / total_processado, 2) as variacao_score,
    round(
        (
            (1.0 * registros_validos / total_processado) +
            (1.0 * com_marketcap / total_processado) +
            (1.0 * dentro_sla_latencia / total_processado)
        ) / 3.0 * 100,
        2
    ) as global_quality_score
FROM metrics
ORDER BY global_quality_score DESC;
