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
SELECT 'Gold' as camada, count(*) as total FROM delta.gold.media_precos_5min;

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

-- 4. LATÊNCIA E TEMPO DE OPERAÇÃO
-- Calcula a duração total do pipeline e a janela de tempo dos dados.
SELECT 
    min(ingestion_timestamp) as primeira_ingestao,
    max(ingestion_timestamp) as ultima_ingestao,
    date_diff('second', min(ingestion_timestamp), max(ingestion_timestamp)) as duracao_total_segundos,
    date_diff('minute', min(ingestion_timestamp), max(ingestion_timestamp)) as duracao_total_minutos
FROM delta.silver.cotacoes;

-- 5. VERIFICAÇÃO DE PARTICIONAMENTO
-- Demonstra como o Delta Lake organiza os dados fisicamente por ticker e data.
SELECT ticker, "date", count(*) as registros_na_particao
FROM delta.silver.cotacoes
GROUP BY ticker, "date"
ORDER BY ticker, "date";

-- 6. AGREGADOS GOLD (MÉDIA MÓVEL 1 MIN)
-- Consulta a camada final de agregação com a latência de processamento.
SELECT 
    window_start,
    window_end,
    ticker,
    avg_price,
    sample_count,
    calculation_timestamp,
    date_diff('second', window_end, calculation_timestamp) as latencia_segundos
FROM delta.gold.media_precos_5min
ORDER BY window_start DESC;
