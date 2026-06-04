# Este script simula um coletor de métricas que consulta o Trino ou os arquivos Delta
# Para simplificar o TCC, vamos focar no cálculo via SQL que pode ser feito no Trino

QUERIES = {
    "qualidade_bronze": """
        SELECT
            count(*) as total_bronze,
            count_if(symbol IS NOT NULL AND trim(symbol) <> '') as com_ticker,
            count_if(regularMarketPrice IS NOT NULL AND regularMarketPrice > 0) as com_preco_valido,
            count_if(try(from_iso8601_timestamp(regularMarketTime)) IS NOT NULL) as com_timestamp_evento_valido,
            count_if(ingestion_timestamp IS NOT NULL) as com_ingestao_valida
        FROM delta.bronze.cotacoes;
    """,
    "qualidade_silver": """
        SELECT
            count(*) as total_silver_validos,
            count_if(ticker IS NULL OR trim(ticker) = '') as tickers_invalidos_remanescentes,
            count_if(price IS NULL OR price <= 0) as precos_invalidos_remanescentes,
            count_if(event_timestamp IS NULL) as eventos_invalidos_remanescentes,
            count_if(ingestion_timestamp IS NULL) as ingestoes_invalidas_remanescentes
        FROM delta.silver.cotacoes;
    """,
    "rejeicoes_silver": """
        SELECT
            rejection_reason,
            count(*) as total_rejeitados
        FROM delta.silver.cotacoes_rejeitadas
        GROUP BY rejection_reason
        ORDER BY total_rejeitados DESC;
    """,
    "duplicidades_bronze": """
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
    """,
    "latencia_por_etapa_silver": """
        SELECT
            ticker,
            count(*) as total_amostras,
            round(avg(date_diff('second', event_timestamp, kafka_timestamp)), 2) as media_fonte_para_kafka_segundos,
            round(avg(date_diff('second', kafka_timestamp, ingestion_timestamp)), 2) as media_kafka_para_silver_segundos,
            round(avg(date_diff('second', event_timestamp, ingestion_timestamp)), 2) as media_fonte_para_silver_segundos
        FROM delta.silver.cotacoes
        WHERE event_timestamp IS NOT NULL
          AND kafka_timestamp IS NOT NULL
          AND ingestion_timestamp IS NOT NULL
        GROUP BY ticker
        ORDER BY media_fonte_para_silver_segundos DESC;
    """,
    "amostras_recentes_latencia": """
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
    """,
    "latencia_operacional_gold": """
        SELECT 
            ticker,
            window_end,
            window_basis,
            calculation_timestamp,
            date_diff('second', window_end, calculation_timestamp) as latencia_calculo_gold_segundos
        FROM delta.gold.media_precos_ingestao_5min
        ORDER BY window_end DESC
        LIMIT 10;
    """,
    "agregados_financeiros_gold": """
        SELECT
            ticker,
            window_start,
            window_end,
            window_basis,
            avg_price,
            min_price,
            max_price,
            sample_count
        FROM delta.gold.media_precos_evento_5min
        ORDER BY window_start DESC
        LIMIT 10;
    """,
    "throughput_silver": """
        SELECT 
            date_trunc('minute', ingestion_timestamp) as minuto,
            count(*) as total_registros,
            count(*) / 60.0 as registros_por_segundo
        FROM delta.silver.cotacoes
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT 5;
    """
}

def monitorar():
    print("=== MONITOR DE PERFORMANCE (TCC) ===")
    print("Sugestão: Execute estas queries no console do Trino para coletar dados para seus gráficos.\n")
    
    for nome, sql in QUERIES.items():
        print(f"--- Métrica: {nome} ---")
        print(sql)
        print("-" * 30)

if __name__ == "__main__":
    monitorar()
