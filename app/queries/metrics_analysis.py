# Queries auxiliares para consultar as tabelas Delta pelo Trino.

QUERIES = {
    "qualidade_bronze": """
        SELECT
            count(*) as total_bronze,
            count_if(symbol IS NOT NULL AND trim(symbol) <> '') as com_ticker,
            count_if(regularMarketPrice IS NOT NULL AND regularMarketPrice > 0) as com_preco_valido,
            count_if(try(from_iso8601_timestamp(regularMarketTime)) IS NOT NULL) as com_data_hora_atualizacao_valida,
            count_if(data_hora_ingestao IS NOT NULL) as com_ingestao_valida
        FROM delta.bronze.cotacoes;
    """,
    "qualidade_silver": """
        SELECT
            count(*) as total_silver_validos,
            count_if(ticket_ativo_b3 IS NULL OR trim(ticket_ativo_b3) = '') as tickers_invalidos_remanescentes,
            count_if(valor_atual IS NULL OR valor_atual <= 0) as precos_invalidos_remanescentes,
            count_if(data_hora_atualizacao IS NULL) as atualizacoes_invalidas_remanescentes,
            count_if(data_hora_ingestao IS NULL) as ingestoes_invalidas_remanescentes
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
    """,
    "latencia_por_etapa_silver": """
        SELECT
            ticket_ativo_b3,
            count(*) as total_amostras,
            round(avg(date_diff('second', data_hora_atualizacao, data_hora_kafka)), 2) as media_fonte_para_kafka_segundos,
            round(avg(date_diff('second', data_hora_kafka, data_hora_ingestao)), 2) as media_kafka_para_silver_segundos,
            round(avg(date_diff('second', data_hora_atualizacao, data_hora_ingestao)), 2) as media_fonte_para_silver_segundos
        FROM delta.silver.cotacoes
        WHERE data_hora_atualizacao IS NOT NULL
          AND data_hora_kafka IS NOT NULL
          AND data_hora_ingestao IS NOT NULL
        GROUP BY ticket_ativo_b3
        ORDER BY media_fonte_para_silver_segundos DESC;
    """,
    "amostras_recentes_latencia": """
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
    """,
    "latencia_operacional_gold": """
        SELECT
            ticket_ativo_b3,
            fim_periodo,
            periodo_base,
            data_hora_processamento,
            date_diff('second', fim_periodo, data_hora_processamento) as latencia_calculo_gold_segundos
        FROM delta.gold.media_precos_ingestao_5min
        ORDER BY fim_periodo DESC
        LIMIT 10;
    """,
    "agregados_atualizacao_gold": """
        SELECT
            ticket_ativo_b3,
            inicio_periodo,
            fim_periodo,
            periodo_base,
            preco_medio_periodo,
            preco_minimo_periodo,
            preco_maximo_periodo,
            quantidade_amostras
        FROM delta.gold.media_precos_atualizacao_5min
        ORDER BY inicio_periodo DESC
        LIMIT 10;
    """,
    "throughput_silver": """
        SELECT
            date_trunc('minute', data_hora_ingestao) as minuto,
            count(*) as total_registros,
            round(count(*) / 60.0, 2) as registros_por_segundo
        FROM delta.silver.cotacoes
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT 5;
    """,
}


def monitorar():
    print("=== MONITOR DE PERFORMANCE (TCC) ===")
    print("Execute estas queries no console do Trino para coletar metricas.\n")

    for nome, sql in QUERIES.items():
        print(f"--- Metrica: {nome} ---")
        print(sql)
        print("-" * 30)


if __name__ == "__main__":
    monitorar()
