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
            count_if(data_hora_atualizacao_valor IS NULL) as atualizacoes_invalidas_remanescentes,
            count_if(data_hora_processamento_silver IS NULL) as processamentos_invalidos_remanescentes
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
            regularMarketTime as data_hora_atualizacao_valor,
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
            round(avg(date_diff('second', data_hora_atualizacao_valor, data_hora_kafka)), 2) as media_fonte_para_kafka_segundos,
            round(avg(date_diff('second', data_hora_kafka, data_hora_ingestao)), 2) as media_kafka_para_bronze_segundos,
            round(avg(date_diff('second', data_hora_ingestao, data_hora_processamento_silver)), 2) as media_bronze_para_silver_segundos,
            round(avg(date_diff('second', data_hora_kafka, data_hora_processamento_silver)), 2) as media_pipeline_kafka_para_silver_segundos,
            round(avg(date_diff('second', data_hora_atualizacao_valor, data_hora_processamento_silver)), 2) as media_fonte_para_silver_segundos
        FROM delta.silver.cotacoes
        WHERE data_hora_atualizacao_valor IS NOT NULL
          AND data_hora_kafka IS NOT NULL
          AND data_hora_ingestao IS NOT NULL
          AND data_hora_processamento_silver IS NOT NULL
        GROUP BY ticket_ativo_b3
        ORDER BY media_pipeline_kafka_para_silver_segundos DESC;
    """,
    "percentis_latencia_pipeline": """
        SELECT
            ticket_ativo_b3,
            approx_percentile(date_diff('second', data_hora_kafka, data_hora_processamento_silver), 0.50) as p50_pipeline_segundos,
            approx_percentile(date_diff('second', data_hora_kafka, data_hora_processamento_silver), 0.95) as p95_pipeline_segundos,
            approx_percentile(date_diff('second', data_hora_kafka, data_hora_processamento_silver), 0.99) as p99_pipeline_segundos
        FROM delta.silver.cotacoes
        WHERE data_hora_kafka IS NOT NULL AND data_hora_processamento_silver IS NOT NULL
        GROUP BY ticket_ativo_b3
        ORDER BY p95_pipeline_segundos DESC;
    """,
    "amostras_recentes_latencia": """
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
        ORDER BY data_hora_processamento_silver DESC
        LIMIT 20;
    """,
    "recalculo_gold_operacional": """
        SELECT
            ticket_ativo_b3,
            fim_periodo,
            periodo_base,
            data_hora_processamento,
            -- Modo complete: data_hora_processamento e o ultimo recalculo, nao a latencia de calculo.
            date_diff('second', fim_periodo, data_hora_processamento) as segundos_desde_fim_janela_ate_ultimo_recalculo
        FROM delta.gold.media_precos_ingestao_5min
        ORDER BY fim_periodo DESC
        LIMIT 10;
    """,
    "agregados_financeiros_gold": """
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
            date_trunc('minute', data_hora_processamento_silver) as minuto,
            count(*) as total_registros,
            round(count(*) / 60.0, 2) as registros_por_segundo
        FROM delta.silver.cotacoes
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT 5;
    """,
    "cobertura_coleta_ultimas_24_horas": """
        WITH configuracao_base AS (
            SELECT
                current_timestamp - INTERVAL '1' DAY as inicio_periodo,
                5 as intervalo_minutos
        ),
        configuracao AS (
            SELECT
                inicio_periodo,
                intervalo_minutos,
                24 * 60 / intervalo_minutos as ciclos_esperados
            FROM configuracao_base
        ),
        tickers_esperados (ticker) AS (
            VALUES
                ('PETR4'), ('VALE3'), ('ITUB4'), ('BBAS3'), ('MGLU3'),
                ('BBDC4'), ('ABEV3'), ('WEGE3'), ('RENT3'), ('SUZB3'),
                ('B3SA3'), ('VIVT3'), ('EQTL3'), ('LREN3'), ('AXIA3')
        ),
        recebidos AS (
            SELECT symbol as ticker, count_if(status_parse_bronze = 'parse_ok') as registros_parse_ok
            FROM delta.bronze.cotacoes
            WHERE data_hora_kafka >= (SELECT inicio_periodo FROM configuracao)
            GROUP BY symbol
        )
        SELECT
            esperado.ticker,
            coalesce(recebidos.registros_parse_ok, 0) as registros_parse_ok,
            (SELECT ciclos_esperados FROM configuracao) as ciclos_esperados,
            round(100.0 * coalesce(recebidos.registros_parse_ok, 0) / (SELECT ciclos_esperados FROM configuracao), 2) as cobertura_pct_estimado
        FROM tickers_esperados esperado
        LEFT JOIN recebidos ON recebidos.ticker = esperado.ticker
        ORDER BY esperado.ticker;
    """,
    "gaps_coleta_bronze_ultimas_24_horas": """
        WITH intervalos AS (
            SELECT
                symbol as ticker,
                data_hora_kafka,
                lag(data_hora_kafka) OVER (PARTITION BY symbol ORDER BY data_hora_kafka) as mensagem_anterior
            FROM delta.bronze.cotacoes
            WHERE symbol IS NOT NULL
              AND data_hora_kafka >= current_timestamp - INTERVAL '1' DAY
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
