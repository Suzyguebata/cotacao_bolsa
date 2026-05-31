# Este script simula um coletor de métricas que consulta o Trino ou os arquivos Delta
# Para simplificar o TCC, vamos focar no cálculo via SQL que pode ser feito no Trino

QUERIES = {
    "latencia_total": """
        SELECT 
            ticker,
            window_end,
            calculation_timestamp,
            date_diff('second', window_end, calculation_timestamp) as latencia_segundos
        FROM delta.gold.media_precos_5min
        ORDER BY window_end DESC
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
