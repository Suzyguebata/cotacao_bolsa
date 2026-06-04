#!/bin/bash

set -u

MAX_ATTEMPTS="${TRINO_REGISTER_MAX_ATTEMPTS:-18}"
SLEEP_SECONDS="${TRINO_REGISTER_SLEEP_SECONDS:-20}"

echo "============================================"
echo "   REGISTRANDO TABELAS DELTA NO TRINO"
echo "============================================"
echo

docker compose up -d trino

TRINO_CONTAINER="$(docker compose ps -q trino)"
if [ -z "$TRINO_CONTAINER" ]; then
    echo "[ERRO] Container do Trino não encontrado."
    exit 1
fi

run_trino() {
    docker exec "$TRINO_CONTAINER" trino --execute "$1"
}

run_with_retry() {
    local label="$1"
    local sql="$2"
    local attempt=1
    local output

    while [ "$attempt" -le "$MAX_ATTEMPTS" ]; do
        echo "[$attempt/$MAX_ATTEMPTS] $label"
        output="$(run_trino "$sql" 2>&1)"
        status=$?

        if [ "$status" -eq 0 ]; then
            echo "OK: $label"
            echo
            return 0
        fi

        if echo "$output" | grep -qi "already exists\|Table.*exists"; then
            echo "OK: $label já estava registrada."
            echo
            return 0
        fi

        echo "$output"
        echo "Aguardando $SLEEP_SECONDS segundo(s) antes de tentar novamente..."
        echo
        sleep "$SLEEP_SECONDS"
        attempt=$((attempt + 1))
    done

    echo "[ERRO] Falha ao executar: $label"
    return 1
}

failures=0

run_with_retry "Criar schema bronze" "CREATE SCHEMA IF NOT EXISTS delta.bronze" || failures=1
run_with_retry "Criar schema silver" "CREATE SCHEMA IF NOT EXISTS delta.silver" || failures=1
run_with_retry "Criar schema gold" "CREATE SCHEMA IF NOT EXISTS delta.gold" || failures=1

run_with_retry "Registrar delta.bronze.cotacoes" "CALL delta.system.register_table(schema_name => 'bronze', table_name => 'cotacoes', table_location => 's3a://datalake/bronze/cotacoes')" || failures=1
run_with_retry "Registrar delta.silver.cotacoes" "CALL delta.system.register_table(schema_name => 'silver', table_name => 'cotacoes', table_location => 's3a://datalake/silver/cotacoes')" || failures=1
run_with_retry "Registrar delta.silver.cotacoes_rejeitadas" "CALL delta.system.register_table(schema_name => 'silver', table_name => 'cotacoes_rejeitadas', table_location => 's3a://datalake/silver/cotacoes_rejeitadas')" || failures=1
run_with_retry "Registrar delta.gold.media_precos_ingestao_5min" "CALL delta.system.register_table(schema_name => 'gold', table_name => 'media_precos_ingestao_5min', table_location => 's3a://datalake/gold/media_precos_ingestao_5min')" || failures=1
run_with_retry "Registrar delta.gold.media_precos_evento_5min" "CALL delta.system.register_table(schema_name => 'gold', table_name => 'media_precos_evento_5min', table_location => 's3a://datalake/gold/media_precos_evento_5min')" || failures=1

if [ "$failures" -ne 0 ]; then
    echo "[ERRO] Uma ou mais tabelas não puderam ser registradas."
    echo "Verifique no MinIO se as pastas bronze, silver e gold já possuem _delta_log."
    exit 1
fi

echo "Tabelas disponíveis no Trino:"
run_trino "SHOW TABLES FROM delta.bronze"
run_trino "SHOW TABLES FROM delta.silver"
run_trino "SHOW TABLES FROM delta.gold"

echo
echo "Registro concluído."
