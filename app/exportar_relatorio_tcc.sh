#!/usr/bin/env bash
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_FILE="$SCRIPT_DIR/queries/relatorio_tcc_metricas.sql"
OUTPUT_DIR="$SCRIPT_DIR/evidencias/queries"
COLLECTION_DATE="${1:-$(date +%Y-%m-%d)}"

if [[ ! "$COLLECTION_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "Informe a data da coleta no formato YYYY-MM-DD."
    exit 2
fi

TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
REPORT_FILE="$OUTPUT_DIR/relatorio-$TIMESTAMP.md"
DIAGNOSTICS_FILE="$OUTPUT_DIR/relatorio-$TIMESTAMP.stderr.log"
REPORT_TMP="$REPORT_FILE.tmp.$$"

mkdir -p "$OUTPUT_DIR"

cd "$SCRIPT_DIR"

SQL_TMP="$(mktemp "$OUTPUT_DIR/.relatorio-$TIMESTAMP.XXXXXX.sql")"
RAW_FILE="$(mktemp "$OUTPUT_DIR/.relatorio-$TIMESTAMP.XXXXXX")"
trap 'rm -f "$SQL_TMP" "$RAW_FILE" "$REPORT_TMP"' EXIT

if ! sed "s/{{DATA_COLETA}}/$COLLECTION_DATE/g" "$SQL_FILE" > "$SQL_TMP"; then
    echo "Falha ao preparar o SQL para a data $COLLECTION_DATE."
    exit 1
fi

# Pré-checagem: aponta quais tabelas faltam no Trino antes de rodar o relatório inteiro.
TABELAS_ESPERADAS="bronze.cotacoes silver.cotacoes silver.cotacoes_rejeitadas gold.media_precos_ingestao_5min gold.media_precos_atualizacao_5min"
TABELAS_REGISTRADAS="$(docker-compose exec -T trino trino --output-format=TSV --execute \
    "SELECT table_schema || '.' || table_name FROM delta.information_schema.tables WHERE table_schema IN ('bronze', 'silver', 'gold')" 2>/dev/null | tr -d '\r')"
FALTANDO=""
for tabela in $TABELAS_ESPERADAS; do
    if ! printf '%s\n' "$TABELAS_REGISTRADAS" | grep -qx "$tabela"; then
        FALTANDO="$FALTANDO $tabela"
    fi
done
if [ -n "$FALTANDO" ]; then
    echo "Tabelas ainda nao registradas no Trino:$FALTANDO"
    echo "Confira se as pastas _delta_log ja existem no MinIO (o Spark leva ate ~15 min apos a coleta)"
    echo "e execute ./register_trino_tables.sh antes de exportar novamente."
    exit 1
fi

if ! docker-compose exec -T trino trino --output-format=MARKDOWN \
    < "$SQL_TMP" > "$RAW_FILE" 2> "$DIAGNOSTICS_FILE"; then
    echo "Falha ao executar o relatorio Trino."
    echo "Nenhum relatorio valido foi gerado."
    echo "Diagnosticos: $DIAGNOSTICS_FILE"
    exit 1
fi

# Testa se o interpretador executa de fato: no Windows, "python3" pode ser o atalho da Microsoft Store.
PYTHON_BIN=""
for candidato in python3 python; do
    if command -v "$candidato" >/dev/null 2>&1 && "$candidato" -c "import sys" >/dev/null 2>&1; then
        PYTHON_BIN="$candidato"
        break
    fi
done
if [ -z "$PYTHON_BIN" ]; then
    echo "Python 3 nao encontrado (python3/python). Instale-o para formatar o relatorio."
    exit 1
fi

if ! "$PYTHON_BIN" "$SCRIPT_DIR/queries/format_trino_markdown.py" "$RAW_FILE" "$REPORT_TMP"; then
    echo "Falha ao formatar o relatorio Markdown."
    echo "Nenhum relatorio valido foi gerado."
    echo "Diagnosticos do Trino: $DIAGNOSTICS_FILE"
    exit 1
fi

mv "$REPORT_TMP" "$REPORT_FILE"

echo "Data da coleta analisada: $COLLECTION_DATE"
echo "Relatorio salvo em: $REPORT_FILE"
echo "Diagnosticos do CLI salvos em: $DIAGNOSTICS_FILE"
echo "Confira os diagnosticos; o aviso do JLine sobre terminal dumb pode ser ignorado em execucao sem TTY."
