#!/usr/bin/env bash
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_FILE="$SCRIPT_DIR/queries/relatorio_tcc_metricas.sql"
OUTPUT_DIR="$SCRIPT_DIR/evidencias/queries"
# Uso: exportar_relatorio_tcc.sh AAAA-MM-DD [HH:MM_INICIO HH:MM_FIM]
# Horários em Brasília (UTC-3, sem horário de verão); padrão 09:45-18:00 (pregão + after-market).
COLLECTION_DATE="${1:-$(date +%Y-%m-%d)}"
JANELA_INICIO="${2:-09:45}"
JANELA_FIM="${3:-18:00}"
INTERVALO_COLETA_MINUTOS=5

if [[ ! "$COLLECTION_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "Informe a data da coleta no formato YYYY-MM-DD."
    exit 2
fi
if [[ ! "$JANELA_INICIO" =~ ^[0-2][0-9]:[0-5][0-9]$ || ! "$JANELA_FIM" =~ ^[0-2][0-9]:[0-5][0-9]$ ]]; then
    echo "Informe a janela no formato HH:MM HH:MM (horario de Brasilia). Ex.: 09:45 18:00"
    exit 2
fi

INICIO_UTC="$(date -u -d "$COLLECTION_DATE $JANELA_INICIO -0300" '+%Y-%m-%d %H:%M:%S' 2>/dev/null)"
FIM_UTC="$(date -u -d "$COLLECTION_DATE $JANELA_FIM -0300" '+%Y-%m-%d %H:%M:%S' 2>/dev/null)"
if [ -z "$INICIO_UTC" ] || [ -z "$FIM_UTC" ]; then
    echo "Data ou horario invalido: $COLLECTION_DATE $JANELA_INICIO-$JANELA_FIM."
    exit 2
fi
DURACAO_MINUTOS=$(( ($(date -u -d "$FIM_UTC" +%s) - $(date -u -d "$INICIO_UTC" +%s)) / 60 ))
if [ "$DURACAO_MINUTOS" -le 0 ]; then
    echo "O fim da janela ($JANELA_FIM) deve ser posterior ao inicio ($JANELA_INICIO)."
    exit 2
fi
# Arredonda para cima: a janela inclui o ciclo do instante inicial (495 min -> 99; 27 min -> 6).
CICLOS_ESPERADOS=$(( (DURACAO_MINUTOS + INTERVALO_COLETA_MINUTOS - 1) / INTERVALO_COLETA_MINUTOS ))

TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
REPORT_FILE="$OUTPUT_DIR/relatorio-$TIMESTAMP.md"
DIAGNOSTICS_FILE="$OUTPUT_DIR/relatorio-$TIMESTAMP.stderr.log"
REPORT_TMP="$REPORT_FILE.tmp.$$"

mkdir -p "$OUTPUT_DIR"

cd "$SCRIPT_DIR"

SQL_TMP="$(mktemp "$OUTPUT_DIR/.relatorio-$TIMESTAMP.XXXXXX.sql")"
RAW_FILE="$(mktemp "$OUTPUT_DIR/.relatorio-$TIMESTAMP.XXXXXX")"
trap 'rm -f "$SQL_TMP" "$RAW_FILE" "$REPORT_TMP"' EXIT

if ! sed -e "s/{{INICIO_UTC}}/$INICIO_UTC/g" \
         -e "s/{{FIM_UTC}}/$FIM_UTC/g" \
         -e "s/{{JANELA_BRT}}/$JANELA_INICIO-$JANELA_FIM/g" \
         -e "s/{{CICLOS_ESPERADOS}}/$CICLOS_ESPERADOS/g" \
         "$SQL_FILE" > "$SQL_TMP"; then
    echo "Falha ao preparar o SQL para a data $COLLECTION_DATE."
    exit 1
fi
if grep -q '{{' "$SQL_TMP"; then
    echo "Placeholder nao substituido no SQL: $(grep -o '{{[A-Z_]*}}' "$SQL_TMP" | sort -u | tr '\n' ' ')"
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

echo "Data da coleta analisada: $COLLECTION_DATE, janela $JANELA_INICIO-$JANELA_FIM BRT ($INICIO_UTC a $FIM_UTC UTC), $CICLOS_ESPERADOS ciclos esperados por ticker"
echo "Relatorio salvo em: $REPORT_FILE"
echo "Diagnosticos do CLI salvos em: $DIAGNOSTICS_FILE"
echo "Confira os diagnosticos; o aviso do JLine sobre terminal dumb pode ser ignorado em execucao sem TTY."
