#!/bin/bash

echo "============================================"
echo "   LIMPANDO AMBIENTE (RESET TOTAL)"
echo "============================================"

# 1. Parar containers e remover volumes
echo "[1/3] Parando containers e removendo volumes do Docker..."
docker-compose down -v

# 2. Limpar volumes locais e pastas temporárias
echo "[2/3] Removendo pastas de dados locais, checkpoints e metastore do Trino..."
rm -rf trino/metastore/*
rm -rf trino/metastore/.* 2>/dev/null
# Remove volumes persistentes se houver pastas mapeadas (opcional dependendo do setup)
# rm -rf ./trino/metastore/* 

# 3. Limpar ambiente virtual (opcional)
if [ -d "venv" ]; then
    echo "[3/3] Removendo ambiente virtual antigo..."
    rm -rf venv
fi

echo ""
echo "Ambiente limpo com sucesso!"
echo "Para iniciar do zero, execute: ./start_pipeline.sh"
echo "============================================"
