#!/bin/bash

echo "============================================"
echo "   INICIANDO PIPELINE LOCAL B3 (Docker)"
echo "============================================"
echo

# 1. Subir Docker Compose
echo "[1/4] Subindo Docker Compose..."
docker-compose up -d --build
echo

# 2. Executar Testes Unitários no Docker
echo "[2/4] Executando Testes Unitários (Spark Transformation)..."
MSYS_NO_PATHCONV=1 docker exec -u root app-spark-bronze-1 /usr/bin/python3 -m pytest /app/tests/test_spark_logic.py
if [ $? -ne 0 ]; then
    echo "[ERRO] Os testes unitários falharam! Verifique a lógica do Spark."
    exit 1
fi
echo "Testes aprovados!"
echo

# 3. Informar serviços conteinerizados
echo "[3/4] API e Scheduler iniciados pelo Docker Compose."
echo "FastAPI Docs: http://localhost:8000/docs"
echo

# 4. Abrir MinIO Console
echo "[4/4] Abrindo MinIO Console..."
if command -v xdg-open >/dev/null; then
    xdg-open http://localhost:9001
elif command -v open >/dev/null; then
    open http://localhost:9001
else
    echo "Abra manualmente: http://localhost:9001"
fi

echo
echo "============================================"
echo "   PIPELINE INICIADO COM SUCESSO!"
echo "============================================"
