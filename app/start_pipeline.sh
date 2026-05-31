#!/bin/bash

echo "============================================"
echo "   INICIANDO PIPELINE LOCAL B3 (Docker)"
echo "============================================"
echo

# 1. Subir Docker Compose
echo "[1/7] Subindo Docker Compose..."
docker-compose up -d
echo

# 2. Ativar ambiente virtual
echo "[2/7] Ativando ambiente virtual..."
if [ -f "venv/Scripts/activate" ]; then
    source venv/Scripts/activate
elif [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
else
    echo "Ambiente virtual não encontrado. Criando..."
    python -m venv venv
    source venv/Scripts/activate 2>/dev/null || source venv/bin/activate
fi
echo "Ambiente virtual ativado."
echo

# 3. Instalar dependências
echo "[3/8] Instalando dependências..."
pip install -r requirements.txt
echo

# 4. Executar Testes Unitários no Docker
echo "[4/8] Executando Testes Unitários (Spark Transformation)..."
docker exec -u root app-spark-bronze-1 pip install pytest --quiet
docker exec app-spark-bronze-1 pytest /app/tests/test_spark_logic.py
if [ $? -ne 0 ]; then
    echo "[ERRO] Os testes unitários falharam! Verifique a lógica do Spark."
    exit 1
fi
echo "Testes aprovados!"
echo

# 5. Iniciar API FastAPI
echo "[5/7] Iniciando API (Lambda local)..."
( python -m uvicorn api.lambda_api:app --reload --port 8000 & )
sleep 2
echo "API iniciada na porta 8000."
echo

# 6. Iniciar Scheduler
echo "[6/7] Iniciando Scheduler..."
( python scheduler.py & )
sleep 2
echo "Scheduler iniciado."
echo

# 7. Abrir MinIO Console
echo "[7/7] Abrindo MinIO Console..."
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