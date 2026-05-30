@echo off
title Pipeline Local B3

echo ============================================
echo   INICIANDO PIPELINE LOCAL B3
echo ============================================
echo.

REM --- 1. Subir Docker (Kafka + MinIO)
echo [1/7] Subindo Docker Compose...
docker-compose up -d
echo.

REM --- 2. Ativar ambiente virtual
echo [2/7] Ativando ambiente virtual...
call venv\Scripts\activate
echo Ambiente virtual ativado.
echo.

REM --- 3. Instalar dependências (se necessário)
echo [3/7] Instalando dependências...
pip install -r requirements.txt
echo.

REM --- 4. Iniciar API FastAPI
echo [4/7] Iniciando API (Lambda local)...
start cmd /k "python -m uvicorn api.lambda_api:app --reload --port 8000"
echo API iniciada na porta 8000.
echo.

REM --- 5. Iniciar Consumer Kafka
echo [5/7] Iniciando Consumer Kafka...
start cmd /k "python consumer\consumer.py"
echo Consumer iniciado.
echo.

REN --- 6. Iniciar Scheduler
echo [6/7] Iniciando Scheduler..
start cmd /k "python scheduler.py"
echo Scheduler iniciado.
echo.

REM --- 7. Abrir MinIO no navegador
echo [7/7] Abrindo MinIO Console...
start http://localhost:9001
echo.

echo ============================================
echo   PIPELINE INICIADO COM SUCESSO!
echo ============================================
pause
