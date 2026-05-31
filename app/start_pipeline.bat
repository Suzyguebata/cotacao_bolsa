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
echo [3/8] Instalando dependências...
pip install -r requirements.txt
echo.

REM --- 4. Executar Testes Unitários no Docker
echo [4/8] Executando Testes Unitários (Spark Transformation)...
docker exec -u root app-spark-bronze-1 pip install pytest --quiet
docker exec app-spark-bronze-1 pytest /app/tests/test_spark_logic.py
if %ERRORLEVEL% NEQ 0 (
    echo [ERRO] Os testes unitários falharam! Verifique a lógica do Spark.
    pause
    exit /b %ERRORLEVEL%
)
echo Testes aprovados!
echo.

REM --- 5. Iniciar API FastAPI
echo [5/7] Iniciando API (Lambda local)...
start cmd /k "python -m uvicorn api.lambda_api:app --reload --port 8000"
echo API iniciada na porta 8000.
echo.

REM --- 6. Iniciar Scheduler
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
