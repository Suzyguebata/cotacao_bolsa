@echo off
title Pipeline Local B3

echo ============================================
echo   INICIANDO PIPELINE LOCAL B3
echo ============================================
echo.

REM --- 1. Subir Docker Compose
echo [1/4] Subindo Docker Compose...
docker-compose up -d --build
echo.

REM --- 2. Executar Testes Unitários no Docker
echo [2/4] Executando Testes Unitários (Spark Transformation)...
docker exec -u root app-spark-bronze-1 /usr/bin/python3 -m pytest /app/tests/test_spark_logic.py
if %ERRORLEVEL% NEQ 0 (
    echo [ERRO] Os testes unitários falharam! Verifique a lógica do Spark.
    pause
    exit /b %ERRORLEVEL%
)
echo Testes aprovados!
echo.

REM --- 3. Informar serviços conteinerizados
echo [3/4] API e Scheduler iniciados pelo Docker Compose.
echo FastAPI Docs: http://localhost:8000/docs
echo.

REM --- 4. Abrir MinIO no navegador
echo [4/4] Abrindo MinIO Console...
start http://localhost:9001
echo.

echo ============================================
echo   PIPELINE INICIADO COM SUCESSO!
echo ============================================
pause
