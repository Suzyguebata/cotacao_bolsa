@echo off
setlocal enabledelayedexpansion

if "%TRINO_REGISTER_MAX_ATTEMPTS%"=="" set TRINO_REGISTER_MAX_ATTEMPTS=18
if "%TRINO_REGISTER_SLEEP_SECONDS%"=="" set TRINO_REGISTER_SLEEP_SECONDS=20

echo ============================================
echo   REGISTRANDO TABELAS DELTA NO TRINO
echo ============================================
echo.

docker compose up -d trino

for /f %%i in ('docker compose ps -q trino') do set TRINO_CONTAINER=%%i
if "%TRINO_CONTAINER%"=="" (
    echo [ERRO] Container do Trino nao encontrado.
    exit /b 1
)

call :run_with_retry "Criar schema bronze" "CREATE SCHEMA IF NOT EXISTS delta.bronze"
if errorlevel 1 exit /b 1
call :run_with_retry "Criar schema silver" "CREATE SCHEMA IF NOT EXISTS delta.silver"
if errorlevel 1 exit /b 1
call :run_with_retry "Criar schema gold" "CREATE SCHEMA IF NOT EXISTS delta.gold"
if errorlevel 1 exit /b 1

call :run_with_retry "Registrar delta.bronze.cotacoes" "CALL delta.system.register_table('bronze', 'cotacoes', 's3a://datalake/bronze/cotacoes')"
if errorlevel 1 exit /b 1
call :run_with_retry "Registrar delta.silver.cotacoes" "CALL delta.system.register_table('silver', 'cotacoes', 's3a://datalake/silver/cotacoes')"
if errorlevel 1 exit /b 1
call :run_with_retry "Registrar delta.gold.media_precos_5min" "CALL delta.system.register_table('gold', 'media_precos_5min', 's3a://datalake/gold/media_precos_5min')"
if errorlevel 1 exit /b 1

echo Tabelas disponiveis no Trino:
docker exec "%TRINO_CONTAINER%" trino --execute "SHOW TABLES FROM delta.bronze"
docker exec "%TRINO_CONTAINER%" trino --execute "SHOW TABLES FROM delta.silver"
docker exec "%TRINO_CONTAINER%" trino --execute "SHOW TABLES FROM delta.gold"

echo.
echo Registro concluido.
exit /b 0

:run_with_retry
set LABEL=%~1
set SQL=%~2
set ATTEMPT=1

:retry_loop
echo [!ATTEMPT!/%TRINO_REGISTER_MAX_ATTEMPTS%] %LABEL%
docker exec "%TRINO_CONTAINER%" trino --execute "%SQL%"
if not errorlevel 1 (
    echo OK: %LABEL%
    echo.
    goto :eof
)

if !ATTEMPT! geq %TRINO_REGISTER_MAX_ATTEMPTS% (
    echo [ERRO] Falha ao executar: %LABEL%
    exit /b 1
)

echo Aguardando %TRINO_REGISTER_SLEEP_SECONDS% segundo(s) antes de tentar novamente...
echo.
timeout /t %TRINO_REGISTER_SLEEP_SECONDS% /nobreak >nul
set /a ATTEMPT+=1
goto :retry_loop
