import pytest
import requests
import subprocess
import time

# Configurações
API_BASE_URL = "http://localhost:8000"
TRINO_CONTAINER = "app-trino-1"

def test_api_status():
    """Verifica se a API está online"""
    try:
        response = requests.get(f"{API_BASE_URL}/docs")
        assert response.status_code == 200
    except requests.exceptions.ConnectionError:
        pytest.fail("A API FastAPI não está rodando. Execute: uvicorn api.lambda_api:app --port 8000")

def test_ingestion_to_silver():
    """Teste de ponta a ponta: Ingestão API -> Bronze -> Silver"""
    ticker = "PETR4"
    
    # 1. Captura contagem atual na Silver para comparação
    def get_count(table):
        query = f"SELECT count(*) FROM delta.raw.{table} WHERE ticker = '{ticker}'"
        cmd = ["docker", "exec", TRINO_CONTAINER, "trino", "--execute", query, "--output-format", "CSV"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        try:
            return int(result.stdout.strip().replace('"', ''))
        except:
            return 0

    count_before = get_count("cotacoes_silver")
    
    # 2. Dispara coleta via API
    response = requests.get(f"{API_BASE_URL}/coletar/{ticker}")
    assert response.status_code == 200
    
    # 3. Aguarda processamento (Spark Streaming + Delta Lake)
    # Streaming local pode levar alguns segundos para fechar o micro-batch
    print("\n[Integrado] Aguardando processamento do Spark...")
    time.sleep(20)
    
    # 4. Verifica se a contagem aumentou
    count_after = get_count("cotacoes_silver")
    
    assert count_after > count_before, f"O dado não chegou na camada Silver. Antes: {count_before}, Depois: {count_after}"
    print(f"\n[Sucesso] Fluxo completo validado para {ticker}!")

if __name__ == "__main__":
    # Permite rodar diretamente: python tests/test_integration.py
    print("Iniciando testes de integração...")
    try:
        test_api_status()
        print("- API: OK")
        test_ingestion_to_silver()
    except Exception as e:
        print(f"\n[ERRO] {e}")
