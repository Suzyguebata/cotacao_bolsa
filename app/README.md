# Brapi Data Pipeline (Medallion Architecture)

Este projeto demonstra um pipeline de dados em tempo real utilizando a API da Brapi, Kafka, Spark Streaming, Delta Lake, MinIO e Trino.

## 🚀 Arquitetura
O pipeline segue o padrão Medallion:
- **Bronze**: Dados crus vindos do Kafka.
- **Silver**: Dados limpos e tipados, particionados por ticker e data.
- **Gold**: Agregações de negócio (médias de preço em janelas de 5 min).

## 🛠️ Tecnologias
- **FastAPI**: Ingestão de dados.
- **Kafka**: Mensageria.
- **Spark Streaming**: Processamento.
- **Delta Lake**: Formato de tabela ACID.
- **MinIO**: Object Storage (S3 local).
- **Trino**: Motor de consulta SQL.

## 🏃 Como Rodar
1. Subir infra: `docker-compose up -d`
2. Instalar deps: `pip install -r requirements.txt`
3. Iniciar API: `uvicorn api.lambda_api:app --port 8000`
4. Iniciar Scheduler: `python scheduler.py`

## 🧪 Guia de Testes e Evidências

Este guia descreve os passos realizados para validar a arquitetura Medallion e como extrair evidências de sucesso.

### 1. Preparação do Ambiente
Certifique-se de que a infraestrutura está ativa:
```bash
cd app
docker-compose up -d
pip install -r requirements.txt
```

### 2. Execução do Fluxo de Dados
... (passos anteriores) ...

### 2.1 Execução de Testes Unitários (Docker)
Como o Spark depende do Java, a forma mais garantida de rodar os testes unitários é dentro do container:
```bash
# Instala o pytest no container (apenas uma vez)
docker exec -u root app-spark-bronze-1 pip install pytest

# Executa os testes de lógica do Spark
docker exec app-spark-bronze-1 pytest /app/tests/test_spark_logic.py
```

### 3. Como Evidenciar os Resultados (via Trino)
Para comprovar que o sistema está funcionando, execute as queries de monitoramento:

*   **Acesso ao Trino**:
    ```bash
    docker exec -it app-trino-1 trino
    ```

*   **Queries de Evidência (Disponíveis em `queries/relatorio_tcc_metricas.sql`)**:
    *   **Volume por Camada**: Comprova que os dados estão fluindo da Bronze para a Silver.
    *   **Throughput**: Mostra a capacidade de processamento por minuto.
    *   **Integridade de Dados**: Verifica se os preços e tickers foram transformados corretamente.

### 4. Resultados Obtidos nos Testes
Durante a fase de validação, os seguintes marcos foram alcançados:
*   **Conectividade**: Kafka, Spark e MinIO integrados sem perda de pacotes.
*   **ACID Compliance**: Tabelas Delta registradas no Trino com suporte total a metadados.
*   **Desempenho**: Processamento médio de 10-14 registros/minuto em ambiente local simulado.
*   **Agregação**: Camada Gold gerando métricas de média móvel em janelas temporais de 1 minuto.

---
*Documentação gerada automaticamente para suporte ao TCC e apresentações técnicas.*
