
from fastapi import FastAPI
from fastapi import HTTPException
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import os

app = FastAPI()

producer = None
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "cotacoes")


def get_producer():
    global producer
    if producer is None:
        producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            acks="all",
            retries=int(os.getenv("KAFKA_PRODUCER_RETRIES", "5")),
            retry_backoff_ms=int(os.getenv("KAFKA_PRODUCER_RETRY_BACKOFF_MS", "500")),
            linger_ms=int(os.getenv("KAFKA_PRODUCER_LINGER_MS", "50")),
            request_timeout_ms=int(os.getenv("KAFKA_PRODUCER_REQUEST_TIMEOUT_MS", "15000")),
            max_block_ms=int(os.getenv("KAFKA_PRODUCER_MAX_BLOCK_MS", "10000")),
            key_serializer=lambda v: v.encode("utf-8") if v is not None else None,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
    return producer


@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/coletar/{ticker}")
def coletar(ticker: str):
    url = f"https://brapi.dev/api/quote/{ticker}"
    params = {}
    token = os.getenv("BRAPI_TOKEN")
    if token:
        params["token"] = token

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"Falha ao consultar Brapi: {exc}") from exc
    except ValueError as exc:
        raise HTTPException(status_code=502, detail="Resposta inválida da Brapi") from exc

    if not data.get("results"):
        raise HTTPException(status_code=502, detail="Resposta da Brapi sem cotações")

    try:
        kafka_producer = get_producer()
        future = kafka_producer.send(KAFKA_TOPIC, key=ticker.upper(), value=data)
        future.get(timeout=15)
    except KafkaError as exc:
        raise HTTPException(status_code=503, detail=f"Falha ao publicar no Kafka: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Falha inesperada ao publicar no Kafka: {exc}") from exc

    return {"status": "enviado", "ticker": ticker}
