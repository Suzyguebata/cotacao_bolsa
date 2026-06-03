
from fastapi import FastAPI
from fastapi import HTTPException
import requests
from kafka import KafkaProducer
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

    kafka_producer = get_producer()
    kafka_producer.send(KAFKA_TOPIC, data)
    kafka_producer.flush()

    return {"status": "enviado", "ticker": ticker}
