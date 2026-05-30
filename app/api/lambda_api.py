
from fastapi import FastAPI
import requests
from kafka import KafkaProducer
import json

app = FastAPI()

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

@app.get("/coletar/{ticker}")
def coletar(ticker: str):
    url = f"https://brapi.dev/api/quote/{ticker}"
    data = requests.get(url).json()

    producer.send("cotacoes", data)
    producer.flush()

    return {"status": "enviado", "ticker": ticker}
