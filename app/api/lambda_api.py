
from fastapi import FastAPI
from fastapi import HTTPException
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
from pydantic import BaseModel, ValidationError
import json
import logging
import os
from typing import Any, Dict, List, Optional

from observability.logging_utils import configure_json_logging, log_event

app = FastAPI()

producer = None
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "cotacoes")
logger = configure_json_logging("api")


class BrapiQuote(BaseModel):
    symbol: Optional[str] = None
    regularMarketPrice: Optional[float] = None
    regularMarketTime: Optional[str] = None
    regularMarketChange: Optional[float] = None
    marketCap: Optional[float] = None


class BrapiResponse(BaseModel):
    results: List[BrapiQuote]
    requestedAt: Optional[str] = None


def _model_to_dict(model: BaseModel) -> Dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


def validate_brapi_payload(data: Dict[str, Any]) -> Dict[str, Any]:
    try:
        payload = BrapiResponse(**data)
    except ValidationError as exc:
        log_event(logger, logging.WARNING, "brapi_schema_validation_failed", error=str(exc))
        raise HTTPException(status_code=502, detail=f"Resposta da Brapi fora do schema esperado: {exc}") from exc

    if not payload.results:
        log_event(logger, logging.WARNING, "brapi_empty_results")
        raise HTTPException(status_code=502, detail="Resposta da Brapi sem cotações")

    return _model_to_dict(payload)


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
    normalized_ticker = ticker.upper()
    log_event(logger, logging.INFO, "market_data_collection_started", ticker=normalized_ticker)

    url = f"https://brapi.dev/api/quote/{ticker}"
    params = {}
    token = os.getenv("BRAPI_TOKEN")
    if token:
        params["token"] = token

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        log_event(logger, logging.INFO, "brapi_request_succeeded", ticker=normalized_ticker, status_code=response.status_code)
    except requests.RequestException as exc:
        log_event(logger, logging.ERROR, "brapi_request_failed", ticker=normalized_ticker, error=str(exc))
        raise HTTPException(status_code=502, detail=f"Falha ao consultar Brapi: {exc}") from exc
    except ValueError as exc:
        log_event(logger, logging.ERROR, "brapi_invalid_json", ticker=normalized_ticker, error=str(exc))
        raise HTTPException(status_code=502, detail="Resposta inválida da Brapi") from exc

    validated_data = validate_brapi_payload(data)

    try:
        kafka_producer = get_producer()
        future = kafka_producer.send(KAFKA_TOPIC, key=normalized_ticker, value=validated_data)
        future.get(timeout=15)
        log_event(logger, logging.INFO, "kafka_publish_succeeded", ticker=normalized_ticker, topic=KAFKA_TOPIC)
    except KafkaError as exc:
        log_event(logger, logging.ERROR, "kafka_publish_failed", ticker=normalized_ticker, topic=KAFKA_TOPIC, error=str(exc))
        raise HTTPException(status_code=503, detail=f"Falha ao publicar no Kafka: {exc}") from exc
    except Exception as exc:
        log_event(logger, logging.ERROR, "kafka_publish_unexpected_error", ticker=normalized_ticker, topic=KAFKA_TOPIC, error=str(exc))
        raise HTTPException(status_code=503, detail=f"Falha inesperada ao publicar no Kafka: {exc}") from exc

    return {"status": "enviado", "ticker": normalized_ticker}
