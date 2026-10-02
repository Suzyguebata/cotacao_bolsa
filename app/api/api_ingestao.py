
from fastapi import FastAPI
from fastapi import HTTPException
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
from pydantic import BaseModel, ValidationError
import json
import logging
import os
import re
from typing import Any, Dict, List, Optional

from observability.logging_utils import configure_json_logging, log_event
from prometheus_fastapi_instrumentator import Instrumentator

app = FastAPI()
Instrumentator().instrument(app).expose(app)

produtor = None
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "cotacoes")
BRAPI_TIMEOUT_SECONDS = 10
KAFKA_PUBLISH_TIMEOUT_SECONDS = 15
# Ações/FIIs/BDRs (PETR4, BOVA11, AAPL34) e índices (^BVSP); bloqueia "/", "?" etc. na URL da Brapi.
TICKER_PATTERN = re.compile(r"\^?[A-Z0-9]{3,12}")
logger = configure_json_logging("api-ingestao")


class CotacaoBrapi(BaseModel):
    symbol: Optional[str] = None
    shortName: Optional[str] = None
    longName: Optional[str] = None
    currency: Optional[str] = None
    regularMarketPrice: Optional[float] = None
    regularMarketDayHigh: Optional[float] = None
    regularMarketDayLow: Optional[float] = None
    regularMarketDayRange: Optional[str] = None
    regularMarketChange: Optional[float] = None
    regularMarketChangePercent: Optional[float] = None
    regularMarketTime: Optional[str] = None
    marketCap: Optional[float] = None
    regularMarketVolume: Optional[int] = None
    regularMarketPreviousClose: Optional[float] = None
    regularMarketOpen: Optional[float] = None
    fiftyTwoWeekRange: Optional[str] = None
    fiftyTwoWeekLow: Optional[float] = None
    fiftyTwoWeekHigh: Optional[float] = None
    priceEarnings:  Optional[float] = None
    earningsPerShare: Optional[float] = None
    logourl: Optional[str] = None


class RespostaBrapi(BaseModel):
    results: List[CotacaoBrapi]
    requestedAt: Optional[str] = None
    took: Optional[int] = None


def _modelo_para_dict(modelo: BaseModel) -> Dict[str, Any]:
    if hasattr(modelo, "model_dump"):
        return modelo.model_dump()
    return modelo.dict()


def validar_payload_brapi(dados: Dict[str, Any]) -> Dict[str, Any]:
    try:
        payload = RespostaBrapi(**dados)
    except ValidationError as exc:
        log_event(logger, logging.WARNING, "brapi_schema_validation_failed", erro=str(exc))
        raise HTTPException(status_code=502, detail="Resposta da Brapi fora do schema esperado") from exc

    if not payload.results:
        log_event(logger, logging.WARNING, "brapi_empty_results")
        raise HTTPException(status_code=502, detail="Resposta da Brapi sem cotações")

    return _modelo_para_dict(payload)


def obter_produtor():
    global produtor
    if produtor is None:
        produtor = KafkaProducer(
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
    return produtor


def normalizar_ticker(ticker: str) -> str:
    ticker_normalizado = ticker.strip().upper()
    if not TICKER_PATTERN.fullmatch(ticker_normalizado):
        log_event(logger, logging.WARNING, "invalid_ticker_rejected", ticker=ticker)
        raise HTTPException(status_code=400, detail="Ticker inválido")
    return ticker_normalizado


def consultar_brapi(ticker: str) -> Dict[str, Any]:
    url = f"https://brapi.dev/api/quote/{ticker}"
    params = {}
    token = os.getenv("BRAPI_TOKEN")
    if token:
        params["token"] = token

    try:
        resposta = requests.get(url, params=params, timeout=BRAPI_TIMEOUT_SECONDS)
        resposta.raise_for_status()
        dados = resposta.json()
        log_event(logger, logging.INFO, "brapi_request_succeeded", ticker=ticker, status_code=resposta.status_code)
    except requests.RequestException as exc:
        log_event(logger, logging.WARNING, "brapi_request_failed", ticker=ticker, erro=str(exc))
        raise HTTPException(status_code=502, detail="Falha ao consultar Brapi") from exc
    except ValueError as exc:
        log_event(logger, logging.ERROR, "brapi_invalid_json", ticker=ticker, erro=str(exc))
        raise HTTPException(status_code=502, detail="Resposta inválida da Brapi") from exc

    return dados


def publicar_cotacao(ticker: str, dados: Dict[str, Any]) -> None:
    """Valida o payload da Brapi e publica no Kafka.

    Ponto único de entrada no Kafka: usado pela coleta via trigger (/coletar)
    e reutilizável por um futuro endpoint de webhook da Brapi.
    """
    dados_validados = validar_payload_brapi(dados)

    try:
        kafka_producer = obter_produtor()
        future = kafka_producer.send(KAFKA_TOPIC, key=ticker, value=dados_validados)
        future.get(timeout=KAFKA_PUBLISH_TIMEOUT_SECONDS)
        log_event(logger, logging.INFO, "kafka_publish_succeeded", ticker=ticker, topic=KAFKA_TOPIC)
    except KafkaError as exc:
        log_event(logger, logging.ERROR, "kafka_publish_failed", ticker=ticker, topic=KAFKA_TOPIC, erro=str(exc))
        raise HTTPException(status_code=503, detail="Falha ao publicar no Kafka") from exc
    except Exception as exc:
        log_event(logger, logging.ERROR, "kafka_publish_unexpected_error", ticker=ticker, topic=KAFKA_TOPIC, erro=str(exc))
        raise HTTPException(status_code=503, detail="Falha inesperada ao publicar no Kafka") from exc


@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/coletar/{ticker}")
def coletar(ticker: str):
    ticker_normalizado = normalizar_ticker(ticker)
    log_event(logger, logging.INFO, "market_data_collection_started", ticker=ticker_normalizado)

    dados = consultar_brapi(ticker_normalizado)
    publicar_cotacao(ticker_normalizado, dados)

    return {"status": "enviado", "ticker": ticker_normalizado}
