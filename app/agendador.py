from apscheduler.schedulers.blocking import BlockingScheduler
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import requests

from observability.logging_utils import configure_json_logging, log_event


DEFAULT_TICKERS = (
    "PETR4,VALE3,ITUB4,BBAS3,MGLU3,"
    "BBDC4,ABEV3,WEGE3,RENT3,SUZB3,B3SA3,VIVT3,EQTL3,LREN3,AXIA3"
)
ATIVOS = [
    ticker.strip().upper()
    for ticker in os.getenv("MARKET_DATA_TICKERS", DEFAULT_TICKERS).split(",")
    if ticker.strip()
]
API_URL = os.getenv("MARKET_DATA_API_URL", "http://localhost:8000/coletar/{}")
POLL_INTERVAL_MINUTES = int(os.getenv("MARKET_DATA_POLL_INTERVAL_MINUTES", "5"))
# Precisa superar o pior caso da API para não registrar falha em coletas publicadas:
# Brapi v2 + fallback v1 (~10s cada) + Kafka (10s max_block + 15s ack) ≈ 45s.
API_TIMEOUT_SECONDS = int(os.getenv("MARKET_DATA_API_TIMEOUT_SECONDS", "60"))
MAX_WORKERS = int(os.getenv("MARKET_DATA_MAX_WORKERS", "4"))
logger = configure_json_logging("agendador")


def coletar_ticker(ticker):
    """Dispara a coleta de um ticker na API. Retorna True em caso de sucesso."""
    try:
        url = API_URL.format(ticker)
        inicio_requisicao = time.perf_counter()
        resposta = requests.get(url, timeout=API_TIMEOUT_SECONDS)
        duracao_ms = round((time.perf_counter() - inicio_requisicao) * 1000, 2)

        if resposta.status_code == 200:
            print(f"Coletando {ticker} ... OK ({duracao_ms} ms)")
            log_event(
                logger,
                logging.INFO,
                "ticker_collection_succeeded",
                ticker=ticker,
                status_code=resposta.status_code,
                duracao_ms=duracao_ms,
            )
            return True

        print(f"Coletando {ticker} ... ERRO ({resposta.status_code})")
        log_event(
            logger,
            logging.WARNING,
            "ticker_collection_failed",
            ticker=ticker,
            status_code=resposta.status_code,
            duracao_ms=duracao_ms,
        )
    except Exception as exc:
        print(f"Falha ao coletar {ticker}: {exc}")
        log_event(logger, logging.ERROR, "ticker_collection_unexpected_error", ticker=ticker, erro=str(exc))
    return False


def executar_coleta():
    inicio_ciclo = time.perf_counter()
    print("\n====================================")
    print(f"Execucao iniciada as {datetime.now()}")
    print("====================================")
    log_event(
        logger,
        logging.INFO,
        "scheduler_cycle_started",
        ativos=ATIVOS,
        intervalo_minutos=POLL_INTERVAL_MINUTES,
        paralelismo=MAX_WORKERS,
    )

    # Paralelismo limitado: mantém o ciclo bem abaixo do intervalo mesmo com 15 tickers
    # e timeouts no pior caso, sem sobrecarregar a Brapi.
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        resultados = list(executor.map(coletar_ticker, ATIVOS))

    sucessos = sum(resultados)
    falhas = len(resultados) - sucessos
    duracao_ciclo_ms = round((time.perf_counter() - inicio_ciclo) * 1000, 2)
    print(f"Execucao finalizada: {sucessos} sucesso(s), {falhas} falha(s) em {duracao_ciclo_ms} ms.")
    print("====================================\n")
    log_event(
        logger,
        logging.INFO,
        "scheduler_cycle_finished",
        ativos=ATIVOS,
        sucessos=sucessos,
        falhas=falhas,
        duracao_ms=duracao_ciclo_ms,
    )

    if duracao_ciclo_ms > POLL_INTERVAL_MINUTES * 60 * 1000:
        log_event(
            logger,
            logging.WARNING,
            "scheduler_cycle_overrun",
            duracao_ms=duracao_ciclo_ms,
            intervalo_minutos=POLL_INTERVAL_MINUTES,
        )
    return sucessos, falhas


def criar_agendador():
    agendador = BlockingScheduler()
    agendador.add_job(
        executar_coleta,
        "interval",
        minutes=POLL_INTERVAL_MINUTES,
        next_run_time=datetime.now(),
    )
    return agendador


def main():
    agendador = criar_agendador()
    print(f"Agendador iniciado. Coletando {', '.join(ATIVOS)} a cada {POLL_INTERVAL_MINUTES} minuto(s)...")
    log_event(logger, logging.INFO, "scheduler_started", ativos=ATIVOS, intervalo_minutos=POLL_INTERVAL_MINUTES)
    agendador.start()


if __name__ == "__main__":
    main()
