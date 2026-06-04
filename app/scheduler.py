from apscheduler.schedulers.blocking import BlockingScheduler
import logging
import os
from datetime import datetime

import requests

from observability.logging_utils import configure_json_logging, log_event


DEFAULT_TICKERS = "PETR4,VALE3,ITUB4,BBAS3,MGLU3"
ATIVOS = [
    ticker.strip().upper()
    for ticker in os.getenv("MARKET_DATA_TICKERS", DEFAULT_TICKERS).split(",")
    if ticker.strip()
]
API_URL = os.getenv("MARKET_DATA_API_URL", "http://localhost:8000/coletar/{}")
POLL_INTERVAL_MINUTES = int(os.getenv("MARKET_DATA_POLL_INTERVAL_MINUTES", "5"))
logger = configure_json_logging("scheduler")


def coletar_ativos():
    print("\n====================================")
    print(f"Execucao iniciada as {datetime.now()}")
    print("====================================")
    log_event(logger, logging.INFO, "scheduler_cycle_started", tickers=ATIVOS, interval_minutes=POLL_INTERVAL_MINUTES)

    for ticker in ATIVOS:
        try:
            url = API_URL.format(ticker)
            print(f"Coletando {ticker} ... ", end="")
            response = requests.get(url, timeout=15)

            if response.status_code == 200:
                print("OK")
                log_event(logger, logging.INFO, "ticker_collection_succeeded", ticker=ticker, status_code=response.status_code)
            else:
                print(f"ERRO ({response.status_code})")
                log_event(logger, logging.WARNING, "ticker_collection_failed", ticker=ticker, status_code=response.status_code)

        except Exception as exc:
            print(f"Falha ao coletar {ticker}: {exc}")
            log_event(logger, logging.ERROR, "ticker_collection_unexpected_error", ticker=ticker, error=str(exc))

    print("Execucao finalizada.")
    print("====================================\n")
    log_event(logger, logging.INFO, "scheduler_cycle_finished", tickers=ATIVOS)


def create_scheduler():
    scheduler = BlockingScheduler()
    scheduler.add_job(
        coletar_ativos,
        "interval",
        minutes=POLL_INTERVAL_MINUTES,
        next_run_time=datetime.now(),
    )
    return scheduler


def main():
    scheduler = create_scheduler()
    print(f"Scheduler iniciado. Coletando {', '.join(ATIVOS)} a cada {POLL_INTERVAL_MINUTES} minuto(s)...")
    log_event(logger, logging.INFO, "scheduler_started", tickers=ATIVOS, interval_minutes=POLL_INTERVAL_MINUTES)
    scheduler.start()


if __name__ == "__main__":
    main()
