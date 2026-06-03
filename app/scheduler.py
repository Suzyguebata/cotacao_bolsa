from apscheduler.schedulers.blocking import BlockingScheduler
import requests
from datetime import datetime
import os

DEFAULT_TICKERS = "PETR4,VALE3,ITUB4,BBAS3,MGLU3"
ATIVOS = [
    ticker.strip().upper()
    for ticker in os.getenv("MARKET_DATA_TICKERS", DEFAULT_TICKERS).split(",")
    if ticker.strip()
]
API_URL = os.getenv("MARKET_DATA_API_URL", "http://localhost:8000/coletar/{}")
POLL_INTERVAL_MINUTES = int(os.getenv("MARKET_DATA_POLL_INTERVAL_MINUTES", "5"))

def coletar_ativos():
    print("\n====================================")
    print(f"Execução iniciada às {datetime.now()}")
    print("====================================")

    for ticker in ATIVOS:
        try:
            url = API_URL.format(ticker)
            print(f"→ Coletando {ticker} ... ", end="")
            response = requests.get(url, timeout=15)

            if response.status_code == 200:
                print("OK")
            else:
                print(f"ERRO ({response.status_code})")

        except Exception as e:
            print(f"Falha ao coletar {ticker}: {e}")

    print("Execução finalizada.")
    print("====================================\n")

def main():
    scheduler = BlockingScheduler()
    # Executa em intervalo configurável. O padrão de 5 minutos fica alinhado à coleta
    # final planejada com Brapi Pro; no Free, os dados podem continuar defasados.
    scheduler.add_job(coletar_ativos, "interval", minutes=POLL_INTERVAL_MINUTES)
    print(f"Scheduler iniciado. Coletando {', '.join(ATIVOS)} a cada {POLL_INTERVAL_MINUTES} minuto(s)...")
    scheduler.start()


if __name__ == "__main__":
    main()
