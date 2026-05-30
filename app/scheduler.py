from apscheduler.schedulers.blocking import BlockingScheduler
import requests
from datetime import datetime

# Lista de ativos que você quer coletar
ATIVOS = [
    "PETR4",
    "VALE3",
    "ITUB4",
    "BBAS3",
    "MGLU3"
]

API_URL = "http://localhost:8000/coletar/{}"

scheduler = BlockingScheduler()

def coletar_ativos():
    print("\n====================================")
    print(f"Execução iniciada às {datetime.now()}")
    print("====================================")

    for ticker in ATIVOS:
        try:
            url = API_URL.format(ticker)
            print(f"→ Coletando {ticker} ... ", end="")
            response = requests.get(url)

            if response.status_code == 200:
                print("OK")
            else:
                print(f"ERRO ({response.status_code})")

        except Exception as e:
            print(f"Falha ao coletar {ticker}: {e}")

    print("Execução finalizada.")
    print("====================================\n")

# Executa a cada 1 minuto
scheduler.add_job(coletar_ativos, "interval", minutes=1)

print("Scheduler iniciado. Coletando ativos periodicamente...")
scheduler.start()