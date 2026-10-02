from datetime import datetime, timezone

import agendador
from agendador import criar_agendador


def test_agendador_executa_coleta_imediatamente_ao_iniciar():
    agendador = criar_agendador()
    jobs = agendador.get_jobs()

    assert len(jobs) == 1
    assert jobs[0].trigger.interval.total_seconds() == 300

    agora = datetime.now(timezone.utc)
    atraso_segundos = abs((jobs[0].next_run_time - agora).total_seconds())
    assert atraso_segundos < 5


class _RespostaFalsa:
    def __init__(self, status_code):
        self.status_code = status_code


def test_executar_coleta_contabiliza_sucessos_e_falhas(monkeypatch):
    status_por_ticker = {"PETR4": 200, "VALE3": 502, "ITUB4": 200}
    urls_chamadas = []

    def get_falso(url, timeout):
        urls_chamadas.append(url)
        ticker = url.rsplit("/", 1)[-1]
        if ticker == "ITUB4":
            raise agendador.requests.Timeout("lento")
        return _RespostaFalsa(status_por_ticker[ticker])

    monkeypatch.setattr(agendador, "ATIVOS", list(status_por_ticker))
    monkeypatch.setattr(agendador.requests, "get", get_falso)

    sucessos, falhas = agendador.executar_coleta()

    assert (sucessos, falhas) == (1, 2)
    assert sorted(urls_chamadas) == sorted(agendador.API_URL.format(t) for t in status_por_ticker)


def test_timeout_do_agendador_supera_pior_caso_da_api():
    from api import api_ingestao

    pior_caso_brapi = 2 * sum(api_ingestao.BRAPI_TIMEOUT_SECONDS)  # v2 + fallback v1
    pior_caso_kafka = 10 + api_ingestao.KAFKA_PUBLISH_TIMEOUT_SECONDS  # max_block_ms + ack
    assert agendador.API_TIMEOUT_SECONDS > pior_caso_brapi + pior_caso_kafka
