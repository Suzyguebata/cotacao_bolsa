from datetime import datetime, timezone

from agendador import criar_agendador


def test_agendador_executa_coleta_imediatamente_ao_iniciar():
    agendador = criar_agendador()
    jobs = agendador.get_jobs()

    assert len(jobs) == 1
    assert jobs[0].trigger.interval.total_seconds() == 300

    agora = datetime.now(timezone.utc)
    atraso_segundos = abs((jobs[0].next_run_time - agora).total_seconds())
    assert atraso_segundos < 5
