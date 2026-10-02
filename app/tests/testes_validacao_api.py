import pytest
from fastapi import HTTPException

from api import api_ingestao
from api.api_ingestao import normalizar_ticker, publicar_cotacao, validar_payload_brapi


def test_validar_payload_brapi_normaliza_campos_conhecidos():
    dados = {
       "results": [
        {
            "symbol": "PETR4",
            "shortName": "PETR4",
            "longName": "Petroleo Brasileiro SA Pfd",
            "currency": "BRL",
            "regularMarketPrice": 41.25,
            "regularMarketDayHigh": 41.87,
            "regularMarketDayLow": 41.25,
            "regularMarketDayRange": "41.25 - 41.87",
            "regularMarketChange": -0.32,
            "regularMarketChangePercent": -0.77,
            "regularMarketTime": "2026-06-04T21:30:30.000Z",
            "marketCap": 561060286488,
            "regularMarketVolume": 42895100,
            "regularMarketPreviousClose": 41.39,
            "regularMarketOpen": 41.65,
            "fiftyTwoWeekRange": "28.86 - 50.69",
            "fiftyTwoWeekLow": 28.86,
            "fiftyTwoWeekHigh": 50.69,
            "priceEarnings": 4.941836086784632,
            "earningsPerShare": 8.347058,
            "logourl": "https://icons.brapi.dev/icons/PETR4.svg"
        }
    ],
    "requestedAt": "2026-06-04T23:27:36.615Z",
    "took": 1
}

    payload = validar_payload_brapi(dados)

    assert payload == {
        "results": [
        {
            "symbol": "PETR4",
            "shortName": "PETR4",
            "longName": "Petroleo Brasileiro SA Pfd",
            "currency": "BRL",
            "regularMarketPrice": 41.25,
            "regularMarketDayHigh": 41.87,
            "regularMarketDayLow": 41.25,
            "regularMarketDayRange": "41.25 - 41.87",
            "regularMarketChange": -0.32,
            "regularMarketChangePercent": -0.77,
            "regularMarketTime": "2026-06-04T21:30:30.000Z",
            "marketCap": 561060286488,
            "regularMarketVolume": 42895100,
            "regularMarketPreviousClose": 41.39,
            "regularMarketOpen": 41.65,
            "fiftyTwoWeekRange": "28.86 - 50.69",
            "fiftyTwoWeekLow": 28.86,
            "fiftyTwoWeekHigh": 50.69,
            "priceEarnings": 4.941836086784632,
            "earningsPerShare": 8.347058,
            "logourl": "https://icons.brapi.dev/icons/PETR4.svg"
        }
    ],
    "requestedAt": "2026-06-04T23:27:36.615Z",
    "took": 1
}


def test_validar_payload_brapi_rejeita_resultados_vazios():
    with pytest.raises(HTTPException) as exc:
        validar_payload_brapi({"results": []})

    assert exc.value.status_code == 502
    assert "sem cotações" in exc.value.detail


def test_validar_payload_brapi_rejeita_schema_invalido():
    with pytest.raises(HTTPException) as exc:
        validar_payload_brapi({"results": "PETR4"})

    assert exc.value.status_code == 502
    assert "schema esperado" in exc.value.detail


@pytest.mark.parametrize("ticker", ["petr4", " VALE3 ", "BOVA11", "AAPL34", "^BVSP"])
def test_normalizar_ticker_aceita_formatos_validos(ticker):
    assert normalizar_ticker(ticker) == ticker.strip().upper()


@pytest.mark.parametrize("ticker", ["", "PE", "PETR4/../x", "PETR4?token=x", "PETR4,VALE3"])
def test_normalizar_ticker_rejeita_formatos_invalidos(ticker):
    with pytest.raises(HTTPException) as exc:
        normalizar_ticker(ticker)

    assert exc.value.status_code == 400


class _FutureFalso:
    def get(self, timeout):
        return None


class _ProdutorFalso:
    def __init__(self):
        self.enviados = []

    def send(self, topico, key, value):
        self.enviados.append((topico, key, value))
        return _FutureFalso()


def test_publicar_cotacao_envia_payload_validado_chaveado_por_ticker(monkeypatch):
    produtor = _ProdutorFalso()
    monkeypatch.setattr(api_ingestao, "obter_produtor", lambda: produtor)

    publicar_cotacao("PETR4", {"results": [{"symbol": "PETR4", "regularMarketPrice": 41.25}]})

    assert len(produtor.enviados) == 1
    topico, chave, valor = produtor.enviados[0]
    assert topico == api_ingestao.KAFKA_TOPIC
    assert chave == "PETR4"
    assert valor["results"][0]["regularMarketPrice"] == 41.25


def test_publicar_cotacao_nao_publica_payload_invalido(monkeypatch):
    produtor = _ProdutorFalso()
    monkeypatch.setattr(api_ingestao, "obter_produtor", lambda: produtor)

    with pytest.raises(HTTPException):
        publicar_cotacao("PETR4", {"results": []})

    assert produtor.enviados == []
