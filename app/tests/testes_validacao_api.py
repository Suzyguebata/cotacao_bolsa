import pytest
import requests
from fastapi import HTTPException

from api import api_ingestao
from api.api_ingestao import consultar_brapi, normalizar_ticker, publicar_cotacao, validar_payload_brapi


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
            "requestedSymbol": None,
            "changed": None,
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


def test_validar_payload_brapi_aceita_formato_v2():
    dados = {
        "results": [
            {
                "requestedSymbol": "PETR4",
                "symbol": "PETR4",
                "changed": False,
                "data": {
                    "symbol": "PETR4",
                    "regularMarketPrice": 41.25,
                    "currency": "BRL",
                    "shortName": "PETR4",
                    "longName": "Petroleo Brasileiro SA Pfd",
                    "regularMarketChangePercent": -0.77,
                },
            }
        ],
        "requestedAt": "2026-06-04T23:27:36.615Z",
        "took": 1,
    }

    payload = validar_payload_brapi(dados)

    assert payload["results"][0]["regularMarketPrice"] == 41.25
    assert payload["results"][0]["symbol"] == "PETR4"
    assert payload["results"][0]["requestedSymbol"] == "PETR4"
    assert payload["results"][0]["changed"] is False


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


class _RespostaFalsa:
    def __init__(self, status_code, corpo=None):
        self.status_code = status_code
        self._corpo = corpo or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            erro = requests.HTTPError(f"HTTP {self.status_code}")
            erro.response = self
            raise erro

    def json(self):
        return self._corpo


def _registrar_chamadas(monkeypatch, respostas):
    chamadas = []

    def get_falso(url, params=None, headers=None, timeout=None):
        chamadas.append({"url": url, "params": params, "headers": headers})
        resposta = respostas[len(chamadas) - 1]
        if isinstance(resposta, Exception):
            raise resposta
        return resposta

    monkeypatch.setattr(api_ingestao.requests, "get", get_falso)
    return chamadas


def test_consultar_brapi_usa_rota_v2_com_token_no_header(monkeypatch):
    monkeypatch.setenv("BRAPI_TOKEN", "segredo")
    chamadas = _registrar_chamadas(monkeypatch, [_RespostaFalsa(200, {"results": []})])

    consultar_brapi("PETR4")

    assert len(chamadas) == 1
    assert chamadas[0]["url"] == "https://brapi.dev/api/v2/stocks/quote"
    assert chamadas[0]["params"] == {"symbols": "PETR4"}
    assert chamadas[0]["headers"] == {"Authorization": "Bearer segredo"}


def test_consultar_brapi_faz_fallback_para_rota_legada(monkeypatch):
    monkeypatch.delenv("BRAPI_TOKEN", raising=False)
    chamadas = _registrar_chamadas(
        monkeypatch,
        [requests.Timeout("lento"), _RespostaFalsa(200, {"results": [{"symbol": "PETR4"}]})],
    )

    dados = consultar_brapi("PETR4")

    assert [c["url"] for c in chamadas] == [
        "https://brapi.dev/api/v2/stocks/quote",
        "https://brapi.dev/api/quote/PETR4",
    ]
    assert chamadas[1]["headers"] == {}
    assert dados["results"][0]["symbol"] == "PETR4"


def test_consultar_brapi_nao_faz_fallback_em_404(monkeypatch):
    chamadas = _registrar_chamadas(monkeypatch, [_RespostaFalsa(404)])

    with pytest.raises(HTTPException) as exc:
        consultar_brapi("XXXX3")

    assert exc.value.status_code == 404
    assert len(chamadas) == 1


def test_consultar_brapi_retorna_502_quando_ambas_rotas_falham(monkeypatch):
    _registrar_chamadas(monkeypatch, [_RespostaFalsa(500), requests.ConnectionError("fora")])

    with pytest.raises(HTTPException) as exc:
        consultar_brapi("PETR4")

    assert exc.value.status_code == 502


def test_publicar_cotacao_nao_publica_payload_invalido(monkeypatch):
    produtor = _ProdutorFalso()
    monkeypatch.setattr(api_ingestao, "obter_produtor", lambda: produtor)

    with pytest.raises(HTTPException):
        publicar_cotacao("PETR4", {"results": []})

    assert produtor.enviados == []
