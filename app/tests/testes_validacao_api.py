import pytest
from fastapi import HTTPException

from api.api_ingestao import validar_payload_brapi


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
