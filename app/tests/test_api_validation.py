import pytest
from fastapi import HTTPException

from app.api.lambda_api import validate_brapi_payload


def test_validate_brapi_payload_normalizes_known_fields():
    data = {
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

    payload = validate_brapi_payload(data)

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


def test_validate_brapi_payload_rejects_empty_results():
    with pytest.raises(HTTPException) as exc:
        validate_brapi_payload({"results": []})

    assert exc.value.status_code == 502
    assert "sem cotações" in exc.value.detail


def test_validate_brapi_payload_rejects_invalid_schema():
    with pytest.raises(HTTPException) as exc:
        validate_brapi_payload({"results": "PETR4"})

    assert exc.value.status_code == 502
    assert "schema esperado" in exc.value.detail
