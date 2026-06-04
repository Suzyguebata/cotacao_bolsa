import pytest
from fastapi import HTTPException

from api.lambda_api import validate_brapi_payload


def test_validate_brapi_payload_normalizes_known_fields():
    data = {
        "results": [
            {
                "symbol": "PETR4",
                "regularMarketPrice": 35.5,
                "regularMarketTime": "2023-10-27T10:00:00Z",
                "regularMarketChange": 0.5,
                "marketCap": 1000000.0,
                "ignoredField": "ignored",
            }
        ],
        "requestedAt": "2023-10-27T10:00:05Z",
    }

    payload = validate_brapi_payload(data)

    assert payload == {
        "results": [
            {
                "symbol": "PETR4",
                "regularMarketPrice": 35.5,
                "regularMarketTime": "2023-10-27T10:00:00Z",
                "regularMarketChange": 0.5,
                "marketCap": 1000000.0,
            }
        ],
        "requestedAt": "2023-10-27T10:00:05Z",
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
