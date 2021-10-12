import requests
from requests import HTTPError
from airbyte_client import AirbyteResponse
from airbyte_client import RESPONSE_CODES


def json():
    return {
        'attribute': 'value'
    }


def raise_error():
    raise HTTPError


def test_recognized_response_code(monkeypatch):
    response = requests.Response()
    response.status_code = 200
    response.json = json
    response.raise_for_status = lambda: True
    airbyte_response = AirbyteResponse(response)

    assert airbyte_response.message == RESPONSE_CODES[200]
    assert airbyte_response.payload['attribute'] == 'value'
    assert airbyte_response.ok


def test_unrecognized_response_code():
    response = requests.Response()
    response.status_code = 500
    response.json = json
    response.raise_for_status = raise_error
    airbyte_response = AirbyteResponse(response)

    assert airbyte_response.message == "Error: Unrecognized response code"
    assert airbyte_response.payload['attribute'] == 'value'
    assert not airbyte_response.ok
