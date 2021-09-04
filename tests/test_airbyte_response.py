import requests
from requests import HTTPError
from airbyte_client import AirbyteResponse
from airbyte_client import RESPONSE_CODES


def mock_json():
    return {
        'attribute': 'value'
    }


def mock_raise_error():
    raise HTTPError


def test_recognized_response_code(monkeypatch):
    mock_response = requests.Response()
    monkeypatch.setattr(mock_response, 'status_code', 200)
    monkeypatch.setattr(mock_response, 'json', mock_json)
    monkeypatch.setattr(mock_response, 'raise_for_status', lambda: True)
    response = AirbyteResponse(mock_response)

    assert response.message == RESPONSE_CODES[200]
    assert response.payload['attribute'] == 'value'
    assert response.ok


def test_unrecognized_response_code(monkeypatch):
    mock_response = requests.Response()
    monkeypatch.setattr(mock_response, 'status_code', 500)
    monkeypatch.setattr(mock_response, 'json', mock_json)
    monkeypatch.setattr(mock_response, 'raise_for_status', mock_raise_error)
    response = AirbyteResponse(mock_response)

    assert response.message == "Unrecognized response code"
    assert response.payload['attribute'] == 'value'
    assert not response.ok
