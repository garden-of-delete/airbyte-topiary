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


def test_recognized_response_code():
    mock_response = requests.Response()
    mock_response.status_code = 200
    mock_response.json = json()
    mock_response.raise_for_status = lambda: True
    response = AirbyteResponse(mock_response)

    assert response.message == RESPONSE_CODES[200]
    assert response.payload['attribute'] == 'value'
    assert response.ok


def test_unrecognized_response_code():
    mock_response = requests.Response()
    mock_response.status_code = 200
    mock_response.json = json()
    mock_response.raise_for_status = raise_error
    response = AirbyteResponse(mock_response)

    assert response.message == "Unrecognized response code"
    assert response.payload['attribute'] == 'value'
    assert not response.ok
