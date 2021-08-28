import pytest
from airbyte_dto_factory import *

@pytest.fixture
def dummy_source_dto():
    """
    Creates a dummy SourceDto
    """
    source = SourceDto()
    source.source_definition_id = 'ef69ef6e-aa7f-4af1-a01d-ef775033524e'
    source.source_id = '7d95ec85-47c6-42d4-a7a2-8e5c22c810d2'
    source.workspace_id = 'f3b9e848-790c-4cdd-a475-5c6bb156dc10'
    source.connection_configuration = {}
    source.name = 'apache/superset'
    source.source_name = 'GitHub'
    source.tag = 'tag1'
    return source

@pytest.fixture
def dummy_destination_dto():
    """
    Creates a dummy SourceDto
    """
    destination = DestinationDto()
    destination.destination_definition_id = '25c5221d-dce2-4163-ade9-739ef790f503'
    destination.destination_id = 'a41cb2f8-fcce-4c91-adfe-37c4586609f5'
    destination.workspace_id = 'f3b9e848-790c-4cdd-a475-5c6bb156dc10'
    destination.connection_configuration = {
        'database': 'postgres',
        'host':  'hostname.com',
        'port': '5432',
        'schema': 'demo',
        'username': 'devrel_master'
    }
    destination.name = 'devrel-rds'
    destination.destination_name = 'Postgres'
    destination.tag = 'tag2'
    return destination


def test_sourcedto__to_payload(dummy_source_dto):
    """
    Test SourceDto.to_payload
    Verifies the data is not mutated by to_payload
    """
    payload = dummy_source_dto.to_payload()
    assert payload['sourceDefinitionId'] == 'ef69ef6e-aa7f-4af1-a01d-ef775033524e'
    assert payload['sourceId'] == '7d95ec85-47c6-42d4-a7a2-8e5c22c810d2'
    assert payload['workspaceId'] == 'f3b9e848-790c-4cdd-a475-5c6bb156dc10'
    assert len(payload['connectionConfiguration']) == 0
    assert payload['name'] == 'apache/superset'
    assert payload['sourceName'] == 'GitHub'


def test_destinationdto__to_payload(dummy_destination_dto):
    """
    Test DestinationDto.to_payload
    Verifies the data is not mutated by to_payload
    """
    payload = dummy_destination_dto.to_payload()
    assert payload['destinationDefinitionId'] == '25c5221d-dce2-4163-ade9-739ef790f503'
    assert payload['destinationId'] == 'a41cb2f8-fcce-4c91-adfe-37c4586609f5'
    assert payload['workspaceId'] == 'f3b9e848-790c-4cdd-a475-5c6bb156dc10'
    assert payload['connectionConfiguration']['database'] == 'postgres'
    assert payload['connectionConfiguration']['host'] == 'hostname.com'
    assert payload['connectionConfiguration']['schema'] == 'Demo'
    assert payload['connectionConfiguration']['username'] == 'devrel_master'
    assert payload['name'] == 'devrel-rds'
    assert payload['destinationName'] == 'Postgres'