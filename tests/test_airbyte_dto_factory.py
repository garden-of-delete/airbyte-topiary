import pytest
from tests.test_fixtures import *


def test_source_dto__to_payload(dummy_source_dto):
    """
    Test SourceDto.to_payload
    Verifies the data is not mutated by to_payload
    """
    payload = dummy_source_dto.to_payload()
    assert payload['sourceDefinitionId'] == 'ef69ef6e-aa7f-4af1-a01d-ef775033524e'
    assert payload['sourceId'] == '7d95ec85-47c6-42d4-a7a2-8e5c22c810d2'
    assert payload['workspaceId'] == 'f3b9e848-790c-4cdd-a475-5c6bb156dc10'
    assert payload['connectionConfiguration'] == {'access_token': '**********'}
    assert payload['name'] == 'apache/superset'
    assert payload['sourceName'] == 'GitHub'


def test_destination_dto__to_payload(dummy_destination_dto):
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
    assert payload['connectionConfiguration']['schema'] == 'demo'
    assert payload['connectionConfiguration']['username'] == 'devrel_master'
    assert payload['name'] == 'devrel-rds'
    assert payload['destinationName'] == 'Postgres'


def test_connection_dto__to_payload(dummy_connection_dto):
    assert False


def test_dto_factory__build_source_dto(dummy_airbyte_dto_factory, dummy_source_dict, dummy_source_dto):
    """
    Test AirbyteDtoFactory.build_source_dto
    """
    t = dummy_airbyte_dto_factory.build_source_dto(dummy_source_dict)
    assert t.source_definition_id == dummy_source_dto.source_definition_id
    assert t.source_id == dummy_source_dto.source_id
    assert t.workspace_id == dummy_source_dto.workspace_id
    assert t.connection_configuration == dummy_source_dto.connection_configuration
    assert t.source_name == dummy_source_dto.source_name
    assert t.name == dummy_source_dto.name
    assert t.tag == dummy_source_dto.tag


def test_dto_factory__build_destination_dto(dummy_airbyte_dto_factory, dummy_destination_dict, dummy_destination_dto):
    """
    Test AirbyteDtoFactory.build_destination_dto
    """
    t = dummy_airbyte_dto_factory.build_destination_dto(dummy_destination_dict)
    assert t.destination_definition_id == dummy_destination_dto.destination_definition_id
    assert t.destination_id == dummy_destination_dto.destination_id
    assert t.workspace_id == dummy_destination_dto.workspace_id
    assert t.connection_configuration == dummy_destination_dto.connection_configuration
    assert t.destination_name == dummy_destination_dto.destination_name
    assert t.name == dummy_destination_dto.name
    assert t.tag == dummy_destination_dto.tag


def test_dto_factory__build_connection_dto(dummy_airbyte_dto_factory, dummy_connection_dict, dummy_connection_dto):
    assert False


def test_dto_factory__populate_secrets(dummy_airbyte_dto_factory, dummy_secrets_dict, dummy_source_dto,
                                       dummy_destination_dto):
    """
    Test AirbyteDtoFactory.populate_secrets
    Verifies placeholder secrets in the DTOs are being correctly overridden
    """
    new_dtos = {'sources': [dummy_source_dto], 'destinations': [dummy_destination_dto]}
    dummy_airbyte_dto_factory.populate_secrets(dummy_secrets_dict, new_dtos)
    assert new_dtos['sources'][0].connection_configuration['access_token'] == 'ghp_SECRET_TOKEN'
    assert new_dtos['destinations'][0].connection_configuration['password'] == 'SECRET_POSTGRES_PASSWORD'