import pytest
from tests.test_fixtures import *
import yaml
import utils


def test_airbyte_config_model__write_yaml(dummy_populated_airbyte_config_model, dummy_source_dict,
                                          dummy_destination_dict):
    config_model = dummy_populated_airbyte_config_model
    config_model.write_yaml("test_airbyte_config_model__write_yaml.yml")
    t = yaml.safe_load(open("test_airbyte_config_model__write_yaml.yml", 'r'))
    assert len(t['sources']) == 1
    assert len(t['destinations']) == 1
    dummy_source_dict.pop('tags')  # tags will never be part of a config model in a workflow where write_yaml is invoked
    dummy_destination_dict.pop('tags')
    assert t['sources'][0] == dummy_source_dict
    assert t['destinations'][0] == dummy_destination_dict


def test_has(dummy_airbyte_config_model, dummy_source_dto, dummy_destination_dto, dummy_connection_dto):
    assert dummy_airbyte_config_model.has(dummy_source_dto) is True
    assert dummy_airbyte_config_model.has(dummy_destination_dto) is True
    assert dummy_airbyte_config_model.has(dummy_connection_dto) is True
    # modify ids and check false
    dummy_source_dto.source_id += 'mod'
    dummy_destination_dto.destination_id += 'mod'
    dummy_connection_dto.connection_id += 'mod'
    assert dummy_airbyte_config_model.has(dummy_source_dto) is False
    assert dummy_airbyte_config_model.has(dummy_destination_dto) is False
    assert dummy_airbyte_config_model.has(dummy_connection_dto) is False
    # remove ids, causing .has to check against name. check true
    dummy_source_dto.source_id = None
    dummy_destination_dto.destination_id += None
    dummy_connection_dto.connection_id += None
    assert dummy_airbyte_config_model.has(dummy_source_dto) is True
    assert dummy_airbyte_config_model.has(dummy_destination_dto) is True
    assert dummy_airbyte_config_model.has(dummy_connection_dto) is True
    # modify names and check false
    dummy_source_dto.source_name += 'mod'
    dummy_destination_dto.destination_namme += 'mod'
    dummy_connection_dto.source_name += 'mod'
    assert dummy_airbyte_config_model.has(dummy_source_dto) is False
    assert dummy_airbyte_config_model.has(dummy_destination_dto) is False
    assert dummy_airbyte_config_model.has(dummy_connection_dto) is False
