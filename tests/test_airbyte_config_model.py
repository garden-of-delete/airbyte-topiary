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


def test_apply_to_deployment():
    pass


def test_wipe_sources():
    pass


def test_wipe_destinations():
    pass


def test_validate():
    pass


def test_validate_sources():
    pass


def test_validate_destinations():
    pass


def test_validate_connections():
    pass
