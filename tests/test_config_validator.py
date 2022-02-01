import pytest
from tests.test_fixtures import *

import config_validator


def test_check_source(dummy_source_dict, dummy_config_loader):
    assert dummy_config_loader.check_source(dummy_source_dict) is True
    dummy_source_dict.pop('sourceName')  # pop a required field
    assert dummy_config_loader.check_source(dummy_source_dict) is False


def test_check_destination(dummy_destination_dict, dummy_config_loader):
    assert dummy_config_loader.check_destination(dummy_destination_dict) is True
    dummy_destination_dict.pop('destinationName')  # pop a required field
    assert dummy_config_loader.check_destination(dummy_destination_dict) is False


def test_check_required(dummy_config_loader, dummy_source_dict, dummy_destination_dict, dummy_new_connection_dict):
    required_source_fields = {x: config_validator.SOURCE_FIELDS[x] for x in config_validator.SOURCE_FIELDS if
                              config_validator.SOURCE_FIELDS[x] is True}
    required_destination_fields = {x: config_validator.DESTINATION_FIELDS[x] for x in config_validator.DESTINATION_FIELDS if
                                   config_validator.DESTINATION_FIELDS[x] is True}
    required_connection_fields = {x: config_validator.CONNECTION_FIELDS[x] for x in config_validator.CONNECTION_FIELDS if
                                  config_validator.CONNECTION_FIELDS[x] is True}
    assert dummy_config_loader.check_required(dummy_source_dict, required_source_fields) is True
    assert dummy_config_loader.check_required(dummy_destination_dict, required_destination_fields) is True
    assert dummy_config_loader.check_required(dummy_new_connection_dict, required_connection_fields) is True
    dummy_source_dict.pop("name")
    dummy_destination_dict.pop("name")
    dummy_new_connection_dict.pop("sourceName")
    assert dummy_config_loader.check_required(dummy_source_dict, required_source_fields) is False
    assert dummy_config_loader.check_required(dummy_destination_dict, required_destination_fields) is False
    assert dummy_config_loader.check_required(dummy_new_connection_dict, required_connection_fields) is False