#!/usr/bin/env python3
"""
This is a simple, open-source tool designed to help manage Airbyte deployments at scale through via the API.
"""

__author__ = "Robert Stolz"
__version__ = "0.1.0"
__license__ = "MIT"

import argparse
import yaml
from airbyte import Airbyte

class AirbyteConfigModel:
    def __init__(self):
        self.sources = {}
        self.destinations = {}
        self.connections = {}
        self.workspaces = {}
        self.global_config = {}

    def write_model_to_yaml(self, filename):
        pass

    def apply_model_to_deployment(self, client):
        pass

    def full_wipe(self, client):
        self.wipe_sources(client)
        self.wipe_destinations(client)
        # self.wipe_connections(client)
        pass

    def wipe_sources(self, client):
        removed = []
        for source in self.sources.values():
            if client.delete_source(source) == 204:
                removed.append(source.source_id)
            else:
                print("AirbyteConfigModel.wipe_sources : Unable to delete source: " + repr(source))
        for source_id in removed:
            self.sources.pop(source_id)
            
    def wipe_destinations(self,client):
        removed = []
        for destination in self.destinations.values():
            if client.delete_destination(destination) == 204:
                removed.append(destination.destination_id)
            else:
                print("AirbyteConfigModel.wipe_destinations : Unable to delete destination: " + repr(destination))
        for destination_id in removed:
            self.destinations.pop(destination_id)


# TODO: Take a look at the the way dbt and/or superset handles yaml files
class AirbyteDtoFactory:
    """Builds data transfer objects, each representing an abstraction inside the Airbyte architecture"""
    def __init__(self, source_definitions, destination_definitions):
        self.source_definitions = source_definitions
        self.destination_definitions = destination_definitions

    def build_dtos_from_yaml_config(self, yaml_config, secrets):
        new_dtos = {}
        if 'global' in yaml_config.keys():  # TODO: not needed?
            for item in yaml_config['global']:
                pass
        if 'workspaces' in yaml_config.keys():
            for item in yaml_config['workspaces']:
                pass
        if 'sources' in yaml_config.keys():
            new_sources = []
            for item in yaml_config['sources']:
                new_sources.append(self.build_source_dto(item))
            new_dtos['sources'] = new_sources
        if 'destinations' in yaml_config.keys():
            new_destinations = []
            for item in yaml_config['destinations']:
                new_destinations.append(self.build_destination_dto(item))
            new_dtos['destinations'] = new_destinations
        if 'connections' in yaml_config.keys():
            for item in yaml_config['connections']:
                pass
        self.populate_secrets(secrets, new_dtos)
        return new_dtos

    def populate_secrets(self, secrets, new_dtos):
        # TODO: Find a better way to deal with unpredictably named secrets
        for source in new_dtos['sources']:
            if source.source_name in secrets['sources']:
                if 'access_token' in source.connection_configuration:
                    source.connection_configuration['access_token'] = secrets['sources'][source.source_name]['access_token']
                elif 'token' in source.connection_configuration:
                    source.connection_configuration['token'] = secrets['sources'][source.source_name]['token']
        for destination in new_dtos['destinations']:
            if destination.destination_name in secrets['destinations']:
                if 'password' in destination.connection_configuration:
                    destination.connection_configuration['password'] = secrets['destinations'][destination.destination_name]['password']
        pass

    def build_source_dto(self, source):
        r = SourceDto()
        r.connection_configuration = source['connectionConfiguration']
        r.name = source['name']
        r.source_name = source['sourceName']
        if 'sourceDefinitionId' in source:
            r.source_definition_id = source['sourceDefinitionId']
        else:
            for definition in self.source_definitions['sourceDefinitions']:
                if r.source_name == definition['name']:
                    r.source_definition_id = definition['sourceDefinitionId']
        if 'sourceId' in source:
            r.source_id = source['sourceId']
        if 'workspaceId' in source:
            r.workspace_id = source['sourceId']
        # TODO: check for validity?
        return r

    def build_destination_dto(self, destination):
        r = DestinationDto()
        r.connection_configuration = destination['connectionConfiguration']
        r.destination_name = destination['destinationName']
        r.name = destination['name']
        if 'destinationDefinitionId' in destination:
            r.destination_definition_id = destination['destinationDefinitionId']
        else:
            for definition in self.destination_definitions['destinationDefinitions']:
                if r.destination_name == definition['name']:
                    r.destination_definition_id = definition['destinationDefinitionId']
        if 'destinationId' in destination:
            r.destination_id = destination['destinationId']
        if 'workspaceId' in destination:
            r.workspace_id = destination['destinationId']
        # TODO: check for validity?
        return r

    def build_connection_dto(self, connection):
        r = ConnectionDto()
        r.connection_id = connection['connectionId']
        r.name = connection['name']
        r.prefix = connection['prefix']
        r.source_id = connection['sourceId']
        r.destination_id = connection['destinationId']
        r.sync_catalog = connection['syncCatalog']
        r.schedule = connection['schedule']
        r.status = connection['status']
        # TODO: check for validity?
        return r

class SourceDto:
    """Data transfer object class for Source-type Airbyte abstractions"""
    def __init__(self):
        self.source_definition_id = None
        self.source_id = None
        self.workspace_id = None
        self.connection_configuration = {}
        self.name = None
        self.source_name = None

    def to_payload(self):
        r = {}
        r['sourceDefinitionId'] = self.source_definition_id
        r['sourceId'] = self.source_id
        r['workspaceId'] = self.workspace_id
        r['connectionConfiguration'] = self.connection_configuration
        r['name'] = self.name
        r['sourceName'] = self.source_name
        return r

class DestinationDto:
    """Data transfer object class for Destination-type Airbyte abstractions"""
    def __init__(self):
        self.destination_definition_id = None
        self.destination_id = None
        self.workspace_id = None
        self.connection_configuration = {}
        self.name = None
        self.destination_name = None

class ConnectionDto:
    """Data transfer object class for Connection-type Airbyte abstractions"""
    def __init__(self):
        self.connection_id = None
        self.name = None
        self.prefix = None
        self.source_id = None
        self.destination_id = None
        self.sync_catalog = {}
        self.schedule = {}
        self.status = None

class WorkspaaceDto:
    """Data transfer object class for Workspace-type Airbyte abstractions"""
    def __init__(self):
        pass

def main(args):
    """ Main entry point of the app """

    airbyte_model = AirbyteConfigModel()
    client = Airbyte(args)
    workspace = client.get_workspace_by_slug()

    #get source and destination definitions
    available_sources = client.get_source_definitions()
    available_destinations = client.get_destination_definitions()
    #initialize data transfer object factory
    dto_factory = AirbyteDtoFactory(available_sources, available_destinations)

    #get config from config.yml
    yaml_config = yaml.safe_load(open("config.yml", 'r'))
    secrets = yaml.safe_load(open("secrets.yml", 'r'))
    new_dtos = dto_factory.build_dtos_from_yaml_config(yaml_config, secrets)  # TODO: don't make an AirbyteConfigModel here. Instead, just leave it as yaml config.

    # get configured connectors and connections from Airbyte API
    configured_sources = client.get_configured_sources(workspace)
    configured_destinations = client.get_configured_destinations(workspace)
    configured_connections = client.get_configured_connections(workspace)

    # send configured_sources to the factory to build sourceDtos
    for source in configured_sources:
        source_dto = dto_factory.build_source_dto(source)
        airbyte_model.sources[source_dto.source_id] = source_dto  # TODO: Refactor airbyte_model -> airbyte_model
    for destination in configured_destinations:
        destination_dto = dto_factory.build_destination_dto(destination)
        airbyte_model.destinations[destination_dto.destination_id] = destination_dto
    for connection in configured_connections:
        connection_dto = dto_factory.build_connection_dto(connection)
        airbyte_model.connections[connection_dto.connection_id] = connection_dto

    # sync yaml to deployment
    if (args.hard):
        airbyte_model.full_wipe(client)

    for source in new_dtos['sources']:
        if source.source_id == None:
            client.create_source(source)
        else:
            pass  # TODO: modify existing source
    for destination in new_dtos['destinations']:
        if destination.destination_id == None:
            client.create_destination(destination)

    # sync deployment to yaml
    # airbyte_model.write_to_yaml()

    # deploment to deployment

    pass

if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Required positional argument
    #parser.add_argument("arg", help="Required positional argument")
    parser.add_argument("url", help="URL of the Airbyte deployment")

    # Optional argument flag which defaults to False
    parser.add_argument("-f", "--flag", action="store_true", default=False)
    parser.add_argument("-ss", "--sync-sources", action="store_true", default=False)
    parser.add_argument("-sd", "--sync-destinations", action="store_true", default=False)
    parser.add_argument("-sa", "--sync-all", action="store_true", default=False)
    parser.add_argument("-ha", "--hard", action="store_true", default=False)

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("-n", "--name", action="store", dest="name")

    # Optional verbosity counter (eg. -v, -vv, -vvv, etc.)
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="Verbosity (-v, -vv, etc)")

    # Specify output of "--version"
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s (version {version})".format(version=__version__))

    args = parser.parse_args()
    main(args)
