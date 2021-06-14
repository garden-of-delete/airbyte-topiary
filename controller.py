from airbyte_config_model import AirbyteConfigModel
from airbyte_client import AirbyteClient
from airbyte_dto_factory import *

class Controller:
    """The controller controls program flow and communicates with the outside world"""
    def __init__(self):
        self.dto_factory = None

    def instantiate_dto_factory(self, source_definitions, destination_definitions):
        self.dto_factory = AirbyteDtoFactory(source_definitions,destination_definitions)

    def get_definitions(self, client):
        """Retrieves source and destination definitions for configured sources"""
        available_sources = client.get_source_definitions().payload
        available_destinations = client.get_destination_definitions().payload
        print("main: retrieved source and destination definitions from: " + client.airbyte_url)
        return {'source_definitions': available_sources, 'destination_definitions': available_destinations}

    def get_airbyte_configuration(self, client, workspace):
        """Retrieves the configuration from an airbyte deployment and returns an AirbyteConfigModel representing it"""
        configured_sources = client.get_configured_sources(workspace).payload['sources']
        configured_destinations = client.get_configured_destinations(workspace).payload['destinations']
        configured_connections = client.get_configured_connections(workspace).payload['connections']
        print("main: retrieved configuration from: " + client.airbyte_url)
        airbyte_model = AirbyteConfigModel()
        for source in configured_sources:
            source_dto = self.dto_factory.build_source_dto(source)
            airbyte_model.sources[source_dto.source_id] = source_dto
        for destination in configured_destinations:
            destination_dto = self.dto_factory.build_destination_dto(destination)
            airbyte_model.destinations[destination_dto.destination_id] = destination_dto
        for connection in configured_connections:
            connection_dto = self.dto_factory.build_connection_dto(connection)
            airbyte_model.connections[connection_dto.connection_id] = connection_dto
        return airbyte_model

    def get_workspace(self, args, client):
        if args.workspace_slug:
            workspace = client.get_workspace_by_slug(args.workspace_slug).payload
        else:
            workspace = client.get_workspace_by_slug().payload
        return workspace

    def build_dtos_from_yaml_config(self, yaml_config, secrets):
        new_dtos = {}
        if 'global' in yaml_config.keys():
            for item in yaml_config['global']:
                pass
        if 'workspaces' in yaml_config.keys():
            for item in yaml_config['workspaces']:
                pass
        if 'sources' in yaml_config.keys():
            new_sources = []
            for item in yaml_config['sources']:
                new_sources.append(self.dto_factory.build_source_dto(item))
            new_dtos['sources'] = new_sources
        if 'destinations' in yaml_config.keys():
            new_destinations = []
            for item in yaml_config['destinations']:
                new_destinations.append(self.dto_factory.build_destination_dto(item))
            new_dtos['destinations'] = new_destinations
        if 'connections' in yaml_config.keys():
            for item in yaml_config['connections']:
                pass
        self.dto_factory.populate_secrets(secrets, new_dtos)
        return new_dtos

    def sync_sources(self, airbyte_model, client, workspace, new_dtos):
        for new_source in new_dtos['sources']:
            if new_source.source_id is None:
                response = client.create_source(new_source, workspace)
                source_dto = self.dto_factory.build_source_dto(response.payload)
                print("Created source: " + source_dto.source_id)
                airbyte_model.sources[source_dto.source_id] = source_dto
            else:
                pass  # TODO: modify existing source

    def sync_destinations(self, airbyte_model, client, workspace, new_dtos):
        for new_destination in new_dtos['destinations']:
            if new_destination.destination_id is None:
                response = client.create_destination(new_destination, workspace)
                destination_dto = self.dto_factory.build_destination_dto(response.payload)
                print("Created destination: " + destination_dto.destination_id)
                airbyte_model.destinations[destination_dto.destination_id] = destination_dto
            else:
                pass  # TODO: modify existing destination

    def wipe(self, airbyte_model, client):
        airbyte_model.full_wipe(client)

    def validate(self):
        pass