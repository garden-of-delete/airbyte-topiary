from airbyte_config_model import AirbyteConfigModel
from airbyte_client import AirbyteClient
from airbyte_dto_factory import *

class Controller:
    """The controller controls program flow and communicates with the outside world"""
    def __init__(self):
        pass


    def get_definitions(self, client):
        # get source and destination definitions
        available_sources = client.get_source_definitions().payload
        available_destinations = client.get_destination_definitions().payload
        # initialize data transfer object factory
        print("main: retrieved source and destination definitions from: " + client.airbyte_url)


    def get_airbyte_configuration(self, client, workspace):
        # get configured connectors and connections from Airbyte API
        configured_sources = client.get_configured_sources(workspace).payload['sources']
        configured_destinations = client.get_configured_destinations(workspace).payload['destinations']
        configured_connections = client.get_configured_connections(workspace).payload['connections']
        print("main: retrieved configuration from: " + client.airbyte_url)
        return {'sources': configured_sources,
                'destinations': configured_destinations,
                'connections': configured_connections}


    def get_workspace(self, args, client):
        if args.workspace_slug:
            workspace = client.get_workspace_by_slug(args.workspace_slug).payload
        else:
            workspace = client.get_workspace_by_slug().payload
        return workspace


    def sync_sources(self, airbyte_model, client, workspace, dto_factory, new_dtos):
        for new_source in new_dtos['sources']:
            if new_source.source_id is None:
                response = client.create_source(new_source, workspace)
                source_dto = dto_factory.build_source_dto(response.payload)
                airbyte_model.sources[source_dto.source_id] = source_dto
            else:
                pass  # TODO: modify existing source


    def sync_destinations(self, airbyte_model, client, workspace, dto_factory, new_dtos):
        for new_destination in new_dtos['destinations']:
            if new_destination.destination_id is None:
                response = client.create_destination(new_destination, workspace)
                destination_dto = dto_factory.build_destination_dto(response.payload)
                airbyte_model.destinations[destination_dto.destination_id] = destination_dto
            else:
                pass  # TODO: modify existing destination


    def wipe(self):
        pass


    def update(self):
        pass


    def validate(self):
        pass