from airbyte_config_model import AirbyteConfigModel
from airbyte_client import AirbyteClient
from airbyte_dto_factory import *
import utils
import yaml

class Controller:
    """Communicates with the user. Provides methods to execute the tasks for each workflow."""
    def __init__(self):
        self.dto_factory = None

    def instantiate_dto_factory(self, source_definitions, destination_definitions):
        self.dto_factory = AirbyteDtoFactory(source_definitions,destination_definitions)

    def instantiate_client(self, args):
        # if origin is a deployment and target is not specified
        if not utils.is_yaml(args.origin) and args.target is None:
            client = AirbyteClient(args.origin)
        # if in sync mode and source is a yaml file
        elif utils.is_yaml(args.origin):
            if utils.is_yaml(args.target):
                print("Fatal error: --target must be followed by a valid "
                      "Airbyte deployment url when the origin is a .yaml file")
                exit(2)
            client = AirbyteClient(args.target)
        elif utils.is_yaml(args.target):
            if utils.is_yaml(args.origin):
                print("Fatal error: --target must be followed by a valid "
                      "Airbyte deployment url when the origin is a .yaml file")
                exit(2)
            client = AirbyteClient(args.origin)
        else:
            print("Fatal error: the origin or --target must be a valid .yaml configuration file")
            exit(2)
        return client

    def read_yaml_config(self, args):
        """get config from config.yml"""
        secrets = None
        if utils.is_yaml(args.origin):
            yaml_config = yaml.safe_load(open(args.origin, 'r'))
        else:
            yaml_config = yaml.safe_load(open(args.target, 'r'))
        if args.secrets:
            secrets = yaml.safe_load(open(args.secrets, 'r'))  # TODO: if no --secrets specified, skip
        else:
            print("Warning: Reading yaml config but --secrets not specified.")
        return yaml_config, secrets

    def get_definitions(self, client):
        """Retrieves source and destination definitions for configured sources"""
        print("Retrieving source and destination definitions from: " + client.airbyte_url)
        available_sources = client.get_source_definitions().payload
        available_destinations = client.get_destination_definitions().payload
        return {'source_definitions': available_sources, 'destination_definitions': available_destinations}

    def get_airbyte_configuration(self, client, workspace):
        """Retrieves the configuration from an airbyte deployment and returns an AirbyteConfigModel representing it"""
        print("Retrieving Airbyte configuration from: " + client.airbyte_url)
        configured_sources = client.get_configured_sources(workspace).payload['sources']
        configured_destinations = client.get_configured_destinations(workspace).payload['destinations']
        configured_connections = client.get_configured_connections(workspace).payload['connections']
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
        """Retrieves workspace specified by the --workspace argument, or the default workspace otherwise"""
        if args.workspace_slug:
            workspace = client.get_workspace_by_slug(args.workspace_slug).payload
        else:
            workspace = client.list_workspaces().payload['workspaces'][0]  # TODO: support for multiple workspaces
        return workspace

    def build_dtos_from_yaml_config(self, yaml_config, secrets):
        """Creates a dict of new DTOs by parsing the user specified .yml config file"""
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
            new_connections = []
            new_connection_groups = []
            for connection_entity in yaml_config['connections']:
                if 'groupName' in connection_entity:  # if entity is a group defined with in shorthand with tags
                    new_connection_groups.append(self.dto_factory.build_connection_group_dto(connection_entity))
                else:  # else connection_entity is a connection, not a connection group
                    new_connections.append(self.dto_factory.build_connection_dto(connection_entity))
            if len(new_connections) > 0:
                new_dtos['connections'] = new_connections
            if len(new_connection_groups) > 0:
                new_dtos['connectionGroups'] = new_connection_groups
        self.dto_factory.populate_secrets(secrets, new_dtos)
        return new_dtos

    def sync_sources_to_deployment(self, airbyte_model, client, workspace, dtos_from_config):
        for new_source in dtos_from_config['sources']:
            if new_source.source_id is None:
                response = client.create_source(new_source, workspace)
                if response.ok:
                    source_dto = self.dto_factory.build_source_dto(response.payload)
                    print("Created source: " + source_dto.source_id)
                    airbyte_model.sources[source_dto.source_id] = source_dto
                else:
                    print("Error creating connector " + new_source.name + ": " + response.message)
            else:
                response = client.update_source(new_source)
                if response.ok:
                    source_dto = self.dto_factory.build_source_dto(response.payload)
                    airbyte_model.sources[source_dto.source_id] = source_dto
                    print("Modified source: " + source_dto.source_id)
                else:
                    print("Error: unable to modify source: " + new_source.source_id)

    def sync_destinations_to_deployment(self, airbyte_model, client, workspace, dtos_from_config):
        for new_destination in dtos_from_config['destinations']:
            if new_destination.destination_id is None:
                response = client.create_destination(new_destination, workspace)
                destination_dto = self.dto_factory.build_destination_dto(response.payload)
                print("Created destination: " + destination_dto.destination_id)
                airbyte_model.destinations[destination_dto.destination_id] = destination_dto
            else:
                response = client.update_destination(new_destination)
                if response.ok:
                    destination_dto = self.dto_factory.build_destination_dto(response.payload)
                    airbyte_model.destinations[destination_dto.destination_id] = destination_dto
                    print("Modified destination: " + destination_dto.destination_id)
                else:
                    print("Error: unable to modify destination: " + new_destination.destination_id)

    def sync_connections_to_deployment(self, airbyte_model, client, workspace, dtos_from_config):
        """
        Applies a collection of connectionDtos and/or connectionGroupDtos, to an airbyte deployment
        """
        # create or modify each connection defined in yml
        if 'connections' in dtos_from_config:
            for new_connection in dtos_from_config['connections']:
                if new_connection.connection_id is None:  # create new connection
                    # if source_id is none, resolve source_id from source_name
                    if new_connection.source_id is None:
                        new_connection.source_id = next(i for i in airbyte_model.connections.val if
                                                        i.source_name == new_connection.source_name)
                    pass
                    # check validity of source?

                    # if destination_id is none, resolve destination_id from destination_name

                    # if sync_catalog is none, generate a generic sync catalog from the source


                    response = client.create_connection(new_connection, workspace)
                    connection_dto = self.dto_factory.build_connection_dto(response.payload)  # TODO: test
                    print("Created connection: " + connection_dto.connection_id)
                    airbyte_model.connections[connection_dto.connection_id] = connection_dto
                else:  # modify existing connection  # TODO: test modify existing connection
                    response = client.update_connection(new_connection)
                    if response.ok:
                        connection_dto = self.dto_factory.build_connection_dto(response.payload)
                        airbyte_model.connections[connection_dto.connection_id] = connection_dto
                        print("Modified connection: " + connection_dto.connection_id)
                    else:
                        print("Error: unable to modify connection: " + new_connection.connection_id)


    def wipe_sources(self, airbyte_model, client):
        """Wrapper for AirbyteConfigModel.wipe_sources"""
        print("Wiping sources on " + client.airbyte_url)
        airbyte_model.wipe_sources(client)

    def wipe_destinations(self, airbyte_model, client):
        """Wrapper for AirbyteConfigModel.wipe_destinations"""
        print("Wiping destinations on " + client.airbyte_url)
        airbyte_model.wipe_destinations(client)

    def wipe_connections(self, airbyte_model, client):
        """Wrapper for AirbyteConfigModel.wipe_connections"""
        pass  # TODO: implement controller.wipe_connections

    def wipe_all(self, airbyte_model, client):
        """Wipes all sources, destinations, and connections in the specified airbyte deployment"""
        print("Wiping deployment: " + client.airbyte_url)
        self.wipe_sources(airbyte_model, client)
        self.wipe_destinations(airbyte_model, client)
        # self.wipe_connections(client)

    def validate_sources(self, airbyte_model, client):
        """Wrapper for AirbyteConfigModel.validate_sources"""
        print("Validating sources...")
        airbyte_model.validate_sources(client)

    def validate_destinations(self, airbyte_model, client):
        """Wrapper for AirbyteConfigModel.validate_destinations"""
        print("Validating destinations...")
        airbyte_model.validate_destinations(client)

    def validate_connections(self, airbyte_mopdel, client):
        """Wrapper for AirbyteConfigModel.validate_connections"""
        pass  # TODO: implement Controller.validate_connections

    def validate_all(self, airbyte_model, client):
        """Validates all sources, destinations, and connections in the specified AirbyteConfigModel"""
        self.validate_sources(airbyte_model, client)
        self.validate_destinations(airbyte_model, client)
        #self.validate_connections(airbyte_model, client)  # TODO: turn on