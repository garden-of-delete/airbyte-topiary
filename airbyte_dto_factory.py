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