class SourceDto:
    """Data transfer object class for Source-type Airbyte abstractions"""

    def __init__(self):
        self.source_definition_id = None
        self.source_id = None
        self.workspace_id = None
        self.connection_configuration = {}
        self.name = None
        self.source_name = None
        self.tag = None

    def to_payload(self):
        """sends this dto object to a dict formatted as a payload"""
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
        self.tag = None

    def to_payload(self):
        """sends this dto object to a dict formatted as a payload"""
        r = {}
        r['destinationDefinitionId'] = self.destination_definition_id
        r['destinationId'] = self.destination_id
        r['workspaceId'] = self.workspace_id
        r['connectionConfiguration'] = self.connection_configuration
        r['name'] = self.name
        r['destinationName'] = self.destination_name
        return r


class ConnectionDto:
    """Data transfer object class for Connection-type Airbyte abstractions"""

    def __init__(self):
        self.connection_id = None
        self.name = None
        self.prefix = None
        self.source_id = None
        self.destination_id = None
        self.sync_catalog = {}  # sync_catalog['streams'] is a list of dicts {stream:, config:}
        self.schedule = {}
        self.status = None

    def to_payload(self):
        pass  # TODO: implement the to_payload method


class StreamDto:
    """Data transfer object class for the stream, belongs to the connection abstraction"""

    def __init__(self):
        self.name = None
        self.json_schema = {}
        self.supported_sync_modes = []
        self.source_defined_cursor = None
        self.default_cursor_field = []
        self.source_defined_primary_key = []
        self.namespace = None


class StreamConfigDto:
    """Data transfer object class for the stream configuration, belongs to the connection abstraction"""

    def __init__(self):
        self.sync_mode = None
        self.cursor_field = []
        self.destination_sync_mode = None
        self.primary_key = []
        self.alias_name = None
        self.selected = None


class WorkspaaceDto:
    """Data transfer object class for Workspace-type Airbyte abstractions"""

    def __init__(self):
        pass

class AirbyteDtoFactory:
    """
    Builds data transfer objects, each representing an abstraction inside the Airbyte architecture
    """
    def __init__(self, source_definitions, destination_definitions):
        self.source_definitions = source_definitions
        self.destination_definitions = destination_definitions

    def populate_secrets(self, secrets, new_dtos):
        # TODO: Find a better way to deal with unpredictably named secrets
        if 'sources' in new_dtos:
            for source in new_dtos['sources']:
                if source.source_name in secrets['sources']:
                    if 'access_token' in source.connection_configuration:
                        source.connection_configuration['access_token'] = secrets['sources'][source.source_name]['access_token']
                    elif 'token' in source.connection_configuration:
                        source.connection_configuration['token'] = secrets['sources'][source.source_name]['token']
        if 'destinations' in new_dtos:
            for destination in new_dtos['destinations']:
                if destination.destination_name in secrets['destinations']:
                    if 'password' in destination.connection_configuration:
                        destination.connection_configuration['password'] = secrets['destinations'][destination.destination_name]['password']

    def build_source_dto(self, source: dict) -> SourceDto:
        """
        Builds a SourceDto object from a dict representing a source
        """
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
            r.workspace_id = source['workspaceId']
        if 'tag' in source:
            r.tag = source['tag']
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
            r.workspace_id = destination['workspaceId']
        if 'tag' in destination:
            r.tag = destination['tag']
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