class SourceDto:
    """
    Data transfer object class for Source-type Airbyte abstractions
    """
    def __init__(self):
        self.source_definition_id = None
        self.source_id = None
        self.workspace_id = None
        self.connection_configuration = {}
        self.name = None
        self.source_name = None
        self.tags = []

    def to_payload(self):
        """
        sends this dto object to a dict formatted as a payload
        """
        r = {}
        r['sourceDefinitionId'] = self.source_definition_id
        r['sourceId'] = self.source_id
        r['workspaceId'] = self.workspace_id
        r['connectionConfiguration'] = self.connection_configuration
        r['name'] = self.name
        r['sourceName'] = self.source_name
        return r


class DestinationDto:
    """
    Data transfer object class for Destination-type Airbyte abstractions
    """
    def __init__(self):
        self.destination_definition_id = None
        self.destination_id = None
        self.workspace_id = None
        self.connection_configuration = {}
        self.name = None
        self.destination_name = None
        self.tags = []

    def to_payload(self):
        """
        sends this dto object to a dict formatted as a payload
        """
        r = {}
        r['destinationDefinitionId'] = self.destination_definition_id
        r['destinationId'] = self.destination_id
        r['workspaceId'] = self.workspace_id
        r['connectionConfiguration'] = self.connection_configuration
        r['name'] = self.name
        r['destinationName'] = self.destination_name
        return r


class ConnectionDto:
    """
    Data transfer object class for Connection-type Airbyte abstractions
    """
    def __init__(self):
        self.connection_id = None
        self.name = 'default'
        self.prefix = ''
        self.source_id = None
        self.source_name = None
        self.destination_id = None
        self.destination_name = None
        self.sync_catalog = {}  # sync_catalog['streams'] is a list of dicts {stream:, config:}
        self.schedule = {}
        self.status = 'active'

    def to_payload(self):
        r = {}
        r['connectionId'] = self.connection_id
        r['sourceId'] = self.source_id
        r['destinationId'] = self.destination_id
        r['name'] = self.name
        r['prefix'] = self.prefix
        r['schedule'] = self.schedule
        r['status'] = self.status
        r['syncCatalog'] = self.sync_catalog
        return r


class ConnectionGroupDto:
    """
    Data transfer object class for connection groups, each one representing a set of connections
    Note, Airbyte does not have this abstraction internally

    ConnectionGroupDto also does not have a to_payload method, as it will never need to be written to .yml,
    or interact directly with the client, only read. Instead, to_incomplete_connection_dict sends the
    relevant info for making a new ConnectionDto to a dict.
    """

    def __init__(self):
        self.group_name = None
        self.prefix = ''
        self.source_tags = None
        self.destination_tags = None
        self.sync_catalog = {}  # sync_catalog['streams'] is a list of dicts {stream:, config:}
        self.schedule = {}
        self.status = 'active'

    def to_incomplete_connection_dict(self):
        """
        This function returns what AirbyteDtoFactory.build_connection_dto craves
        """
        r = {
            'name': self.group_name,
            'prefix': self.prefix,
            'syncCatalog': self.sync_catalog,
            'schedule': self.schedule,
            'status': self.status
        }
        return r


class StreamDto:
    """
    Data transfer object class for the stream, belongs to the connection abstraction
    """

    def __init__(self):
        self.name = None
        self.json_schema = {}
        self.supported_sync_modes = []
        self.source_defined_cursor = None
        self.default_cursor_field = []
        self.source_defined_primary_key = []
        self.namespace = None


class StreamConfigDto:
    """
    Data transfer object class for the stream configuration, belongs to the connection abstraction
    """

    def __init__(self):
        self.sync_mode = None
        self.cursor_field = []
        self.destination_sync_mode = None
        self.primary_key = []
        self.alias_name = None
        self.selected = None


class WorkspaceDto:
    """
    Data transfer object class for Workspace-type Airbyte abstractions
    """

    def __init__(self):
        pass


class AirbyteDtoFactory:
    """
    Builds data transfer objects, each modeling an abstraction inside Airbyte
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
                    elif 'credentials_json' in destination.connection_configuration:
                        destination.connection_configuration['credentials_json'] = \
                        secrets['destinations'][destination.destination_name]['credentials_json']

    def build_source_dto(self, source: dict) -> SourceDto:
        """
        Builds a SourceDto object from a dict representing a source
        """
        r = SourceDto()
        if 'connectionConfiguration' in source:
            r.connection_configuration = source['connectionConfiguration']
        r.name = source['name']
        r.source_name = source['sourceName']
        if 'sourceDefinitionId' in source:
            r.source_definition_id = source['sourceDefinitionId']
        else:
            for definition in self.source_definitions['sourceDefinitions']:
                if r.source_name == definition['name']:
                    r.source_definition_id = definition['sourceDefinitionId']
            # TODO: handle exception where no sourceDefinitionId matches the provided source name
        if 'sourceId' in source:
            r.source_id = source['sourceId']
        if 'workspaceId' in source:
            r.workspace_id = source['workspaceId']
        if 'tags' in source:
            r.tags = source['tags']
        return r

    def build_destination_dto(self, destination):
        """
        Builds a DestinationDto object from a dict representing a source
        """
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
        if 'tags' in destination:
            r.tags = destination['tags']
        return r

    def build_connection_dto(self, connection):
        """
        Builds a ConnectionDto from a dict representing a connection
        """
        r = ConnectionDto()
        if 'prefix' in connection:
            r.prefix = connection['prefix']
        if 'connectionId' in connection:  # => connection is already defined in an Airbyte deployment
            r.connection_id = connection['connectionId']
        if 'sourceId' in connection:
            r.source_id = connection['sourceId']
        if 'sourceName' in connection:
            r.source_name = connection['sourceName']
        if 'destinationId' in connection:
            r.destination_id = connection['destinationId']
        if 'destinationName' in connection:
            r.destination_name = connection['destinationName']
        if 'name' in connection:
            r.name = connection['name']
        if 'syncCatalog' in connection:
            r.sync_catalog = connection['syncCatalog']
        r.schedule = connection['schedule']
        r.status = connection['status']
        return r
    
    def build_connection_group_dto(self, connection_group):
        """
        Builds a ConnectionGroupDto from a dict representing a connection_group
        Note: unlike the other DTO classes, ConnectionGroupDto doesn't represent an abstraction inside Airbyte
        """
        r = ConnectionGroupDto()
        r.group_name = connection_group['groupName']
        if 'syncCatalog' in connection_group:
            r.sync_catalog = connection_group['syncCatalog']
        r.schedule = connection_group['schedule']
        r.status = connection_group['status']
        r.source_tags = connection_group['sourceTags']
        r.destination_tags = connection_group['destinationTags']
        r.prefix = connection_group['prefix']
        return r
