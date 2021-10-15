import yaml

SOURCE_FIELDS = {  # {fieldName: required}
    "sourceId": False,
    "name": True,
    "sourceName": True,
    "connectionConfiguration": True,
    "syncCatalog": False
}

DESTINATION_FIELDS = {  # {fieldName: required}
    "destinationId": False,
    "name": True,
    "destinationName": True,
    "connectionConfiguration": True,
}

CONNECTION_FIELDS = {  # {fieldName: required}
    "sourceName": True,
    "destinationName": True,
    "prefix": False,
    "namespaceDefinition": False,
    "schedule": True,
    "status": True,
    "syncCatalog": False
}


class ConfigException(Exception):
    pass


class SourceConfigException(ConfigException):
    pass


class DestinationConfigException(ConfigException):
    pass


class ConnectionConfigException(ConfigException):
    pass


class ConfigLoader:

    def __init__(self):
        self.good_config = True

    def load_config(self, yaml_config):
        config = yaml.safe_load(yaml_config)
        if 'sources' in config:
            for source in config['sources']:
                try:
                    self.check_source(source)
                except SourceConfigException as e:
                    self.good_config = False
                    print(e.message)
        if 'destinations' in config:
            for destination in config['destinations']:
                self.check_destination(destination)
        if 'connections' in config:
            if not ('sources' in config and 'destinations' in config):
                print("Error: Connections can't be defined without at least one source and one destination")

    def load_secrets(self, yaml_secrets):
        pass

    def check_source(self, source_dict):
        # check that minimums are provided
        pass

    def check_destination(self, destination_dict):
        pass

    def check_connection(self, connection_dict):
        pass

    def check_required(self, input_dict, required_list):
        return set(required_list.keys()).issubset(set(input_dict.keys()))

    def check_types(self, input_dict, expected_type):
        pass
