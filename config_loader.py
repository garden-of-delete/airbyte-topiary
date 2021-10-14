import yaml

class Field:

    def __init__(self, name_ = None, required_ = None):
        self.name = name_
        self.required = required_


SOURCE_FIELDS = [  # (fieldName, required)
    ("sourceId", False),
    ("name", True),
    ("SourceName", True),
    ("connectionConfiguration", True),
    ("syncCatalog", False),
]

DESTINATION_FIELDS = [  # (fieldName, required)
    ("destinationId", False),
    ("name", True),
    ("destinationName", True),
    ("connectionConfiguration", True),
]

CONNECTION_FIELDS = [  # (fieldName, required)

]

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

    def check_required(self, input_dict):
        pass

    def check_unique(self, input_dict):
        pass

    def check_types(self, input_dict, expected_type):
        pass
