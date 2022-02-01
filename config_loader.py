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


class ConfigLoader:

    def __init__(self):
        self.config_is_good = True

    def load_config(self, yaml_config):
        config = yaml.safe_load(yaml_config)
        if 'sources' in config:
            for source in config['sources']:
                self.check_source(source)
        if 'destinations' in config:
            for destination in config['destinations']:
                self.check_destination(destination)
        if 'connections' in config:
            if not ('sources' in config and 'destinations' in config):
                print("Error: Connections can't be defined without at least one source and one destination")

    def load_secrets(self, yaml_secrets):  # TODO: implement
        pass

    def check_source(self, source_dict) -> bool:
        return self.check_required(source_dict, SOURCE_FIELDS)

    def check_destination(self, destination_dict) -> bool:
        return self.check_required(destination_dict, DESTINATION_FIELDS)

    def check_connection(self, connection_dict) -> bool:
        return self.check_required(connection_dict, CONNECTION_FIELDS)

    def check_required(self, input_dict: dict, parameters) -> bool:
        test_a = set([x for x in parameters.keys() if parameters[x] is True])
        test_b = set(input_dict.keys())
        result = test_a.issubset(test_b)
        return result

    def check_types(self, input_dict, expected_type):
        pass
