SOURCE_FIELDS = {  # {fieldName: is_required}
    "sourceId": False,
    "name": True,
    "sourceName": True,
    "connectionConfiguration": True,
    "syncCatalog": False
}

DESTINATION_FIELDS = {  # {fieldName: is_required}
    "destinationId": False,
    "name": True,
    "destinationName": True,
    "connectionConfiguration": True,
}

CONNECTION_FIELDS = {  # {fieldName: is_required}
    "sourceName": True,
    "destinationName": True,
    "prefix": False,
    "namespaceDefinition": False,
    "schedule": True,
    "status": True,
    "syncCatalog": False
}


class ConfigValidator:

    def __init__(self):
        self.config_is_good = True

    def validate_config(self, config: dict) -> bool:
        """Validates that a config read from yaml is valid. Validity here means:
            - all sources have required information specified (see above or Airbyte API docs)
            - all destinations have required information specified
            - all connections refer to a valid source and a valid destination
        Not to be confused with the unrelated --validate command line argument
        """

        if 'sources' in config:
            for source in config['sources']:
                if not self.check_source(source):  # if source is not valid
                    print("Error: invalid source")
                    print(repr(source))
                    self.config_is_good = False
        if 'destinations' in config:
            for destination in config['destinations']:
                if not self.check_destination(destination):
                    print("Error: invalid destination")
                    print(repr(destination))
                    self.config_is_good = False
        if 'connections' in config and not ('sources' in config and 'destinations' in config):
            print("Error: Connections can't be defined without at least one valid source and one valid destination")
            self.config_is_good = False
        return self.config_is_good

    def check_secrets(self, secrets):  # TODO: On hold for secrets v2
        pass

    def check_source(self, source_dict) -> bool:
        return self.check_required(source_dict, SOURCE_FIELDS)

    def check_destination(self, destination_dict) -> bool:
        return self.check_required(destination_dict, DESTINATION_FIELDS)

    def check_connection(self, connection_dict) -> bool:
        return self.check_required(connection_dict, CONNECTION_FIELDS)

    def check_required(self, input_dict: dict, parameters: dict) -> bool:
        """
        Checks that required parameters are a subset of provided parameters for the given source or destination
        """

        test_a = set([x for x in parameters.keys() if parameters[x] is True])
        test_b = set(input_dict.keys())
        result = test_a.issubset(test_b)
        return result
