import yaml


class AirbyteConfigModel:
    def __init__(self):
        self.sources = {}
        self.destinations = {}
        self.connections = {}
        self.workspaces = {}
        self.global_config = {}

    def write_yaml(self, filename):
        payload = {'sources': [source.to_payload() for source in self.sources.values()],
                   'destinations': [destination.to_payload() for destination in self.destinations.values()],
                   'connections': [connection.to_payload() for connection in self.connections.values()]}
        with open(filename, 'w') as yaml_file:
            yaml.safe_dump(payload, yaml_file)

    def apply_to_deployment(self, client):
        pass

    def wipe_sources(self, client):
        """Removes all sources in self.sources from the deployment and the model"""
        # TODO: delete_sources would ideally return an AirbyteResponse and not a bool.
        removed = []
        for source in self.sources.values():
            if client.delete_source(source):
                removed.append(source.source_id)
            else:
                print("AirbyteConfigModel.wipe_sources : Unable to delete source: " + repr(source))
        for source_id in removed:
            self.sources.pop(source_id)

    def wipe_destinations(self, client):
        """Removes all destinations in self.destinations from deployment and the model"""
        removed = []
        for destination in self.destinations.values():
            if client.delete_destination(destination):
                removed.append(destination.destination_id)
            else:
                print("AirbyteConfigModel.wipe_destinations : Unable to delete destination: " + repr(destination))
        for destination_id in removed:
            self.destinations.pop(destination_id)

    def validate(self, client):
        """this function validates the model and all included connectors"""
        print("Validating connectors...")
        for source in self.sources.values():
            response = client.check_source_connection(source).payload
            print("Source is valid: " + source.source_id + ' ' + repr(response['jobInfo']['succeeded']))
        for destination in self.destinations.values():
            response = client.check_destination_connection(destination).payload
            print("Destination is valid: " + destination.destination_id + ' ' + repr(response['jobInfo']['succeeded']))
        for connection in self.connections.values():
            pass  # TODO: implement connection validation