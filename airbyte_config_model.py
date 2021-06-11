import yaml

class AirbyteConfigModel:
    def __init__(self):
        self.sources = {}
        self.destinations = {}
        self.connections = {}
        self.workspaces = {}
        self.global_config = {}

    def write_yaml(self, filename):
        pass

    def apply_to_deployment(self, client):
        pass

    def full_wipe(self, client):
        self.wipe_sources(client)
        self.wipe_destinations(client)
        # self.wipe_connections(client)
        pass

    def wipe_sources(self, client):
        """Removes all sources in self.sources from deployment and the object"""
        # TODO: delete_sources would ideally return an AirbyteResponse and not a bool
        removed = []
        for source in self.sources.values():
            if client.delete_source(source):
                removed.append(source.source_id)
            else:
                print("AirbyteConfigModel.wipe_sources : Unable to delete source: " + repr(source))
        for source_id in removed:
            self.sources.pop(source_id)

    def wipe_destinations(self, client):
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
        for source in self.sources.values():
            response = client.check_source_connection(source).payload
            print("Source is valid: " + response['jobInfo']['id'] + ' ' + repr(response['jobInfo']['succeeded']))
        for destination in self.destinations.values():
            response = client.check_destination_connection(destination).payload
            print("destination is valid: " + response['jobInfo']['id'] + ' ' + repr(response['jobInfo']['succeeded']))
            pass