class AirbyteConfigModel:
    def __init__(self):
        self.sources = {}
        self.destinations = {}
        self.connections = {}
        self.workspaces = {}
        self.global_config = {}

    def write_model_to_yaml(self, filename):
        pass

    def apply_model_to_deployment(self, client):
        pass

    def full_wipe(self, client):
        self.wipe_sources(client)
        self.wipe_destinations(client)
        # self.wipe_connections(client)
        pass

    def wipe_sources(self, client):
        removed = []
        for source in self.sources.values():
            if client.delete_source(source) == 204:
                removed.append(source.source_id)
            else:
                print("AirbyteConfigModel.wipe_sources : Unable to delete source: " + repr(source))
        for source_id in removed:
            self.sources.pop(source_id)

    def wipe_destinations(self, client):
        removed = []
        for destination in self.destinations.values():
            if client.delete_destination(destination) == 204:
                removed.append(destination.destination_id)
            else:
                print("AirbyteConfigModel.wipe_destinations : Unable to delete destination: " + repr(destination))
        for destination_id in removed:
            self.destinations.pop(destination_id)