import requests

class AirbyteClient:
    """Airbyte interface"""
    def __init__(self, url):
        self.airbyte_url = url.strip('/') + '/'  # TODO: a little awkward
        self.workspace_slug = "default"
        self.workspace_uuid = self.get_workspace_by_slug(self.workspace_slug)['workspaceId']  # TODO: move workspace info to AirbyteClientModel

    def get_workspace_by_slug(self, slug='default'):
        """Route: POST /v1/workspaces/get_by_slug"""
        route = self.airbyte_url + 'api/v1/workspaces/get_by_slug'
        r = requests.post(route, json={"slug": "default"})
        return r.json()

    def get_workspace_by_id(self, workspace_uuid=None):
        """(LOW PRIO) Route: POST /v1/workspaces/get"""
        uuid = None
        if workspace_uuid:
            uuid = workspace_uuid
        elif self.workspace_uuid:
            uuid = self.workspace_uuid
        else:
            return
        route = self.airbyte_url + 'api/v1/workspaces/get'
        r = requests.post(route, json={"workspace_id": uuid})
        return r.json()
        pass

    def get_source_definitions(self):
        """Route: /v1/source_definitions/list"""
        route = self.airbyte_url + 'api/v1/source_definitions/list'
        r = requests.post(route)
        return r.json()

    def get_source_definition_spec(self, source_id):
        """Route: /v1/source_definition_specifications/get"""
        route = self.airbyte_url + 'api/v1/source_definition_specifications/get'
        r = requests.post(route, json={'sourceDefinitionId': source_id})
        return r.json()

    def get_destination_definitions(self):
        """Route: /v1/destination_definitions/list"""
        route = self.airbyte_url + 'api/v1/destination_definitions/list'
        r = requests.post(route)
        return r.json()

    def check_source_connection(self, source_dto):
        """Route: POST /v1/sources/check_connection"""
        route = self.airbyte_url + 'api/v1/sources/check_connection'
        payload = {'sourceId': source_dto.source_id}
        r = requests.post(route, json=payload)
        if r.status_code == '404':
            print(source_dto.source_id + ': Unable to validate, source not found')
        return r.json()

    def create_source(self, source_dto):
        """ Route: POST /v1/sources/create"""
        route = self.airbyte_url + 'api/v1/sources/create'
        payload = {'sourceDefinitionId': source_dto.source_definition_id,
                   'workspaceId': self.workspace_uuid,
                   'connectionConfiguration': source_dto.connection_configuration,
                   'name': source_dto.name}
        r = requests.post(route, json=payload)
        if r.status_code == 200:
            source_dto.source_id = r.json()['sourceId']
            print("Created source: " + r.json()['sourceId'])
            return r.json()
        elif r.status_code == 422:
            print("AirbyteClient.create_source : Invalid input")
        else:
            print("AirbyteClient.create_source : Unrecognized response code " + str(r.status_code))


    def delete_source(self, source_dto):
        """Route: POST /v1/sources/delete"""
        route = self.airbyte_url + 'api/v1/sources/delete'
        payload = {'sourceId': source_dto.source_id}
        print("Deleting source: " + source_dto.source_id)
        r = requests.post(route, json=payload)
        return r.status_code

    def get_configured_sources(self, workspace):
        """Route: POST /v1/sources/list"""
        route = self.airbyte_url + 'api/v1/sources/list'
        r = requests.post(route, json={'workspaceId': workspace['workspaceId']})
        test = r.json()
        return r.json()['sources']

    def update_source(self):
        """Route: POST /v1/sources/update"""
        pass

    def check_destination_connection(self, destination_dto):
        """Route: POST /v1/destinations/check_connection"""
        route = self.airbyte_url + 'api/v1/destinations/check_connection'
        payload = {'destinationId': destination_dto.destination_id}
        r = requests.post(route, json=payload)
        if r.status_code == '404':
            print(destination_dto.destination_id + ': Unable to validate, destination not found')
        return r.json()

    def create_destination(self, destination_dto):
        """ Route: POST /v1/destinations/create"""
        route = self.airbyte_url + 'api/v1/destinations/create'
        payload = {'destinationDefinitionId': destination_dto.destination_definition_id,
                   'workspaceId': self.workspace_uuid,
                   'connectionConfiguration': destination_dto.connection_configuration,
                   'name': destination_dto.name}
        r = requests.post(route, json=payload)
        if r.status_code == 200:
            destination_dto.destination_id = r.json()['destinationId']
            print("Created destination: " + r.json()['destinationId'])
            return r.json()
        elif r.status_code == 422:
            print("AirbyteClient.create_destination : Invalid input")
        else:
            print("AirbyteClient.create_destination : Unrecognized response code " + str(r.status_code))

    def delete_destination(self, destination_dto):
        """Route: POST /v1/destinations/delete"""
        route = self.airbyte_url + 'api/v1/destinations/delete'
        payload = {'destinationId': destination_dto.destination_id}
        print("Deleting destination: " + destination_dto.destination_id)
        r = requests.post(route, json=payload)
        return r.status_code

    def list_destinations(self):
        """Route: POST /v1/destinations/list"""
        pass

    def get_configured_destinations(self, workspace):
        """Route: POST /v1/destinations/list"""
        route = self.airbyte_url + 'api/v1/destinations/list'
        r = requests.post(route, json={'workspaceId': workspace['workspaceId']})
        return r.json()['destinations']

    def update_destination(self):
        """Route: POST /v1/destinations/update"""
        pass

    def create_connection(self):
        """Route: POST /v1/connections/create"""
        pass

    def delete_connection(self):
        """Route: POST /v1/connections/delete"""
        pass

    def reset_conection(self):
        """Route: POST /v1/connections/reset"""
        pass

    def sync_connection(self):
        """Route: POST /v1/connections/sync"""
        pass

    def update_connection(self):
        """Route: POST /v1/connections/update"""
        pass

    def get_connection_state(self):
        """Route: POST /v1/state/get"""
        pass

    def get_configured_connections(self, workspace):
        """Route: POST /v1/connections/list"""
        route = self.airbyte_url + 'api/v1/connections/list'
        r = requests.post(route, json={'workspaceId': workspace['workspaceId']})
        test = r.json()
        return r.json()['connections']