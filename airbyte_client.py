import requests

RESPONSE_CODES = {
    200: "Operation successful",
    204: "The resource was deleted successfully",
    404: "Resource not found",
    422: "Invalid input",
}


class AirbyteResponse:
    def __init__(self, response):
        if response.status_code in RESPONSE_CODES:
            self.message = RESPONSE_CODES[response.status_code]
        else:
            self.message = "Unrecognized response code"
        self.payload = response.json()
        self.ok = response.ok
        # TODO: include the full response object


class AirbyteClient:
    """Airbyte interface"""
    def __init__(self, url):
        self.airbyte_url = url.strip('/') + '/'  # TODO: a little awk

    def get(self, relative_url) -> AirbyteResponse:
        route = self.airbyte_url + relative_url
        r = requests.get(route)
        return AirbyteResponse(r)

    def post(self, relative_url, payload) -> AirbyteResponse:
        route = self.airbyte_url + relative_url
        r = requests.post(route, payload)
        return AirbyteResponse(r)

    def get_workspace_by_slug(self, slug='default'):
        """Route: POST /v1/workspaces/get_by_slug"""
        return self.post('api/v1/workspaces/get_by_slug', dict(slug=slug))

    def get_workspace_by_id(self, workspace_uuid):
        """Route: POST /v1/workspaces/get"""
        uuid = None
        route = self.airbyte_url + 'api/v1/workspaces/get'
        r = requests.post(route, json={"workspace_id": uuid})
        return AirbyteResponse(r)

    def list_workspaces(self):
        """Route: /v1/workspaces/list"""
        return self.post('api/v1/workspaces/list', payload={})

    def get_source_definitions(self):
        """Route: /v1/source_definitions/list"""
        route = self.airbyte_url + 'api/v1/source_definitions/list'
        r = requests.post(route)
        return AirbyteResponse(r)

    def get_source_definition_connection_spec(self, source_definition_id):
        """Route: /v1/source_definition_specifications/get"""
        route = self.airbyte_url + 'api/v1/source_definition_specifications/get'
        r = requests.post(route, json={'sourceDefinitionId': source_definition_id})
        return AirbyteResponse(r)

    def get_destination_definitions(self):
        """Route: /v1/destination_definitions/list"""
        route = self.airbyte_url + 'api/v1/destination_definitions/list'
        r = requests.post(route)
        return AirbyteResponse(r)

    def check_source_connection(self, source_dto):
        """Route: POST /v1/sources/check_connection"""
        route = self.airbyte_url + 'api/v1/sources/check_connection'
        payload = {'sourceId': source_dto.source_id}
        r = requests.post(route, json=payload)
        if r.status_code == '404':
            print(source_dto.source_id + ': Unable to validate, source not found')
        return AirbyteResponse(r)

    def create_source(self, source_dto, workspace) -> AirbyteResponse:
        """ Route: POST /v1/sources/create"""
        route = self.airbyte_url + 'api/v1/sources/create'
        connection_spec = self.get_source_definition_connection_spec(source_dto.source_definition_id)
        payload = {'sourceDefinitionId': source_dto.source_definition_id,
                   'workspaceId': workspace['workspaceId'],
                   'connectionConfiguration': source_dto.connection_configuration,
                   'name': source_dto.name}
        r = requests.post(route, json=payload)
        return AirbyteResponse(r)

    def delete_source(self, source_dto):
        """Route: POST /v1/sources/delete"""
        route = self.airbyte_url + 'api/v1/sources/delete'
        payload = {'sourceId': source_dto.source_id}
        print("Deleting source: " + source_dto.source_id)
        r = requests.post(route, json=payload)
        return r.ok  # TODO: should return an AirbyteResponse, but Airbyte API returns a different type for this route

    def get_configured_sources(self, workspace):
        """Route: POST /v1/sources/list"""
        route = self.airbyte_url + 'api/v1/sources/list'
        r = requests.post(route, json={'workspaceId': workspace['workspaceId']})
        return AirbyteResponse(r)

    def update_source(self, source_dto):
        """Route: POST /v1/sources/update"""
        route = self.airbyte_url + 'api/v1/sources/update'
        payload = {'sourceId': source_dto.source_id,
                   'connectionConfiguration': source_dto.connection_configuration,
                   'name': source_dto.name}
        r = requests.post(route, json=payload)
        return AirbyteResponse(r)

    def discover_source_schema(self, source_dto):
        """Route: POST /v1/sources/discover_schema"""
        route = self.airbyte_url + 'api/v1/sources/discover_schema'
        payload = {'sourceId': source_dto.source_id}
        r = requests.post(route, json=payload)
        return AirbyteResponse(r)

    def check_destination_connection(self, destination_dto):
        """Route: POST /v1/destinations/check_connection"""
        route = self.airbyte_url + 'api/v1/destinations/check_connection'
        payload = {'destinationId': destination_dto.destination_id}
        r = requests.post(route, json=payload)
        if r.status_code == '404':
            print(destination_dto.destination_id + ': Unable to validate, destination not found')
        return AirbyteResponse(r)

    def create_destination(self, destination_dto, workspace):
        """ Route: POST /v1/destinations/create"""
        route = self.airbyte_url + 'api/v1/destinations/create'
        payload = {'destinationDefinitionId': destination_dto.destination_definition_id,
                   'workspaceId': workspace['workspaceId'],
                   'connectionConfiguration': destination_dto.connection_configuration,
                   'name': destination_dto.name}
        r = requests.post(route, json=payload)
        return AirbyteResponse(r)

    def delete_destination(self, destination_dto):
        """Route: POST /v1/destinations/delete"""
        route = self.airbyte_url + 'api/v1/destinations/delete'
        payload = {'destinationId': destination_dto.destination_id}
        print("Deleting destination: " + destination_dto.destination_id)
        r = requests.post(route, json=payload)
        return r.ok  # TODO: should return an AirbyteResponse, but Airbyte API returns a different type for this route

    def list_destinations(self):
        """Route: POST /v1/destinations/list"""
        pass

    def get_configured_destinations(self, workspace):
        """Route: POST /v1/destinations/list"""
        route = self.airbyte_url + 'api/v1/destinations/list'
        r = requests.post(route, json={'workspaceId': workspace['workspaceId']})
        return AirbyteResponse(r)

    def update_destination(self, destination_dto):
        """Route: POST /v1/destinations/update"""
        route = self.airbyte_url + 'api/v1/destinations/update'
        payload = {'destinationId': destination_dto.destination_id,
                   'connectionConfiguration': destination_dto.connection_configuration,
                   'name': destination_dto.name}
        r = requests.post(route, json=payload)
        return AirbyteResponse(r)

    def create_connection(self, connection_dto, source_dto):
        """Route: POST /v1/connections/create"""
        route = self.airbyte_url + 'api/v1/connections/create'
        if not connection_dto.sync_catalog:
            source_schema = self.discover_source_schema(source_dto)  # TODO: test
            connection_dto.sync_catalog = source_schema
        payload = {
            'name': connection_dto.name,
            'prefix': connection_dto.prefix,
            'sourceId': connection_dto.source_id,
            'destinationId': connection_dto.destination_id,
            'syncCatalog': connection_dto.sync_catalog,
        }
        if connection_dto.schedule:
            payload['syncCatalog']['schedule'] = connection_dto.schedule
        r = requests.post(route, json=payload)
        return AirbyteResponse(r)

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
        return AirbyteResponse(r)