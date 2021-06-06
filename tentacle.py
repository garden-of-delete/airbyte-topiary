#!/usr/bin/env python3
"""
This is a simple, open-source tool designed to help manage Airbyte deployments at scale through via the API.

TODO LIST:
- (done) Implement "check" routes for source and destination validation
- Implement the connection routes, dto class, and all associated functions
- Finalize the yaml to deployment workflow
    - (done) Add print statements to create_source and create_destination
    - Add ability for user to override workspace slug
    - Address modification of existing sources and destinations
    - Print logable info to stdout
    - Add a function to each dto to enable self-validation
    - Clarify all arg processor functions related to this workflow
    - implement validate changes option
    - (Stretch): modification of connections
- Restructure main method
- Update deployment workflow
- Deployment to yaml workflow
- Deployment to deployment workflow
- Wipe target workflow
- Readme
- License
- Tests!
- Post 0.1.0
    - Linter?
    - Multiple workspaces
"""

__author__ = "Robert Stolz"
__version__ = "0.1.0"
__license__ = "MIT"

import argparse
import yaml
from airbyte_client import AirbyteClient
from airbyte_config_model import AirbyteConfigModel
from airbyte_dto_factory import AirbyteDtoFactory

def main(args):
    """ Main entry point of the app """

    airbyte_model = AirbyteConfigModel()
    if args.source.strip().split('.')[-1] == 'yml' or args.source.strip().split('.')[-1] == 'yaml':
        client = AirbyteClient(args.destination)
    else:
        client = AirbyteClient(args.source)
    # TODO: check to see if destination is also an airbyte deployment

    workspace = client.get_workspace_by_slug()

    #get source and destination definitions
    available_sources = client.get_source_definitions()
    available_destinations = client.get_destination_definitions()
    #initialize data transfer object factory
    dto_factory = AirbyteDtoFactory(available_sources, available_destinations)

    #get config from config.yml
    yaml_config = yaml.safe_load(open("config.yml", 'r'))
    secrets = yaml.safe_load(open("secrets.yml", 'r'))
    new_dtos = dto_factory.build_dtos_from_yaml_config(yaml_config, secrets)

    # get configured connectors and connections from Airbyte API
    configured_sources = client.get_configured_sources(workspace)
    configured_destinations = client.get_configured_destinations(workspace)
    configured_connections = client.get_configured_connections(workspace)

    # send configured_sources to the factory to build sourceDtos
    for source in configured_sources:
        source_dto = dto_factory.build_source_dto(source)
        airbyte_model.sources[source_dto.source_id] = source_dto
    for destination in configured_destinations:
        destination_dto = dto_factory.build_destination_dto(destination)
        airbyte_model.destinations[destination_dto.destination_id] = destination_dto
    for connection in configured_connections:
        connection_dto = dto_factory.build_connection_dto(connection)
        airbyte_model.connections[connection_dto.connection_id] = connection_dto

    # sync yaml to deployment
    if args.wipe:
        airbyte_model.full_wipe(client)

    for new_source in new_dtos['sources']:
        if new_source.source_id is None:
            response = client.create_source(new_source)
            source_dto = dto_factory.build_source_dto(response)
            airbyte_model.sources[new_source.source_id] = new_source
        else:
            pass  # TODO: modify existing source
    for destination in new_dtos['destinations']:
        if destination.destination_id is None:
            response = client.create_destination(destination)
            destination_dto = dto_factory.build_destination_dto(response)
            airbyte_model.destinations[destination_dto.destination_id] = destination_dto
        else:
            pass  # TODO: modify existing destination

    # validate changes
    if args.validate or args.mode == 'validate':
        airbyte_model.validate(client)

    # sync deployment to yaml
    # airbyte_model.write_to_yaml()

    # deploment to deployment

    pass

if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Required positional argument
    #parser.add_argument("arg", help="Required positional argument")
    parser.add_argument("mode", help="Operating mode. Choices are sync, sync-all, wipe, update")
    parser.add_argument("source", help="location of the source Airbyte deployment or yaml file")

    # Optional argument flag which defaults to False
    parser.add_argument("-w", "--wipe", action="store_true", default=False,
                        help="deletes all connectors on the target")
    parser.add_argument("-v", "--validate", action="store_true", default=False,
                        help="validates all connectors on the destination after applying changes")

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("-d", "--destination", action="store", dest="destination",
                        help="specifies the airbyte deployment or yaml file to sync ")
    parser.add_argument("-a", "--sync-all", action="store_true", default=False)

    # Specify output of "--version"
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s (version {version})".format(version=__version__))

    args = parser.parse_args()
    main(args)
