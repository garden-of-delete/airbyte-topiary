#!/usr/bin/env python3
"""
This is a simple, open-source tool designed to help manage Airbyte deployments at scale through via the API.

TODO LIST:
- (done) Implement "check" routes for source and destination validation
- (done) Drop description and code supporting the deployment to deployment use case
- Implement the connection routes, dto class, and all associated functions
- Finalize the yaml to deployment workflow
    - (done) Add print statements to create_source and create_destination
    - (done) Add ability for user to override workspace slug
    - Address modification of existing sources and destinations
    - (done) Clarify all arg processor functions related to this workflow
    - implement validate changes option
    - implement the --dump option
    - (Stretch): modification of connections
- (partially done) Restructure main method as a proper controller. All feedback to user should come from controller
- Update deployment workflow
- Deployment to yaml workflow
- Deployment to deployment workflow
- (done) Wipe target workflow
- (in progress) Readme
- License
- Tests!
- Post 0.1.0
    - Linter?
    - Support for multiple workspaces
    - Update deployment workflow
    - Better management of multiple sets of credentials / better secrets management in general
"""

__author__ = "Robert Stolz"
__version__ = "0.1.0"
__license__ = "MIT"

import argparse
import yaml
from airbyte_client import AirbyteClient
from airbyte_config_model import AirbyteConfigModel
from airbyte_dto_factory import AirbyteDtoFactory

VALID_MODES = ['wipe', 'update', 'validate', 'sync']

def main(args):
    """ Main entry point of the app """
    airbyte_model = AirbyteConfigModel()
    if args.mode == 'sync':
        # if in sync mode and source is a yaml file
        if args.origin.strip().split('.')[-1] == 'yml' or args.origin.strip().split('.')[-1] == 'yaml':
            if args.target is None or \
                    args.target.strip().split('.')[-1] == 'yml' or \
                    args.target.strip().split('.')[-1] == 'yaml':
                print("Fatal error: --destination must be followed by a valid "
                      "Airbyte deployment url when the origin is a .yaml file")
                exit(2)
            client = AirbyteClient(args.target)
        else:  # in sync mode and source is not a yaml file
            client = AirbyteClient(args.origin)
    else:  # not in sync mode
        client = AirbyteClient(args.origin)

    if args.workspace_slug:
        workspace = client.get_workspace_by_slug(args.workspace_slug).payload
    else:
        workspace = client.get_workspace_by_slug().payload

    # get source and destination definitions
    available_sources = client.get_source_definitions().payload
    available_destinations = client.get_destination_definitions().payload
    # initialize data transfer object factory
    dto_factory = AirbyteDtoFactory(available_sources, available_destinations)
    print("main: retrieved source and destination definitions from: " + client.airbyte_url)

    # get config from config.yml
    yaml_config = yaml.safe_load(open("config.yml", 'r'))
    secrets = yaml.safe_load(open("secrets.yml", 'r'))
    new_dtos = dto_factory.build_dtos_from_yaml_config(yaml_config, secrets)
    print("main: read configuration from source yaml")

    # get configured connectors and connections from Airbyte API
    configured_sources = client.get_configured_sources(workspace).payload
    configured_destinations = client.get_configured_destinations(workspace).payload
    configured_connections = client.get_configured_connections(workspace).payload
    print("main: retrieved configuration from: " + client.airbyte_url)

    '''
    # DEBUG
    dump = {"sources": configured_sources, "destinations": configured_destinations, "connections": configured_connections}
    outfile = open("config_dump.yml", 'w')
    yaml.safe_dump({'sources': configured_sources}, outfile)
    yaml.safe_dump({'destinations': configured_destinations}, outfile)
    yaml.safe_dump({'connections': configured_connections}, outfile)
    '''

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
    if args.wipe or args.mode == 'wipe':
        print("Wiping deployment: " + client.airbyte_url)
        airbyte_model.full_wipe(client)

    print("Applying changes to deployment: " + client.airbyte_url)
    if args.sources or args.all:
        for new_source in new_dtos['sources']:
            if new_source.source_id is None:
                response = client.create_source(new_source, workspace)
                source_dto = dto_factory.build_source_dto(response)
                airbyte_model.sources[new_source.source_id] = new_source
            else:
                pass  # TODO: modify existing source
    if args.destinations or args.all:
        for new_destination in new_dtos['destinations']:
            if new_destination.destination_id is None:
                response = client.create_destination(new_destination, workspace)
                destination_dto = dto_factory.build_destination_dto(response)
                airbyte_model.destinations[destination_dto.destination_id] = destination_dto
            else:
                pass  # TODO: modify existing destination

    # validate
    if args.validate or args.mode == 'validate':
        print("Validating connectors...")
        airbyte_model.validate(client)

    # sync deployment to yaml

    # airbyte_model.write_to_yaml()

    # update workflow

    pass

if __name__ == "__main__":
    """ This is executed when run from the command line """
    parser = argparse.ArgumentParser()

    # Required positional argument
    #parser.add_argument("arg", help="Required positional argument")
    parser.add_argument("mode", help="Operating mode. Choices are sync, validate, wipe, update")
    parser.add_argument("origin", help="location of the source Airbyte deployment or yaml file")

    # Optional argument flag which defaults to False
    parser.add_argument("-s", "--sources", action="store_true", default=False,
                        help="sync sources")
    parser.add_argument("-d", "--destinations", action="store_true", default=False,
                        help="syncs destinations")
    parser.add_argument("-c", "--connections", action="store_true", default=False,
                        help="syncs connections")
    parser.add_argument("-a", "--all", action="store_true", default=False,
                        help="syncs sources, destinations, and connections")
    parser.add_argument("-w", "--wipe", action="store_true", default=False,
                        help="deletes all connectors on the target")
    parser.add_argument("-v", "--validate", action="store_true", default=False,
                        help="validates all connectors on the destination after applying changes")

    # Optional argument which requires a parameter (eg. -d test)
    parser.add_argument("--target", action="store", dest="target",
                        help="specifies the airbyte deployment or yaml file to modify")
    parser.add_argument("--dump", action="store", dest="dump_file",
                        help="specifies a .yaml file to dump the configuration of the destination before syncing")
    parser.add_argument("--secrets", action="store", dest="secrets",
                        help="specifies a .yaml file containing the secrets for each source and destination type")
    parser.add_argument("--workspace", action="store", dest="workspace_slug",
                        help="species the workspace name (slug). Allows use of a non-default workspace")
    # Specify output of "--version"
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s (version {version})".format(version=__version__))

    args = parser.parse_args()
    main(args)
