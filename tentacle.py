#!/usr/bin/env python3
"""
This is a simple, open-source tool designed to help manage Airbyte deployments at scale through via the API.

TODO LIST:
- (done) Implement "check" routes for source and destination validation
- (done) Drop description and code supporting the deployment to deployment use case
- Implement the connection routes, dto class, and all associated functions
- (partially done) Restructure main method as a proper controller. All feedback to user should come from controller
- Finalize the yaml to deployment workflow
    - (done) Add print statements to create_source and create_destination
    - (done) Add ability for user to override workspace slug
    - Address modification of existing sources and destinations
    - (done) Clarify all arg processor functions related to this workflow
    - implement validate changes option
    - implement the --dump option
    - (stretch): modification of connections
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
from controller import Controller


VALID_MODES = ['wipe', 'update', 'validate', 'sync']

def is_origin_yaml(args):
    if args.origin.strip().split('.')[-1] == 'yml' or args.origin.strip().split('.')[-1] == 'yaml':
        return True
    else:
        return False


def is_target_yaml(args):
    if args.origin.strip().split('.')[-1] == 'yml' or args.origin.strip().split('.')[-1] == 'yaml':
        return False
    else:
        return True


def instantiate_client(args):  # TODO: Move into controller
    # if in sync mode and source is a yaml file
    if is_origin_yaml(args):
        if is_target_yaml(args):
            print("Fatal error: --target must be followed by a valid "
                  "Airbyte deployment url when the origin is a .yaml file")
            exit(2)
        client = AirbyteClient(args.target)
    elif is_target_yaml(args):
        if is_origin_yaml(args):
            print("Fatal error: --target must be followed by a valid "
                  "Airbyte deployment url when the origin is a .yaml file")
            exit(2)
        client = AirbyteClient(args.origin)
    else:
        print("Fatal error: the origin or --target must be a valid .yaml configuration file")
        exit(2)
    return client


def read_yaml_config(args):  # TODO: Move into controller
    """get config from config.yml"""
    if is_origin_yaml(args):
        yaml_config = yaml.safe_load(open(args.origin, 'r'))
    else:
        yaml_config = yaml.safe_load(open(args.target, 'r'))
    secrets = yaml.safe_load(open(args.secrets, 'r'))
    return yaml_config, secrets


def main(args):
    """ Main entry point of the app. Chooses the correct controller workflow"""
    client = instantiate_client(args)
    controller = Controller()
    controller.get_definitions(client)
    dto_factory = AirbyteDtoFactory(client.get_source_definitions().payload,
                                    client.get_destination_definitions().payload)  # TODO: should the controller own dto_factory?
    yaml_config, secrets = read_yaml_config(args)
    workspace = controller.get_workspace(args, client)
    print("main: read configuration from source yaml")
    airbyte_model = AirbyteConfigModel()
    configured_objects = controller.get_airbyte_configuration(client, workspace)
    new_dtos = dto_factory.build_dtos_from_yaml_config(yaml_config, secrets)  # TODO: move into controller?

    for source in configured_objects['sources']:  # TODO: this block probably belings in a controller function
        source_dto = dto_factory.build_source_dto(source)
        airbyte_model.sources[source_dto.source_id] = source_dto
    for destination in configured_objects['destinations']:
        destination_dto = dto_factory.build_destination_dto(destination)
        airbyte_model.destinations[destination_dto.destination_id] = destination_dto
    for connection in configured_objects['connections']:
        connection_dto = dto_factory.build_connection_dto(connection)
        airbyte_model.connections[connection_dto.connection_id] = connection_dto

    if args.mode == 'sync':
        if args.wipe:
            print("Wiping deployment: " + client.airbyte_url)
            airbyte_model.full_wipe(client)
        print("Applying changes to deployment: " + client.airbyte_url)
        if args.sources or args.all:
            controller.sync_sources(airbyte_model, client, workspace, dto_factory, new_dtos)  # TODO: simplify this function arg list by moving dto_factory into controller
        if args.destinations or args.all:
            controller.sync_destinations(airbyte_model, client, workspace, dto_factory, new_dtos)
        if args.connections or args.all:
            pass
        if args.validate:
            print("Validating connectors...")
            airbyte_model.validate(client)
        pass
    elif args.mode == 'wipe':
        print("Wiping deployment: " + client.airbyte_url)
        airbyte_model.full_wipe(client)
    elif args.mode == 'update':
        controller.update()  # TODO: implement the update workflow
    elif args.mode == 'validate':
        print("Validating connectors...")
        airbyte_model.validate(client)
    else:
        print("main: unrecognized mode " + args.mode)


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
