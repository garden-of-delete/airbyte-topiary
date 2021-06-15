#!/usr/bin/env python3
"""
This is a simple, open-source tool designed to help manage Airbyte deployments at scale through via the API.

TODO LIST:
- (done) Implement "check" routes for source and destination validation
- (done) Drop description and code supporting the deployment to deployment use case
- Implement the connection routes, dto class, and all associated functions
- (done) Restructure main method as a proper controller. All feedback to user should come from controller
- Finalize the yaml to deployment workflow
    - (done) Add print statements to create_source and create_destination
    - (done) Add ability for user to override workspace slug
    - Address modification of existing sources and destinations
    - (done) Clarify all arg processor functions related to this workflow
    - implement the --dump option
    - (stretch): modification of connections
- Deployment to yaml workflow
- (done) Wipe target workflow
- (in progress) README.md
- (done) License
- Decorators
- Tests!
- Post 0.1.0
    - CI
    - Linter
    - Support for multiple workspaces
    - Update deployment workflow
    - Better management of multiple sets of credentials / better secrets management in general
"""

__author__ = "Robert Stolz"
__version__ = "0.1.0"
__license__ = "MIT"

import argparse
import utils
from controller import Controller


VALID_MODES = ['wipe', 'validate', 'sync']


def main(args):
    """Handles arguments and setup tasks. Invokes controller methods to carry out the specified workflow"""
    # setup
    controller = Controller()
    client = controller.instantiate_client(args)
    definitions = controller.get_definitions(client)
    controller.instantiate_dto_factory(definitions['source_definitions'], definitions['destination_definitions'])
    workspace = controller.get_workspace(args, client)
    print("main: read configuration from source yaml")
    airbyte_model = controller.get_airbyte_configuration(client, workspace)

    # sync workflow
    if args.mode == 'sync':
        if utils.is_yaml(args.target):
            airbyte_model.write_yaml(args.target)
        else:
            yaml_config, secrets = controller.read_yaml_config(args)
            new_dtos = controller.build_dtos_from_yaml_config(yaml_config, secrets)
            if args.wipe:
                controller.wipe_all(airbyte_model, client)
            print("Applying changes to deployment: " + client.airbyte_url)
            if args.sources or args.all:
                controller.sync_sources(airbyte_model, client, workspace, new_dtos)
            if args.destinations or args.all:
                controller.sync_destinations(airbyte_model, client, workspace, new_dtos)
            if args.connections or args.all:
                pass  # TODO: implement controller.sync_connection
            if args.validate:
                airbyte_model.validate(client)

    # wipe workflow
    elif args.mode == 'wipe':
        if args.sources or args.all:
            controller.wipe_sources(airbyte_model, client)
        if args.destinations or args.all:
            controller.wipe_destinations(airbyte_model, client)
        if args.connections or args.all:
            pass  # TODO: implement controller.wipe_connections

    # validate workflow
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
    parser.add_argument("mode", help="Operating mode. Choices are sync, validate, wipe")
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
