#!/usr/bin/env python3
"""
This is a simple, open-source tool designed to help manage Airbyte deployments through via the API.
"""

__author__ = "Robert Stolz"
__version__ = "0.1.0"
__license__ = "MIT"

import argparse
import utils
from airbyte_client import AirbyteClient
from airbyte_config_model import AirbyteConfigModel
from controller import Controller
from config_validator import ConfigValidator

VALID_MODES = ['wipe', 'validate', 'sync']


def main(args):
    """Handles arguments and setup tasks. Invokes controller methods to carry out the specified workflow"""
    controller: Controller = Controller()
    config_validator: ConfigValidator = ConfigValidator()
    client: AirbyteClient = controller.instantiate_client(args)
    definitions: dict = controller.get_definitions(client)
    controller.instantiate_dto_factory(definitions['source_definitions'], definitions['destination_definitions'])
    workspace: str = controller.get_workspace(args, client)
    airbyte_model: AirbyteConfigModel = controller.get_airbyte_configuration(client, workspace)

    # sync workflow
    if args.mode == 'sync':
        if utils.is_yaml(args.target):  # deployment to yaml sync workflow
            airbyte_model.write_yaml(args.target)
            print("Output written to: " + args.target)
        else:  # yaml to deployment sync workflow
            yaml_config, secrets = controller.read_yaml_config(args)
            if not config_validator.validate_config(yaml_config):
                print("Error: Invalid config provided as yaml. Exiting...")
                exit(2)
            dtos_from_config = controller.build_dtos_from_yaml_config(yaml_config, secrets)
            if args.backup_file:
                airbyte_model.write_yaml(args.backup_file)
            if args.wipe:
                controller.wipe_all(airbyte_model, client)
            print("Applying changes to deployment: " + client.airbyte_url)
            if args.sources or args.all:
                controller.sync_sources_to_deployment(airbyte_model, client, workspace, dtos_from_config)
                if args.validate:
                    controller.validate_sources(airbyte_model, client)
            if args.destinations or args.all:
                controller.sync_destinations_to_deployment(airbyte_model, client, workspace, dtos_from_config)
                if args.validate:
                    controller.validate_destinations(airbyte_model, client)
            if args.connections or args.all:
                controller.sync_connections_to_deployment(airbyte_model, client, dtos_from_config)
                if args.validate:
                    controller.validate_connections(airbyte_model, client)

    # wipe workflow
    elif args.mode == 'wipe':
        if args.sources or args.all:
            controller.wipe_sources(airbyte_model, client)
        if args.destinations or args.all:
            controller.wipe_destinations(airbyte_model, client)
        if args.connections or args.all:
            controller.wipe_connections(airbyte_model, client)

    # validate workflow
    elif args.mode == 'validate':
        if args.sources or args.all:
            controller.validate_sources(airbyte_model, client)
        if args.destinations or args.all:
            controller.validate_destinations(airbyte_model, client)
        if args.connections or args.all:
            #  Q: what does it mean to validate a connection beyond validating its source and destination?
            pass  # TODO: Implement Controller.validate_connections ?
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
    parser.add_argument("--backup", action="store", dest="backup_file",
                        help="specifies a .yaml file to backup the configuration of the target before syncing")
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
